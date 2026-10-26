import build from "pino-abstract-transport";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { gzipSync } from "node:zlib";
import { readFileSync, writeFileSync, rmSync } from "node:fs";
import { Writable } from "node:stream";

/**
 * Backend log collector. Batches entries in memory and flushes them to the
 * Rudow ingest endpoint on an interval or when the batch fills.
 *
 * One-line setup for any Node backend:
 *
 *   const logger = createLogger({ apiKey: process.env.RUDOW_API_KEY! });
 *   logger.error("checkout failed", { orderId, status: 500 });
 */

export type Level = "debug" | "info" | "warn" | "error" | "fatal";

export interface LoggerOptions {
  apiKey: string;
  /** Ingest endpoint. Defaults to the public API's /v1/ingest. */
  endpoint?: string;
  /** Flush when this many entries are buffered. Default 20. */
  batchSize?: number;
  /** Flush at most this often, in ms. Default 2000. */
  flushIntervalMs?: number;
  /** Logical source label. Default "backend". */
  source?: string;
  /**
   * Max entries kept buffered while the endpoint is unreachable. Beyond this the
   * oldest are dropped, so an outage can't grow memory without bound. Default 10000.
   */
  maxBufferSize?: number;
  /**
   * Send attempts per flush on a retryable failure (network error, 429, or 5xx)
   * before the batch is re-queued for the next cycle. Default 4.
   */
  maxRetries?: number;
  /** Base delay for exponential backoff between retries, in ms. Default 500. */
  retryBackoffMs?: number;
  /**
   * gzip the request body once the JSON reaches this many bytes (the API
   * transparently decompresses). Set 0 to disable. Default 1024.
   */
  gzipThreshold?: number;
  /**
   * File path for crash-durable buffering. When set, entries that couldn't be
   * delivered are written here and reloaded on the next startup. Disabled by
   * default. Recommended on platforms with a writable disk.
   * For containers this MUST be on a mounted volume to survive recreation.
   */
  persistPath?: string;
  /**
   * On SIGTERM/SIGINT/beforeExit, flush buffered entries (and, if persistPath is
   * set, checkpoint the remainder to disk) before the process exits — so a
   * graceful container stop / rolling deploy doesn't drop the buffer. Default
   * true. The handler calls process.exit(0); if your app manages its own signal
   * handling or uses multiple loggers, set this false and call flush() yourself.
   * Give the container enough shutdown grace (e.g. compose `stop_grace_period`).
   */
  flushOnExit?: boolean;
  /**
   * Last-resort durability with NO disk required: when entries can't be delivered
   * (buffer overflow while the endpoint is unreachable, permanent API rejection,
   * or undelivered at shutdown without persistPath), print them to stderr as
   * newline-delimited JSON so the container platform's log pipeline (docker logs,
   * CloudWatch, …) captures them instead of losing them. Default true.
   */
  fallbackToStderr?: boolean;
  /**
   * Auto-emit process metrics heartbeat for transport integrations (Winston/Pino).
   * Default true for transports, ignored by direct createLogger unless you call
   * startMetricsHeartbeat yourself.
   */
  enableMetricsHeartbeat?: boolean;
  /** Sampling interval for auto heartbeat, in ms. Default 15000. */
  metricsHeartbeatMs?: number;
}

interface Entry {
  source: string;
  level: Level;
  timestamp: string;
  message: string;
  attributes?: Record<string, unknown>;
}

export interface Logger {
  log(level: Level, message: string, attributes?: Record<string, unknown>): void;
  debug(message: string, attributes?: Record<string, unknown>): void;
  info(message: string, attributes?: Record<string, unknown>): void;
  warn(message: string, attributes?: Record<string, unknown>): void;
  error(message: string, attributes?: Record<string, unknown>): void;
  fatal(message: string, attributes?: Record<string, unknown>): void;
  /** Force-send buffered entries. Call before process exit. */
  flush(): Promise<void>;
}

export interface WinstonLikeInfo {
  level?: unknown;
  message?: unknown;
  [key: string]: unknown;
  [key: symbol]: unknown;
}

// Winston's internal symbol keys come from `triple-beam`; we use Symbol.for to
// avoid adding a runtime dependency just for extraction.
const WINSTON_LEVEL: symbol = Symbol.for("level");
const WINSTON_MESSAGE: symbol = Symbol.for("message");
const WINSTON_SPLAT: symbol = Symbol.for("splat");
const ANSI_RE = /\u001b\[[0-9;]*m/g;

function stripAnsi(text: string): string {
  return text.replace(ANSI_RE, "");
}

function stringifyJsonSafe(value: unknown): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(value, (_k, v) => {
    if (typeof v === "bigint") return v.toString();
    if (typeof v === "function") return `[Function ${v.name || "anonymous"}]`;
    if (typeof v === "symbol") return v.toString();
    if (typeof v === "object" && v !== null) {
      if (seen.has(v)) return "[Circular]";
      seen.add(v);
    }
    return v;
  });
}

function toJsonSafeValue(value: unknown): unknown {
  try {
    return JSON.parse(stringifyJsonSafe(value));
  } catch {
    return { unserializable: true, type: typeof value };
  }
}

// Stay well under the ingest API's 1000-entry/batch cap, so a large backlog is
// drained over several requests rather than rejected wholesale.
const MAX_SEND_BATCH = 500;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function createLogger(opts: LoggerOptions): Logger {
  const endpoint = opts.endpoint ?? "https://api.rudow.ai/v1/ingest";
  const batchSize = opts.batchSize ?? 20;
  const flushIntervalMs = opts.flushIntervalMs ?? 2000;
  const source = opts.source ?? "backend";
  const maxBufferSize = opts.maxBufferSize ?? 10000;
  const maxRetries = opts.maxRetries ?? 4;
  const retryBackoffMs = opts.retryBackoffMs ?? 500;
  const gzipThreshold = opts.gzipThreshold ?? 1024;
  const persistPath = opts.persistPath;
  const flushOnExit = opts.flushOnExit ?? true;
  const fallbackToStderr = opts.fallbackToStderr ?? true;

  let buffer: Entry[] = loadPersisted();
  let timer: ReturnType<typeof setInterval> | undefined;
  let sending = false; // guards against overlapping flushes (interval + batch trigger)
  let lastDropWarn = 0;

  // Crash durability: undelivered entries live on disk between runs.
  function loadPersisted(): Entry[] {
    if (!persistPath) return [];
    try {
      const arr = JSON.parse(readFileSync(persistPath, "utf8"));
      rmSync(persistPath, { force: true });
      return Array.isArray(arr) ? arr : [];
    } catch {
      return []; // no file / unreadable / FS unavailable — start clean
    }
  }

  function persist(): void {
    if (!persistPath) return;
    try {
      if (buffer.length === 0) rmSync(persistPath, { force: true });
      else writeFileSync(persistPath, JSON.stringify(buffer));
    } catch {
      // Durability is best-effort; never break the host app.
    }
  }

  // Surface dropped logs instead of losing them silently — throttled so an
  // outage can't itself become a log flood.
  function warnDropped(n: number, reason: string): void {
    const now = Date.now();
    if (now - lastDropWarn < 30000) return;
    lastDropWarn = now;
    console.warn(`[rudow] dropped ${n} log entr${n === 1 ? "y" : "ies"}: ${reason}`);
  }

  // Durability of last resort (no disk required): emit undeliverable entries to
  // stderr as newline-delimited JSON so the container platform's log pipeline
  // captures them. Called for overflow/permanent-rejection/undelivered-at-exit.
  function emitEntriesToStderr(entries: Entry[], reason: string): void {
    if (!fallbackToStderr || entries.length === 0) return;
    process.stderr.write(
      `[rudow] ${entries.length} undelivered log entr${
        entries.length === 1 ? "y" : "ies"
      } (${reason}) — emitting to stderr for platform capture\n`
    );
    for (const e of entries) {
      try {
        process.stderr.write(JSON.stringify(e) + "\n");
      } catch {
        // never let logging break the host app
      }
    }
  }

  // Non-retryable (4xx) rejections carry a machine-readable body — in
  // particular { error: "plan_limit_exceeded", metric, limit, planId } from
  // the ingest API's monthly-quota checkpoint — so surface something a
  // developer can actually act on instead of a generic "rejected" message.
  async function describeRejection(res: Response): Promise<string> {
    try {
      const body = (await res.json()) as {
        error?: string;
        metric?: string;
        limit?: number;
        planId?: string;
      };
      if (body?.error === "plan_limit_exceeded") {
        return (
          `monthly ${body.metric} quota exceeded for the "${body.planId}" plan ` +
          `(limit ${body.limit}) — upgrade at your Rudow dashboard to continue sending logs`
        );
      }
      if (typeof body?.error === "string") return `rejected by ingest API (${res.status}): ${body.error}`;
    } catch {
      // body wasn't JSON — fall through to the generic message
    }
    return `rejected by ingest API (${res.status})`;
  }

  // POST one batch. "ok" on 2xx; { ok: false, reason } on a non-retryable 4xx
  // (bad key or payload, or a plan-limit rejection — retrying won't help);
  // throws on a retryable error (network/429/5xx).
  async function postBatch(entries: Entry[]): Promise<{ ok: true } | { ok: false; reason: string }> {
    const json = JSON.stringify({ entries });
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${opts.apiKey}`,
    };
    let body: string | Blob = json;
    if (gzipThreshold > 0 && Buffer.byteLength(json) >= gzipThreshold) {
      body = new Blob([gzipSync(json)]);
      headers["Content-Encoding"] = "gzip";
    }
    const res = await fetch(endpoint, { method: "POST", headers, body });
    if (res.ok) return { ok: true };
    if (res.status === 429 || res.status >= 500) throw new Error(`ingest ${res.status}`);
    return { ok: false, reason: await describeRejection(res) };
  }

  // Send with exponential backoff + jitter.
  async function trySend(
    entries: Entry[]
  ): Promise<{ status: "sent" } | { status: "drop"; reason: string } | { status: "retry" }> {
    for (let attempt = 0; ; attempt++) {
      try {
        const result = await postBatch(entries);
        return result.ok ? { status: "sent" } : { status: "drop", reason: result.reason };
      } catch {
        if (attempt >= maxRetries) return { status: "retry" }; // keep buffered for next cycle
        await sleep(retryBackoffMs * 2 ** attempt + Math.floor(Math.random() * 100));
      }
    }
  }

  async function flushNow(): Promise<void> {
    if (sending || buffer.length === 0) return;
    sending = true;
    try {
      while (buffer.length > 0) {
        const batch = buffer.slice(0, MAX_SEND_BATCH);
        const result = await trySend(batch);
        if (result.status === "retry") break; // endpoint unreachable — retain & stop
        buffer.splice(0, batch.length); // delivered or permanently rejected
        if (result.status === "drop") {
          emitEntriesToStderr(batch, result.reason);
          warnDropped(batch.length, result.reason);
        }
      }
    } finally {
      persist(); // checkpoint whatever's left (or clear the file when drained)
      sending = false;
    }
  }

  function ensureTimer() {
    if (timer) return;
    timer = setInterval(() => void flushNow(), flushIntervalMs);
    // Don't keep the event loop alive just for flushing.
    if (typeof timer.unref === "function") timer.unref();
  }

  function enqueue(level: Level, message: string, attributes?: Record<string, unknown>) {
    buffer.push({
      source,
      level,
      timestamp: new Date().toISOString(),
      message,
      attributes,
    });
    if (buffer.length > maxBufferSize) {
      const overflow = buffer.length - maxBufferSize;
      const dropped = buffer.splice(0, overflow); // drop oldest first
      emitEntriesToStderr(dropped, "buffer full — endpoint unreachable?");
      warnDropped(overflow, "buffer full (endpoint unreachable?)");
    }
    ensureTimer();
    if (buffer.length >= batchSize) void flushNow();
  }

  // Reloaded backlog needs a timer even before the host logs anything new.
  if (buffer.length > 0) ensureTimer();

  // Graceful-shutdown durability: on exit, best-effort network flush, then keep
  // whatever couldn't be delivered — on disk (persistPath) or, failing that, on
  // stderr for the platform to capture. Covers rolling deploys / `docker stop`;
  // a SIGKILL/OOM can't be caught, so pair with persistPath for that.
  let shuttingDown = false;
  // `exitAfter` distinguishes a caught signal (we prevented default termination,
  // so we must exit ourselves) from `beforeExit` (the loop drained naturally —
  // don't force-exit, or we'd clobber the exit code and other listeners).
  async function handleExit(reason: string, exitAfter: boolean): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      await flushNow();
    } catch {
      // fall through to the disk/stderr safety nets below
    }
    if (buffer.length > 0) {
      if (persistPath) persist(); // synchronous checkpoint to disk
      else emitEntriesToStderr(buffer, `undelivered at ${reason}`);
    }
    if (exitAfter) process.exit(0);
  }

  if (flushOnExit) {
    process.once("SIGTERM", () => handleExit("SIGTERM", true));
    process.once("SIGINT", () => handleExit("SIGINT", true));
    process.once("beforeExit", () => handleExit("beforeExit", false));
  }

  return {
    log: enqueue,
    debug: (m, a) => enqueue("debug", m, a),
    info: (m, a) => enqueue("info", m, a),
    warn: (m, a) => enqueue("warn", m, a),
    error: (m, a) => enqueue("error", m, a),
    fatal: (m, a) => enqueue("fatal", m, a),
    flush: flushNow,
  };
}

// Pino numeric levels → our level names.
const PINO_LEVELS: Record<number, Level> = {
  10: "debug", // trace
  20: "debug",
  30: "info",
  40: "warn",
  50: "error",
  60: "fatal",
};

// Winston string levels (npm/syslog/custom) normalized to ingest levels.
const WINSTON_LEVELS: Record<string, Level> = {
  silly: "debug",
  verbose: "debug",
  debug: "debug",
  http: "info",
  info: "info",
  notice: "info",
  warn: "warn",
  warning: "warn",
  error: "error",
  crit: "fatal",
  critical: "fatal",
  alert: "fatal",
  emerg: "fatal",
  emergency: "fatal",
  fatal: "fatal",
};

function levelFromWinston(rawLevel: unknown): Level {
  const normalized = stripAnsi(String(rawLevel ?? "info")).trim().toLowerCase();
  return WINSTON_LEVELS[normalized] ?? "info";
}

function pickString(...values: unknown[]): string | undefined {
  for (const v of values) {
    if (typeof v === "string" && v.trim()) return v;
  }
  return undefined;
}

function pickNumber(...values: unknown[]): number | undefined {
  for (const v of values) {
    if (typeof v === "number" && Number.isFinite(v)) return v;
    if (typeof v === "string" && v.trim()) {
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
  }
  return undefined;
}

function pickStatusCode(...values: unknown[]): number | undefined {
  const n = pickNumber(...values);
  if (n === undefined) return undefined;
  const rounded = Math.round(n);
  if (rounded < 100 || rounded > 599) return undefined;
  return rounded;
}

const SENSITIVE_KEY_RE = /(?:pass(?:word)?|authorization|token|secret|api[_-]?key|cookie|jwt)/i;
const REDACTED = "[REDACTED]";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPrimitive(value: unknown): value is string | number | boolean | null {
  return value === null || ["string", "number", "boolean"].includes(typeof value);
}

function redactSensitive(value: unknown, depth = 0): unknown {
  if (depth > 8) return "[Truncated]";
  if (typeof value === "string") {
    return /^Bearer\s+/i.test(value) ? REDACTED : value;
  }
  if (Array.isArray(value)) return value.map((item) => redactSensitive(item, depth + 1));
  if (!isRecord(value)) return value;

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value)) {
    out[k] = SENSITIVE_KEY_RE.test(k) ? REDACTED : redactSensitive(v, depth + 1);
  }
  return out;
}

function summarizeError(value: unknown): Record<string, unknown> | undefined {
  if (!isRecord(value)) return undefined;
  const statusCode = pickNumber(
    value.statusCode,
    value.status,
    value.httpCode,
    (isRecord(value.response) ? value.response.statusCode : undefined),
    (isRecord(value.response) ? value.response.status : undefined)
  );

  const summary: Record<string, unknown> = {
    name: pickString(value.name) ?? "Error",
    message: pickString(value.message) ?? String(value),
  };
  if (statusCode !== undefined) summary.statusCode = Math.round(statusCode);
  if (typeof value.code === "string" && value.code.trim()) summary.code = value.code;
  return summary;
}

function normalizeWinstonMeta(attrs: Record<string, unknown>): void {
  const meta = isRecord(attrs.meta) ? attrs.meta : undefined;
  if (!meta) return;

  const noisyMetaKeys = new Set(["req", "request", "res", "response", "splat", "socket", "client"]);
  for (const [k, v] of Object.entries(meta)) {
    if (noisyMetaKeys.has(k) || attrs[k] !== undefined) continue;
    if (isPrimitive(v)) attrs[k] = v;
    else if (Array.isArray(v) && v.length <= 8 && v.every((x) => isPrimitive(x))) attrs[k] = v;
  }
  delete attrs.meta;
}

function compactWinstonHttpAttributes(attrs: Record<string, unknown>): void {
  if (attrs.type !== "http_request") return;

  const noisyTopLevelKeys = [
    "req",
    "request",
    "res",
    "response",
    "socket",
    "client",
    "splat",
    "rawHeaders",
    "rawTrailers",
  ];
  for (const k of noisyTopLevelKeys) delete attrs[k];
}

function sanitizeWinstonAttributes(attrs: Record<string, unknown>): Record<string, unknown> {
  const meta = isRecord(attrs.meta) ? attrs.meta : undefined;

  const splatFirst = Array.isArray(attrs.splat) && attrs.splat.length > 0
    ? (attrs.splat[0] as Record<string, unknown> | undefined)
    : undefined;

  const errSummary =
    summarizeError(attrs.err) ?? summarizeError(splatFirst?.err) ?? summarizeError(meta?.err);
  if (errSummary) attrs.err = errSummary;

  normalizeWinstonMeta(attrs);
  compactWinstonHttpAttributes(attrs);
  return redactSensitive(attrs) as Record<string, unknown>;
}

function normalizeWinstonHttpAttributes(attrs: Record<string, unknown>): void {
  if (attrs.type === "http_request") return;

  const splatFirst = Array.isArray(attrs.splat) && attrs.splat.length > 0
    ? (attrs.splat[0] as Record<string, unknown> | undefined)
    : undefined;
  const splatReq = splatFirst?.req as Record<string, unknown> | undefined;
  const splatRes = splatFirst?.res as Record<string, unknown> | undefined;
  const splatErr = splatFirst?.err as Record<string, unknown> | undefined;

  const req = (attrs.req as Record<string, unknown> | undefined) ??
    (attrs.request as Record<string, unknown> | undefined) ??
    splatReq ??
    (((attrs.meta as Record<string, unknown> | undefined)?.req as Record<string, unknown> | undefined) ??
      ((attrs.meta as Record<string, unknown> | undefined)?.request as
        | Record<string, unknown>
        | undefined));
  const res = (attrs.res as Record<string, unknown> | undefined) ??
    (attrs.response as Record<string, unknown> | undefined) ??
    splatRes ??
    (((attrs.meta as Record<string, unknown> | undefined)?.res as Record<string, unknown> | undefined) ??
      ((attrs.meta as Record<string, unknown> | undefined)?.response as
        | Record<string, unknown>
        | undefined));
  const err = (attrs.err as Record<string, unknown> | undefined) ?? splatErr;

  const method = pickString(
    attrs.method,
    attrs.httpMethod,
    req?.method,
    req?.httpMethod
  );
  const route = pickString(
    attrs.route,
    attrs.path,
    attrs.url,
    attrs.originalUrl,
    splatFirst?.route,
    splatFirst?.path,
    splatFirst?.url,
    splatFirst?.originalUrl,
    req?.route,
    req?.path,
    req?.url,
    req?.originalUrl
  );
  const status = pickStatusCode(
    attrs.status,
    attrs.statusCode,
    attrs.status_code,
    splatFirst?.status,
    splatFirst?.statusCode,
    err?.status,
    err?.statusCode,
    res?.status,
    res?.statusCode,
    (attrs.meta as Record<string, unknown> | undefined)?.status,
    (attrs.meta as Record<string, unknown> | undefined)?.statusCode,
    (attrs.meta as Record<string, unknown> | undefined)?.status_code,
    ((attrs.meta as Record<string, unknown> | undefined)?.res as Record<string, unknown> | undefined)
      ?.status,
    ((attrs.meta as Record<string, unknown> | undefined)?.res as Record<string, unknown> | undefined)
      ?.statusCode,
    ((attrs.meta as Record<string, unknown> | undefined)?.response as Record<string, unknown> | undefined)
      ?.status,
    ((attrs.meta as Record<string, unknown> | undefined)?.response as Record<string, unknown> | undefined)
      ?.statusCode
  );
  const durationMs = pickNumber(
    attrs.duration_ms,
    attrs.durationMs,
    splatFirst?.duration_ms,
    splatFirst?.durationMs,
    splatFirst?.executionTime,
    splatFirst?.execution_time,
    splatFirst?.responseTime,
    splatFirst?.response_time,
    attrs.executionTime,
    attrs.execution_time,
    attrs.responseTime,
    attrs.response_time,
    splatReq?.executionTime,
    req?.executionTime,
    (attrs.meta as Record<string, unknown> | undefined)?.responseTime,
    (attrs.meta as Record<string, unknown> | undefined)?.durationMs,
    (attrs.meta as Record<string, unknown> | undefined)?.duration_ms,
    (attrs.meta as Record<string, unknown> | undefined)?.executionTime,
    (attrs.meta as Record<string, unknown> | undefined)?.execution_time,
    ((attrs.meta as Record<string, unknown> | undefined)?.req as Record<string, unknown> | undefined)
      ?.executionTime
  );

  if (status === undefined && durationMs === undefined) return;

  attrs.type = "http_request";
  if (method) attrs.method = method;
  if (route) attrs.route = route.split("?")[0];
  if (status !== undefined) attrs.status = Math.round(status);
  if (status === undefined && (method || route || err)) {
    attrs.status_probe = {
      top: {
        status: attrs.status,
        statusCode: attrs.statusCode,
        status_code: attrs.status_code,
      },
      err: {
        status: err?.status,
        statusCode: err?.statusCode,
        name: err?.name,
        code: err?.code,
      },
      res: {
        status: res?.status,
        statusCode: res?.statusCode,
      },
      meta: {
        status: (attrs.meta as Record<string, unknown> | undefined)?.status,
        statusCode: (attrs.meta as Record<string, unknown> | undefined)?.statusCode,
        status_code: (attrs.meta as Record<string, unknown> | undefined)?.status_code,
        resStatus: ((attrs.meta as Record<string, unknown> | undefined)?.res as Record<string, unknown> | undefined)
          ?.status,
        resStatusCode: ((attrs.meta as Record<string, unknown> | undefined)?.res as Record<string, unknown> | undefined)
          ?.statusCode,
        responseStatus:
          ((attrs.meta as Record<string, unknown> | undefined)?.response as Record<string, unknown> | undefined)
            ?.status,
        responseStatusCode:
          ((attrs.meta as Record<string, unknown> | undefined)?.response as Record<string, unknown> | undefined)
            ?.statusCode,
      },
    };
    attrs.status_missing = true;
  }
  if (durationMs !== undefined) attrs.duration_ms = Math.max(Math.round(durationMs), 0);
}

/**
 * Winston transport stream. Use it as a custom stream transport:
 *
 *   import winston from "winston";
 *   import { createWinstonTransport } from "@rudow/node-logger";
 *
 *   const logger = winston.createLogger({
 *     transports: [new winston.transports.Stream({ stream: createWinstonTransport({ apiKey }) })],
 *   });
 */
export function createWinstonTransport(opts: LoggerOptions): Writable {
  const client = createLogger(opts);
  const stopHeartbeat =
    opts.enableMetricsHeartbeat === false
      ? () => {}
      : startMetricsHeartbeat(client, { intervalMs: opts.metricsHeartbeatMs });

  return new Writable({
    objectMode: true,
    write(chunk: unknown, _encoding, callback) {
      try {
        const info = (chunk ?? {}) as WinstonLikeInfo;
        const level = levelFromWinston(
          info.level ?? info[WINSTON_LEVEL] ?? info.logLevel ?? info.severity_text
        );
        const messageValue = info.message ?? info[WINSTON_MESSAGE] ?? "";
        const message =
          typeof messageValue === "string"
            ? stripAnsi(messageValue)
            : stringifyJsonSafe(messageValue);

        const attrs: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(info)) {
          if (k === "level" || k === "message") continue;
          attrs[k] = toJsonSafeValue(v);
        }
        const splat = info[WINSTON_SPLAT];
        if (Array.isArray(splat)) attrs.splat = toJsonSafeValue(splat);

        // Winston HTTP logs often carry status/duration in custom keys (or nested
        // req/res/meta objects). Normalize them to the dashboard metrics schema.
        normalizeWinstonHttpAttributes(attrs);
        const sanitized = sanitizeWinstonAttributes(attrs);

        client.log(level, message, sanitized);
        callback();
      } catch {
        // Keep host logging pipeline resilient.
        callback();
      }
    },
    final(callback) {
      stopHeartbeat();
      void client.flush().finally(() => callback());
    },
  });
}

/**
 * Pino transport. Wire it up so existing pino logs flow to the ingest API:
 *
 *   import pino from "pino";
 *   const logger = pino({
 *     transport: {
 *       target: "@rudow/node-logger/transport",
 *       options: { apiKey: process.env.RUDOW_API_KEY },
 *     },
 *   });
 */
export default function pinoTransport(opts: LoggerOptions) {
  const client = createLogger(opts);
  const stopHeartbeat =
    opts.enableMetricsHeartbeat === false
      ? () => {}
      : startMetricsHeartbeat(client, { intervalMs: opts.metricsHeartbeatMs });

  return build(
    async (source) => {
      for await (const obj of source as AsyncIterable<Record<string, unknown>>) {
        const level = PINO_LEVELS[(obj.level as number) ?? 30] ?? "info";
        const message = (obj.msg as string) ?? "";
        const { level: _l, msg: _m, time: _t, pid: _p, hostname: _h, ...rest } = obj;
        client.log(level, message, rest);
      }
    },
    {
      close: async () => {
        stopHeartbeat();
        await client.flush();
      },
    }
  );
}

// ---------------------------------------------------------------------------
// Telemetry capture
//
// These helpers turn the logger from a message forwarder into lightweight
// instrumentation: structured events that let the AI reason about HOW the app
// is running (status-code trends, latency, error rate, memory/event-loop
// health). Every event rides in the normalized record's `attributes`, keyed by
// `attributes.type` — no schema change needed.
// ---------------------------------------------------------------------------

function levelForStatus(status: number): Level {
  if (status >= 500) return "error";
  if (status >= 400) return "warn";
  return "info";
}

export interface HttpRequestEvent {
  method: string;
  route: string;
  status: number;
  durationMs: number;
  /** Extra attributes (ip, userId, bytes, …). */
  extra?: Record<string, unknown>;
}

/** Record one completed HTTP request as a structured `http_request` event. */
export function recordHttpRequest(client: Logger | undefined, e: HttpRequestEvent): void {
  if (!client) return;
  const status = Number.isFinite(e.status) ? e.status : 0;
  const ms = Math.round(e.durationMs);
  client.log(levelForStatus(status), `${e.method} ${e.route} ${status} (${ms}ms)`, {
    type: "http_request",
    method: e.method,
    route: e.route,
    status,
    duration_ms: ms,
    ...(e.extra ?? {}),
  });
}

export interface ExceptionEvent {
  error: unknown;
  route?: string;
  method?: string;
  status?: number;
  extra?: Record<string, unknown>;
}

/** Record an unhandled error as a structured `exception` event. */
export function recordException(client: Logger | undefined, e: ExceptionEvent): void {
  if (!client) return;
  const err = e.error as { name?: string; message?: string; stack?: string } | undefined;
  const name = err?.name ?? "Error";
  const message = err?.message ?? String(e.error);
  client.log("error", `${name}: ${message}`, {
    type: "exception",
    error: name,
    route: e.route,
    method: e.method,
    status: e.status,
    stack:
      typeof err?.stack === "string" ? err.stack.split("\n").slice(0, 6).join("\n") : undefined,
    ...(e.extra ?? {}),
  });
}

export interface MetricsOptions {
  /** Sampling interval in ms. Default 15000. */
  intervalMs?: number;
}

/**
 * Periodically emit a `metrics` event with process health: memory, event-loop
 * lag, and uptime. Returns a stop function. No-op if `client` is undefined.
 */
export function startMetricsHeartbeat(
  client: Logger | undefined,
  opts: MetricsOptions = {}
): () => void {
  if (!client) return () => {};
  const intervalMs = opts.intervalMs ?? 15000;
  const lag = monitorEventLoopDelay({ resolution: 20 });
  lag.enable();

  const mb = (bytes: number) => Math.round((bytes / 1048576) * 10) / 10;

  const tick = () => {
    const mem = process.memoryUsage();
    const lagMs = Math.round((lag.mean / 1e6) * 10) / 10;
    lag.reset();
    client.log(
      "debug",
      `metrics rss=${mb(mem.rss)}MB heap=${mb(mem.heapUsed)}/${mb(mem.heapTotal)}MB lag=${lagMs}ms`,
      {
        type: "metrics",
        rss_mb: mb(mem.rss),
        heap_used_mb: mb(mem.heapUsed),
        heap_total_mb: mb(mem.heapTotal),
        event_loop_lag_ms: lagMs,
        uptime_s: Math.round(process.uptime()),
      }
    );
  };

  const timer = setInterval(tick, intervalMs);
  if (typeof timer.unref === "function") timer.unref();
  return () => {
    clearInterval(timer);
    lag.disable();
  };
}

// Minimal structural types so we don't depend on @types/express.
interface MinReq {
  method?: string;
  originalUrl?: string;
  url?: string;
  baseUrl?: string;
  route?: { path?: string };
}
interface MinRes {
  statusCode?: number;
  on(event: "finish", cb: () => void): void;
}

export interface TelemetryOptions {
  /**
   * Fraction (0..1) of *successful* requests to record; anything >= 400 is
   * always recorded. Lowers ingest volume (and bandwidth) on busy services.
   * Default 1 (record everything).
   *
   * Note: sampling scales down the recorded request volume, so volume-based
   * metrics (request_count, error_rate denominator) under-report by this
   * factor. Use it when raw error visibility matters more than exact counts.
   */
  sampleRate?: number;
}

/**
 * Express middleware that records requests as `http_request` events. By default
 * every request is recorded; pass `{ sampleRate }` to sample successful ones.
 *
 *   app.use(expressTelemetry(createLogger({ apiKey })));
 *   app.use(expressTelemetry(client, { sampleRate: 0.1 })); // 10% of 2xx/3xx, all errors
 */
export function expressTelemetry(client: Logger | undefined, opts: TelemetryOptions = {}) {
  const sampleRate = opts.sampleRate ?? 1;
  return (req: MinReq, res: MinRes, next: () => void): void => {
    if (!client) return next();
    const start = Date.now();
    res.on("finish", () => {
      const status = res.statusCode ?? 0;
      // Always keep errors; sample the successful majority.
      if (status < 400 && sampleRate < 1 && Math.random() > sampleRate) return;
      const path = (req.originalUrl ?? req.url ?? "").split("?")[0];
      const route = req.route?.path
        ? (req.baseUrl ?? "") + req.route.path
        : path || "unknown";
      recordHttpRequest(client, {
        method: req.method ?? "GET",
        route,
        status,
        durationMs: Date.now() - start,
      });
    });
    next();
  };
}
