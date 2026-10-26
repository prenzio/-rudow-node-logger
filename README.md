# @rudow/node-logger

**Rudow — AI that reads your logs and tells you what broke, in plain English.**

Send logs to Rudow, then ask questions like "why did checkout errors spike this
morning?" from the dashboard, Slack, or any MCP-compatible AI agent
(Claude Code, Cursor, Windsurf).

- **Read-only by design** — Rudow never mutates, rewrites, or acts on your
  systems. It reads logs. That's it.
- **Flat pricing** — monthly caps, never usage meters. Limits are caps, not
  overage charges.
- **Free-forever tier** — the Hobby plan is free, no credit card, no time limit.
- **MCP server** — `claude mcp add --transport http rudow https://mcp.rudow.ai/mcp`

One-line setup for any Node service: batches log entries and ships them to the
ingest API.


Homepage: https://rudow.ai · Documentation: https://rudow.ai/docs

## Install

```bash
npm install @rudow/node-logger
```

## Direct use

```ts
import { createLogger } from "@rudow/node-logger";

const logger = createLogger({
  apiKey: process.env.RUDOW_API_KEY!,            // your project API key
});

logger.info("server started", { port: 8080 });
logger.error("checkout failed", { orderId, status: 500, error: "ECONNREFUSED" });
```

## Exported API

`@rudow/node-logger` exports:

- `createLogger(options)`
- `createWinstonTransport(options)`
- default export (Pino transport target)
- `recordHttpRequest(client, event)`
- `recordException(client, event)`
- `startMetricsHeartbeat(client, options?)`
- `expressTelemetry(client, options?)`

Types are exported too (`Logger`, `LoggerOptions`, `Level`, `HttpRequestEvent`,
`ExceptionEvent`, `MetricsOptions`, `TelemetryOptions`).

Entries are buffered and flushed every 2s or once 20 are queued (both configurable).
Sending is best-effort — logging never throws into your app. On a graceful exit the
logger flushes automatically (see **Shutdown durability**); call `await
logger.flush()` yourself only if you've set `flushOnExit: false`.

## Bandwidth & durability

The logger is built to be light on the integrator's network and to not lose logs
on a blip:

- **Batched** — one POST per batch (every `flushIntervalMs`, or when `batchSize`
  is reached), never one request per log line.
- **Compressed** — bodies at/above `gzipThreshold` bytes are gzipped (the API
  decompresses transparently).
- **Retried** — network errors, `429`, and `5xx` are retried with exponential
  backoff; if still failing, the batch stays buffered for the next cycle.
  Non-retryable `4xx` (bad key/payload) is dropped with a throttled `console.warn`
  rather than retried forever.
- **Bounded** — the buffer is capped at `maxBufferSize`; during a long outage the
  oldest entries are dropped so memory can't grow without bound. Dropped entries
  aren't lost silently — see `fallbackToStderr` below.
- **Flush on exit** (`flushOnExit`, default on) — on `SIGTERM`/`SIGINT`/`beforeExit`
  the logger flushes, then checkpoints anything undelivered (to `persistPath` if set,
  else to stderr). Covers rolling deploys and `docker stop`.
- **stderr fallback** (`fallbackToStderr`, default on) — when entries can't be
  delivered and there's no `persistPath`, they're printed to **stderr as JSON** so
  the platform's log pipeline (`docker logs`, CloudWatch, etc.) captures them
  instead of losing them. Zero infra required.
- **Crash-durable** (`persistPath`) — point it at a writable file and undelivered
  entries survive a restart (checkpointed on flush and on exit, reloaded on startup).

```ts
const logger = createLogger({
  apiKey: process.env.RUDOW_API_KEY!,
  batchSize: 20,           // flush after N entries
  flushIntervalMs: 2000,   // …or after this long
  maxBufferSize: 10000,    // cap while offline (oldest dropped beyond this)
  maxRetries: 4,           // attempts per flush before re-queueing
  retryBackoffMs: 500,     // base for exponential backoff
  gzipThreshold: 1024,     // gzip bodies ≥ 1KB (0 disables)
  flushOnExit: true,       // flush + checkpoint on graceful shutdown (default)
  fallbackToStderr: true,  // dump undeliverable entries to stderr (default)
  persistPath: "/data/app-logs-buffer.json", // crash durability (optional)
});
```

### Containers: don't lose logs on shutdown

- Without `persistPath`, a graceful stop still delivers (flush on exit) when the API
  is reachable; if it isn't, entries fall back to **stderr** — captured by your
  platform's logs. A `SIGKILL`/OOM can't be caught, so nothing survives that without
  a disk.
- To survive hard kills, set `persistPath` to a path on a **mounted volume** (an
  ephemeral container layer is wiped on recreation), e.g. in `docker-compose.yml`:

  ```yaml
  services:
    your-app:
      stop_grace_period: 30s          # give the flush time to finish
      volumes:
        - applogs-buffer:/data        # persistPath lives here
  volumes:
    applogs-buffer:
  ```

- If your app does its own signal handling or creates multiple loggers, set
  `flushOnExit: false` (it calls `process.exit(0)`) and flush them yourself.

### HTTP request sampling

`expressTelemetry` records one `http_request` event per request. On a busy
service you can sample the *successful* ones to cut volume — errors (status ≥ 400)
are always kept:

```ts
app.use(expressTelemetry(logger, { sampleRate: 0.1 })); // 10% of 2xx/3xx, all errors
```

Note: sampling scales down recorded request volume, so volume-based metrics
(request_count, error-rate denominator) under-report by that factor.

## Telemetry helpers

### `recordHttpRequest`

Emit a single completed request as a normalized `http_request` event.

```ts
import { createLogger, recordHttpRequest } from "@rudow/node-logger";

const logger = createLogger({ apiKey: process.env.RUDOW_API_KEY! });
recordHttpRequest(logger, {
  method: "GET",
  route: "/v1/orders/:id",
  status: 200,
  durationMs: 37,
  extra: { userId: "u_123" },
});
```

### `recordException`

Emit an exception event with route/method/status context and a shortened stack.

```ts
import { createLogger, recordException } from "@rudow/node-logger";

const logger = createLogger({ apiKey: process.env.RUDOW_API_KEY! });

try {
  await doWork();
} catch (error) {
  recordException(logger, {
    error,
    route: "/v1/orders/:id",
    method: "GET",
    status: 500,
    extra: { orderId: "ord_42" },
  });
}
```

### `startMetricsHeartbeat`

Periodically emits process metrics (`rss`, heap, event-loop lag, uptime) as
`metrics` events. Returns a stop function.

```ts
import { createLogger, startMetricsHeartbeat } from "@rudow/node-logger";

const logger = createLogger({ apiKey: process.env.RUDOW_API_KEY! });
const stop = startMetricsHeartbeat(logger, { intervalMs: 15000 });

// On shutdown/tests:
stop();
await logger.flush();
```

### `expressTelemetry`

Express middleware that records one `http_request` event per response. You can
sample successful traffic while always keeping 4xx/5xx.

```ts
import express from "express";
import { createLogger, expressTelemetry, recordException } from "@rudow/node-logger";

const app = express();
const logger = createLogger({ apiKey: process.env.RUDOW_API_KEY! });

app.use(expressTelemetry(logger, { sampleRate: 0.1 }));

app.use((err: unknown, req, res, next) => {
  recordException(logger, {
    error: err,
    route: req.route?.path ?? req.path,
    method: req.method,
    status: res.statusCode >= 400 ? res.statusCode : 500,
  });
  next(err);
});
```

## With Pino

```ts
import pino from "pino";

const logger = pino({
  transport: {
    target: "@rudow/node-logger",
    options: { apiKey: process.env.RUDOW_API_KEY },
  },
});

logger.error({ orderId, status: 500 }, "checkout failed");
```

Pino levels map to `debug | info | warn | error | fatal`; structured fields become
the log entry's `attributes`.

## With Winston

```ts
import winston from "winston";
import { createWinstonTransport } from "@rudow/node-logger";

const logger = winston.createLogger({
  level: "info",
  transports: [
    new winston.transports.Stream({
      stream: createWinstonTransport({
        apiKey: process.env.RUDOW_API_KEY!,
      }),
    }),
  ],
});

logger.info("server started", { port: 8080 });
logger.error("checkout failed", { orderId, status: 500, error: "ECONNREFUSED" });
```

Winston levels are normalized to `debug | info | warn | error | fatal`.
Common HTTP fields (`status`, `statusCode`, `durationMs`, `responseTime`,
`req/res`, `meta.req/meta.res`) are auto-normalized into `http_request` events
so request count, error rate, and latency cards can populate without extra code.

Process metrics are auto-emitted by `createWinstonTransport(...)` and the Pino
transport every 15s by default (as `type=metrics`), so the dashboard Process
card populates without extra setup. Control this with:

- `enableMetricsHeartbeat: false` to disable
- `metricsHeartbeatMs: 10000` to change interval

```ts
new winston.transports.Stream({
  stream: createWinstonTransport({
    apiKey: process.env.RUDOW_API_KEY!,
    enableMetricsHeartbeat: true,
    metricsHeartbeatMs: 15000,
  }),
});
```

## Examples

Runnable Express and NestJS apps are in [`examples/`](examples/).

## License

[MIT](LICENSE)
