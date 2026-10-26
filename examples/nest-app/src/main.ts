import "reflect-metadata";
import type { LoggerService } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { createLogger, expressTelemetry } from "@rudow/node-logger";
import { AppModule } from "./app.module";

const sdkLogger = createLogger({
    apiKey: process.env.RUDOW_API_KEY || "",
    endpoint: process.env.RUDOW_ENDPOINT,
});

const nestLogger: LoggerService = {
    log: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.info(asText(message), asAttrs(optionalParams));
    },
    warn: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.warn(asText(message), asAttrs(optionalParams));
    },
    error: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.error(asText(message), asAttrs(optionalParams));
    },
    debug: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.debug(asText(message), asAttrs(optionalParams));
    },
    verbose: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.debug(asText(message), asAttrs(optionalParams));
    },
    fatal: (message: unknown, ...optionalParams: unknown[]) => {
        sdkLogger.fatal(asText(message), asAttrs(optionalParams));
    },
};

async function bootstrap() {
    const app = await NestFactory.create(AppModule, { bufferLogs: true });
    app.useLogger(nestLogger);
    app.use(expressTelemetry(sdkLogger));
    app.enableShutdownHooks();
    const port = Number(process.env.PORT || 4200);

    await app.listen(port);
    sdkLogger.info("nestjs example started", { port });
    console.log(`nestjs example listening on http://localhost:${port}`);

    const flushAndExit = async () => {
        await sdkLogger.flush();
        process.exit(0);
    };
    process.once("SIGINT", () => void flushAndExit());
    process.once("SIGTERM", () => void flushAndExit());
}

void bootstrap();

function asText(message: unknown): string {
    if (typeof message === "string") return message;
    try {
        return JSON.stringify(message);
    } catch {
        return String(message);
    }
}

function asAttrs(optionalParams: unknown[]): Record<string, unknown> {
    if (optionalParams.length === 0) return {};
    if (
        optionalParams.length === 1 &&
        optionalParams[0] !== null &&
        typeof optionalParams[0] === "object"
    ) {
        return optionalParams[0] as Record<string, unknown>;
    }
    return { details: optionalParams.map((v) => asText(v)) };
}
