const path = require("node:path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const express = require("express");
const { createLogger, expressTelemetry } = require("@rudow/node-logger");

const app = express();
const port = Number(process.env.PORT || 4100);

const logger = createLogger({
    apiKey: process.env.RUDOW_API_KEY || "",
    endpoint: process.env.RUDOW_ENDPOINT,
});

app.use(express.json());
app.use(expressTelemetry(logger));

app.get("/health", (_req, res) => {
    logger.info("health check", { status: "ok" });
    res.json({ ok: true, framework: "express" });
});

app.post("/users", (req, res) => {
    const name = String(req.body?.name || "anonymous");
    logger.info("POST /users", { name });
    res.status(201).json({ id: "u_1", name });
});

app.listen(port, () => {
    logger.info("express example started", { port });
    console.log(`express example listening on http://localhost:${port}`);
});
