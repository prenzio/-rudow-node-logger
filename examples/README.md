# Examples

Small apps that use `@rudow/node-logger` with common Node frameworks. Each one installs the SDK from this repo (`"@rudow/node-logger": "file:../.."`), so build it first.

- `express-app`: Express server with `GET /health` and `POST /users`
- `nest-app`: NestJS server with `GET /ping` and `POST /orders`

## 1) Build the SDK

From the repository root:

```bash
npm install
npm run build
```

## 2) Express example

```bash
cd examples/express-app
npm install
cp .env.example .env   # then paste your project's ingest API key
npm start
```

## 3) NestJS example

```bash
cd examples/nest-app
npm install
cp .env.example .env   # then paste your project's ingest API key
npm start
```

## Env vars

- `RUDOW_API_KEY`: your project's ingest API key, from the Rudow dashboard (required to actually send logs)
- `RUDOW_ENDPOINT`: optional, defaults to `https://api.rudow.ai/v1/ingest`
- `PORT`: optional

Without `RUDOW_API_KEY` the apps still run; logs just aren't delivered.
