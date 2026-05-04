# Reliable Webhook

A small, focused webhook delivery service for reliable asynchronous HTTP callbacks.

The project accepts events, stores them in MySQL, delivers them to a target URL, retries retryable failures, and exposes delivery state and Prometheus metrics.

## Core Flow

1. `POST /events` stores an event and creates one delivery.
2. The delivery pool claims ready deliveries and marks them as `running`.
3. A worker sends the payload to `target_url`.
4. The delivery becomes `succeeded`, `dead`, or `pending` for a later retry.
5. `POST /events/:id/replay` creates a new delivery for a previous event when no active delivery exists.

## Project Cases

See [docs/project-cases.md](docs/project-cases.md) for concrete scenarios this project solves, including order notifications, payment success events, and worker crash recovery.

## Delivery States

- `pending`: ready to run now or after `next_retry_at`.
- `running`: claimed by a worker until `locked_until`.
- `succeeded`: target returned a 2xx response.
- `dead`: non-retryable failure or max attempts reached.

If the process exits while a delivery is `running`, the delivery can be claimed again after `locked_until`.

## API

Create an event:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_key": "order-1001-created",
    "event_type": "order.created",
    "payload": "{\"order_id\":1001}",
    "target_url": "http://localhost:8080/mock-downstream"
  }'
```

Get event detail:

```bash
curl http://localhost:8080/events/1
```

Replay an event:

```bash
curl -X POST http://localhost:8080/events/1/replay
```

Health and metrics:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

## Configuration

The service reads `config.yml` by default. You can override the path with `CONFIG_FILE`.

Common environment overrides:

- `HTTP_ADDR`
- `MYSQL_DSN`
- `REQUEST_TIMEOUT`
- `DELIVERY_TIMEOUT`
- `SHUTDOWN_TIMEOUT`
- `WORKER_COUNT`
- `QUEUE_SIZE`
- `POLL_INTERVAL`

## Database

Create the database, then run:

```bash
mysql -u root -p webhook_platform < sql/init.sql
```

## Development

Run checks:

```bash
go test ./...
```

Start the server:

```bash
go run ./cmd/server
```

## Docker

Build and start the service with MySQL:

```bash
docker compose up --build
```

The API will be available at:

```text
http://localhost:8080
```

Stop the containers:

```bash
docker compose down
```
