# FluxRisk

> An explainable, replay-safe real-time risk decision platform.

FluxRisk evaluates payment and login events under low latency and returns `allow`, `review`, or `block` with the exact features, rules, event-time state, and model evidence behind the decision. It is not a thin model wrapper: it demonstrates correctness under duplicate, concurrent, late, failed, and replayed events.

## Why this project is interview-worthy

- **M0–M1 — decision core and durable state:** sliding windows, global idempotency, per-account concurrency, PostgreSQL advisory locks, migrations, and transactional outbox.
- **M2 — streaming:** leased outbox workers publish to Redpanda HTTP Proxy with bounded exponential retry and dead-letter state.
- **M3 — event time:** watermarks distinguish accepted out-of-order events from too-late events; deterministic replay rebuilds decisions in event-time order.
- **M4 — model governance:** a versioned calibrated baseline supports shadow and assist modes; every probability and score contribution is audited.
- **M5 — reliability:** p50/p95/p99, Prometheus output, executable failure specifications, health checks, and a concurrent load generator.
- **M6 — product loop:** dashboard, event simulator, decision evidence, replay API, and optimistic-concurrency case review.
- **M7 — open source:** CI, runbooks, model card, benchmark protocol, issue templates, security and contribution policies.

## Quick start on Windows CMD

Fast in-memory loop:

```bat
scripts\verify.cmd
scripts\run-api.cmd
```

Keep that window open. In a second CMD window:

```bat
scripts\smoke-test.cmd
scripts\load-test.cmd http://127.0.0.1:8080 1000 32
```

Open `http://127.0.0.1:8080` for the dashboard. Metrics are at `/v1/metrics` (JSON) and `/metrics` (Prometheus text).

Full PostgreSQL + Redpanda stack (Docker Desktop required):

```bat
scripts\run-stack.cmd
```

The API starts at `http://127.0.0.1:8080`, PostgreSQL at `5432`, the Redpanda Kafka listener at `19092`, and HTTP Proxy at `18082`. Startup applies SQL migrations; committed outbox records are then relayed asynchronously to `risk-decisions`.

## API surface

| Endpoint | Purpose |
|---|---|
| `POST /v1/decisions` | score one event; duplicates return the original decision |
| `GET /v1/decisions/{eventId}` | retrieve auditable decision evidence |
| `GET /v1/replay/{accountId}` | replay stored events deterministically |
| `GET /v1/cases?status=open` | list human-review cases |
| `POST /v1/cases/{id}/resolve` | resolve with an expected version |
| `GET /v1/metrics` | request/action/latency snapshot |

## Engineering boundary

The included `logistic-baseline-v1` is a deterministic portfolio baseline, not a claim of fraud-detection accuracy. Shadow mode is the default: it records model output without changing decisions. Promotion to assist mode requires an offline labeled dataset, threshold evaluation, calibration, drift monitoring, and an explicit rollback rule.

See [architecture](docs/architecture.md), [M2–M7 learning roadmap](docs/learning-roadmap.md), [model card](docs/model-card.md), [reliability runbook](docs/runbook.md), and [benchmark protocol](docs/benchmark.md).

## Example

```bash
curl -X POST http://127.0.0.1:8080/v1/decisions \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-001","accountId":"acct-001","deviceId":"device-a","amount":120,"currency":"CNY","country":"CN","occurredAt":"2026-09-15T00:00:00Z"}'
```

## License

MIT. See [LICENSE](LICENSE).
