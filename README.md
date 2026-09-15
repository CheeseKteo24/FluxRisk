# FluxRisk

> An explainable, replay-safe real-time risk decision platform.

FluxRisk evaluates payment and login events under low latency, producing an `allow`, `review`, or `block` decision with the exact window features and rules that caused it. It is the systems-engineering complement to QueryWeaver: event-driven processing, concurrency, idempotency, state, replay, and observability instead of RAG and Text-to-SQL.

## Current M0 slice

- strongly typed risk events and decisions;
- five-minute, one-hour, and 24-hour event-time features;
- composable amount, velocity, device, and country rules;
- deterministic score-to-action policy;
- globally owned idempotency keys;
- per-account concurrency isolation;
- duplicate delivery returns the original decision without double-counting;
- ASP.NET Core ingestion and decision lookup API;
- dependency-free executable specifications.

## Run

```bash
dotnet build
dotnet run --project tests/FluxRisk.Specs
dotnet run --project src/FluxRisk.Api --urls http://127.0.0.1:8080
```

Submit an event:

```bash
curl -X POST http://127.0.0.1:8080/v1/decisions \
  -H "Content-Type: application/json" \
  -d '{
    "eventId":"evt-001",
    "accountId":"acct-001",
    "deviceId":"device-a",
    "amount":120,
    "currency":"CNY",
    "country":"CN",
    "occurredAt":"2026-09-15T00:00:00Z"
  }'
```

## Roadmap

- **M0 — Decision core:** rules, window features, idempotency, concurrency, API.
- **M1 — Durable state:** PostgreSQL event/audit store, migrations, transactional outbox.
- **M2 — Streaming:** Kafka/Redpanda partitions, consumer groups, retry and dead-letter topics.
- **M3 — Event-time correctness:** watermark, late/out-of-order events, deterministic replay.
- **M4 — Risk model:** calibrated anomaly scorer, shadow deployment, drift and threshold evaluation.
- **M5 — Reliability:** OpenTelemetry, SLOs, backpressure, load/chaos tests.
- **M6 — Product:** live decision dashboard, case review workflow, Docker deployment.
- **M7 — Open source:** benchmark report, demo video, runbooks, contributor workflow.

## What you will learn

Account-partitioned concurrency, sliding windows, idempotent event handling, audit-friendly decisions, eventual consistency, stream replay, outbox patterns, latency percentiles, model/rule fusion, and production failure analysis.

## Status

M0 is intentionally in-memory and rule-based. It proves decision semantics and concurrency invariants before adding Kafka, databases, or machine learning.

