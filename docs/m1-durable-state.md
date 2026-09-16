# M1: durable state and transactional outbox

## Goal

M1 makes a risk decision survive process restarts without weakening idempotency or sliding-window correctness. It deliberately does not publish to Kafka yet. Instead, it commits a pending outbox record in the same transaction as the event and decision, giving M2 a reliable source to publish and retry.

## Code boundaries

- `FluxRisk.Core/Persistence.cs` defines storage-neutral transaction and session contracts.
- `InMemoryRiskDecisionStore` keeps the zero-infrastructure development loop.
- `PostgresRiskDecisionStore` owns PostgreSQL connections, transactions, advisory locks, parameterized SQL, and serialization.
- `DatabaseMigrator` applies embedded SQL exactly once under a migration advisory lock.
- `RiskDecisionEngine` owns validation, feature construction, rules, score thresholds, and the outbox event contract. It contains no PostgreSQL code.

This boundary matters: business decisions can be tested without a database, while persistence guarantees receive separate integration tests against a real PostgreSQL instance.

## Transaction invariant

For every accepted unique event, one transaction performs:

1. acquire the event-id lock;
2. acquire the account lock;
3. return the existing decision if this is a duplicate;
4. load the account's retained event-time window;
5. calculate features, rule hits, score, and action;
6. insert `risk_events`;
7. insert `risk_decisions` with feature and rule evidence;
8. insert one pending `risk.decision.v1` outbox message;
9. commit.

If any insert fails, PostgreSQL rolls back all three records. A duplicate returns the original decision and creates neither a new event nor a new outbox message.

## Schema

- `risk_events` is the immutable input ledger and owns the globally unique `event_id`.
- `risk_decisions` is the auditable output, including JSONB features and rule hits.
- `outbox_messages` stores publishable events plus retry metadata.
- `schema_migrations` records applied embedded migrations.

The `(account_id, occurred_at)` index supports window reads. The partial outbox index contains only unpublished rows, keeping the future publisher's hot query small.

All timestamps are normalized to UTC microsecond precision at the domain boundary. PostgreSQL `timestamptz` stores microseconds while .NET exposes 100-nanosecond ticks; normalizing once keeps in-memory evaluation, persisted decisions, and deterministic replay byte-for-byte consistent.

## Windows CMD verification

Fast verification without Docker:

```bat
cd /d C:\path\to\FluxRisk
scripts\verify.cmd
```

Expected results include `5/5 specs passed`, zero build warnings/errors, and an explicit PostgreSQL `SKIP` when no connection is configured.

Full integration verification with Docker Desktop running:

```bat
scripts\verify-postgres.cmd
```

This starts only PostgreSQL, exports `FLUXRISK_POSTGRES`, and verifies persistence across new engine instances, duplicate outbox suppression, durable window state, and cross-account event ownership.

Run the complete API plus PostgreSQL stack:

```bat
scripts\run-stack.cmd
```

Then open another CMD window:

```bat
curl http://127.0.0.1:8080/health
curl -X POST http://127.0.0.1:8080/v1/decisions -H "Content-Type: application/json" -d "{\"eventId\":\"cmd-001\",\"accountId\":\"account-001\",\"deviceId\":\"device-a\",\"amount\":120,\"currency\":\"CNY\",\"country\":\"CN\",\"occurredAt\":\"2026-09-16T00:00:00Z\"}"
curl http://127.0.0.1:8080/v1/decisions/cmd-001
```

The health response should report `"storage":"postgres"`. Stop the stack with `docker compose down`; add `-v` only when you intentionally want to delete the database volume.

## Interview-level learning points

- Idempotency is a data invariant, not merely an HTTP retry feature.
- A database transaction solves atomicity, while per-key advisory locks solve concurrent ordering; they address different problems.
- The transactional outbox closes the database/message-broker dual-write gap.
- Ports and adapters keep risk policy independent from storage technology.
- Integration tests must recreate process boundaries; constructing a new engine proves the result did not survive only in memory.
