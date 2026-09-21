# Reliability runbook

## Triage order

1. Check `/health` for storage, streaming, and model mode.
2. Check `/v1/metrics` for error rate and p95/p99 movement.
3. Separate request failures from delayed broker publication: the outbox deliberately decouples them.
4. Inspect unpublished/dead-letter outbox rows and their last error.
5. Replay one affected account before broad remediation.

## Broker unavailable

Decisions continue committing with outbox rows. The relay releases failed rows with exponential delay. Restore Redpanda, confirm health, then verify `published_at` and `broker_metadata`. Do not manually delete rows; dead-letter records are evidence.

## Database unavailable

The API cannot safely decide because idempotency, windows, and audit would diverge. Fail closed at the service boundary, avoid blind retries, restore PostgreSQL, and verify migrations before traffic resumes.

## Latency regression

Compare p50 with p95/p99. A tail-only increase usually indicates contention, queueing, timeouts, or a slow dependency. Reproduce with the same load profile, check account-key skew, PostgreSQL locks, relay backlog, and resource saturation.

## Incorrect decision investigation

Retrieve the decision by event ID. Preserve its feature snapshot, rule hits, model version, watermark, and timestamp. Replay the account in event-time order. If replay differs, treat it as a correctness incident; do not merely retune the threshold.

## Rollback

Set `RiskModel__Mode=Shadow` to remove model influence without losing observations. Rules and model versions are evidence and should not be rewritten in historical records.
