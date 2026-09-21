# M2–M7 learning roadmap

This roadmap is evidence-driven. For every milestone, first predict the failure, then run the verification, and finally explain the invariant without reading the code.

## M2 — streaming and delivery semantics

**Read:** `Streaming.cs`, `PostgresRiskDecisionStore.cs`, `RedpandaHttpPublisher.cs`, migration `002`.

1. Draw the dual-write failure: database commit succeeds but direct broker publish fails.
2. Trace how one database transaction writes the decision and outbox row.
3. Run the full stack and stop Redpanda while submitting events. Decisions must remain committed and unpublished rows must remain retryable.
4. Restart Redpanda. Observe relay recovery; inspect `attempts`, `available_at`, `published_at`, and `broker_metadata`.
5. Change `MaxAttempts` to two in a test and prove a poison message becomes dead-lettered.

**You pass when:** you can explain at-least-once delivery, why consumers still need idempotency, lease ownership, exponential backoff, and why “exactly once” is usually an end-to-end business invariant rather than a broker switch.

## M3 — event-time correctness and replay

1. Submit events at minute 10 and minute 9 for the same account. Minute 9 is out of order but inside the two-minute allowance.
2. Submit minute 7 after minute 10. Confirm HTTP 422 and inspect the returned watermark.
3. Call `/v1/replay/{accountId}` and compare replayed features, actions, and model version with stored evidence.
4. Modify the tie-breaker or lateness threshold and add a regression spec before changing behavior.

**You pass when:** you can distinguish event time, processing time, maximum observed time, watermark, late data, and deterministic replay.

## M4 — model lifecycle, not merely model inference

1. Read `CalibratedAnomalyModel`; calculate one logit and sigmoid result by hand.
2. Run in `Shadow`: verify `ModelAssessment` exists but the rule score is unchanged.
3. Run with `RiskModel__Mode=Assist`: verify the contribution changes the combined score.
4. Create a small labeled CSV offline and report PR-AUC, recall at a fixed review rate, calibration error, and subgroup false-positive rates.
5. Write a promotion gate and rollback threshold before considering Assist mode.

**You pass when:** you can explain feature parity, leakage, calibration, threshold trade-offs, shadow/canary rollout, drift, and rollback. The included model is intentionally a baseline; accuracy claims require data.

## M5 — reliability and performance

1. Start the memory API and run `scripts\load-test.cmd http://127.0.0.1:8080 1000 32`.
2. Save throughput, errors, p50, p95, and p99 with CPU/runtime/storage details.
3. Repeat against PostgreSQL. Do not compare numbers unless hardware, request mix, warm-up, and concurrency match.
4. Inject broker failure, database failure, stale case versions, duplicates, and too-late events.
5. Use `/metrics` to define an SLO and alert budget; tail latency matters more than the average.

**You pass when:** you can diagnose saturation, queueing, backpressure, timeout budgets, retry storms, circuit breaking, and coordinated omission.

## M6 — product and human-in-the-loop loop

1. Open the dashboard, submit a high-value event, and explain every feature/rule/model field.
2. Find the generated review case and resolve it with the visible version.
3. Attempt the same version twice; the second write must fail rather than overwrite another reviewer.
4. Replay the account and show how an operator can reconstruct the decision.

**You pass when:** a reviewer can move from alert → evidence → disposition → audit trail without database access.

## M7 — open-source delivery

1. Run all commands from a clean clone, not your IDE cache.
2. Open a deliberately failing pull request and read CI output.
3. File one issue using the template; fix it on a feature branch and use the PR checklist.
4. Execute the benchmark protocol and record actual environment-specific numbers rather than invented claims.
5. Prepare a three-minute demo: failure scenario first, invariant second, code third, metrics last.

**You pass when:** another developer can reproduce, test, diagnose, and contribute without asking you privately.
