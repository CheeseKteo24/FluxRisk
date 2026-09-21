# Reproducible benchmark protocol

FluxRisk does not publish made-up universal throughput numbers. Record results for your own machine using this protocol.

1. Record OS, CPU, memory, .NET version, Git commit, storage mode, model mode, request count, concurrency, and event-key distribution.
2. Warm up with 200 requests.
3. Run at least three trials: `scripts\load-test.cmd http://127.0.0.1:8080 5000 32`.
4. Report median throughput and each trial's error rate, p50, p95, and p99.
5. Verify `/v1/metrics` and sample stored decisions after the run; speed without correctness is a failed benchmark.
6. Repeat against PostgreSQL only after resetting or using unique event IDs. Label memory and PostgreSQL results separately.

The load tool uses bounded client concurrency and unique idempotency keys. It measures client-observed latency. For rigorous distributed benchmarking, also measure server spans and account for coordinated omission.
