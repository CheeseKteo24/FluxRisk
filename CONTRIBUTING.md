# Contributing

1. Open an issue describing the invariant or user outcome before a large change.
2. Create a focused branch, add an executable specification for behavior, and keep infrastructure behind interfaces in `FluxRisk.Core`.
3. Run `scripts\verify.cmd`. If PostgreSQL or streaming behavior changed, also run the Docker stack and document the failure test you performed.
4. Update architecture/runbook/model-card documentation when a boundary changes.
5. Open a pull request using the checklist. Never include secrets, private data, or fabricated benchmark/model claims.

Changes should preserve idempotency, deterministic replay, historical audit evidence, and the default-safe shadow model mode.
