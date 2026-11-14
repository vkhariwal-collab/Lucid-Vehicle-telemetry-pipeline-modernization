# Instructions for Devin (and humans)

## Non-negotiables
- Never change `legacy/`. Goldens are produced by the legacy Spark job (`make golden`) and are the contract.
- Port quirks as-is. A semantic fix ships as a separate PR behind a flag, with a parity report showing only the intended diff.
- Privacy template (`privacy/template.yaml`):
  - Log only through `privacy::LogFields`.
  - Never add VIN or location to logs, the DLQ or new outputs without review.
  - Coarsen location before it enters state.

## Rust conventions
- Stable Rust. `cargo fmt`; `cargo clippy --all-targets -D warnings`.
- No `unwrap` on I/O paths; errors propagate.
- Pure logic in `agg.rs` (no clock, no I/O); all Kafka effects in `kafka.rs`.
- Outputs, DLQ, state snapshots and offsets are committed in one transaction. Never commit offsets outside it.

## Gates before a PR
`make rs-test parity-replay kafka-demo k8s-validate` must all pass. Attach `reports/parity_*.md`.
