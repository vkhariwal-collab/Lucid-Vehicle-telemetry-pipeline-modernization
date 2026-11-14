# 15-minute run of show (telemetry)

Pre-flight:
- `make up` (Redpanda).
- `make demo` once (must end with "All demo checks passed").
- Open the recorded Devin sessions as fallback.

| Time | Beat | Do | Line to land |
|---|---|---|---|
| 0:00 | Frame | Show the Scala job and the Rust skeleton. "One job ported, N to go." | "Your bottleneck is porting throughput, not design." |
| 1:30 | **Capture** | Prompt 1: Devin writes `docs/spec/battery_window_agg.md`, including the Spark subtleties (late filter before dedupe, HALF_UP rounding, update mode) and quirks TQ-01…05. | "These are decisions for you to make. Devin doesn't silently change semantics." |
| 4:00 | **Lock** | Show replays plus goldens produced by the **real Spark job** (`make golden`). | "Spark itself is the oracle." |
| 6:00 | **Port** | Prompt 2: Devin's Rust PR: pure `agg.rs`, `privacy.rs`, transactional `kafka.rs`. | "Your conventions, clippy -D warnings, no PII in logs by construction." |
| 8:00 | **Prove** | `make rs-test parity-replay`: 632/632. | "Same replays, same answers." |
| 9:30 | **Break it** | `make kafka-demo`: crash mid-transaction (exit 134), restart, 632/632, 0 duplicates, 0 ordering violations, DLQ sanitized. | "Exactly-once holds through a crash." |
| 12:30 | Privacy | `privacy/template.yaml` plus tests. | "One review covers every job." |
| 13:30 | Cutover | Overlays: shadow → dual-run → cutover. | "Consumers never change. Rollback means restarting Spark." |
| 14:30 | Hand back | Ask. | "Pick 3 jobs (filter, window, join) for a pilot. What are your exit criteria?" |
