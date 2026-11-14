# Prompts used live

**1. Capture**
> Read `legacy/spark-jobs/.../BatteryWindowAgg.scala`. Write `docs/spec/battery_window_agg.md` with:
> - every rule, in evaluation order;
> - Spark-specific semantics (null handling, dedupe order, `round` mode, output mode);
> - quirks marked as NEEDS-REVIEW.
>
> Don't modify `legacy/`.

**2. Port**
> Port the job into `services/battery-agg-rs` following AGENTS.md:
> - a pure aggregation module;
> - the shared privacy module;
> - a transactional Kafka loop that commits outputs, a state snapshot and offsets in one transaction.
>
> Preserve every quirk. Make `tests/golden.rs` pass on `fixtures/` (goldens come from Spark). Gates: fmt, clippy -D warnings, tests, `make parity-replay`, `make kafka-demo`.

**3. Scale out (pilot)**
> For each job in `jobs.txt`, start a session using this playbook: spec, then a golden run on Spark, then the Rust port, then parity. Open one PR per job with its parity report.
