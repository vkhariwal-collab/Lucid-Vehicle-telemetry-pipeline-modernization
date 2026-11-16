# ADR 0001: Exactly-once Rust processor and zero-disruption cutover (synthetic demo)

## Decision
1. **Exactly-once:** read_committed consumer plus a transactional producer. Each batch commits outputs, DLQ records, the **changed state** (compacted changelog: one record per window key plus one per-partition meta record, so a transaction writes only what changed) and input offsets in **one** transaction. A crash before commit means nothing is visible. On restart, `init_transactions` fences the zombie; state comes from the changelog and processing resumes from committed offsets.
2. **Ordering:** both topics are keyed by VIN. Windows are emitted per VIN in `window_start` order (finalisation depends only on window start).
3. **No Spark state migration:** at cutover set `CUTOVER_EMIT_FROM_MS` = window start of (Spark stop − window − lateness bound). Rust emits only windows starting at or after it; these final values supersede Spark's last partial updates (consumers already keep the last version, TQ-05). Partitions without committed offsets start at that time minus `START_LOOKBACK_MS` (default 1 h, for fast vehicle clocks, TQ-01). Shadow/dual-run use their own changelog topic, so cutover never restores shadow state. Parity on the overlap proves it.
4. **Cutover without touching consumers:** shadow topic (continuous parity), then one consumer on shadow behind a flag (dual-run), then stop Spark and point Rust at the real topic. Rollback is to stop Rust and restart Spark from its checkpoint.
5. **Exactly one record per window:** a final window is remembered (`Closed` changelog entry) for 1 h of ingest time, so a record with a skewed ingest timestamp is counted as `closed_dropped` instead of producing a second record.
6. **Scaling:** one `transactional.id` per replica, with partitions pinned per replica. `Recreate` strategy, so there are never two writers.
7. **Privacy:** one reviewed template (`privacy/template.yaml`) inherited by every job.

## Evidence (this repo)
- Golden parity: Rust = Spark on the same replays (632 windows plus edge cases).
- `make kafka-demo`: crash injected mid-transaction, then restart. 632/632 windows, 0 duplicates, 0 ordering violations.
