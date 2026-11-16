# lucid-telemetry-migration-demo

A synthetic, runnable demo of moving a vehicle-telemetry job from **Scala/Spark Structured Streaming** to a **Rust stream processor on Kubernetes**. It keeps the Kafka contract, ordering and exactly-once guarantees, and covers privacy with one reviewed template. **No Lucid code or data.**

## Method: capture → lock → port → prove → cut over
| Step | Where | Check |
|---|---|---|
| Capture: spec of the Spark job, quirks TQ-01…05 | `docs/spec/battery_window_agg.md` | review |
| Lock: replays plus goldens from the **real Spark job** | `fixtures/`, `legacy/spark-jobs/` | `make golden` (needs Spark) |
| Port: pure aggregation, privacy module, transactional Kafka loop | `services/battery-agg-rs/` | `make rs-test` |
| Prove: offline parity, plus a crash mid-transaction on Redpanda | `tools/parity.py`, `scripts/kafka_demo.sh` | `make parity-replay kafka-demo` |
| Cut over: shadow → dual-run → cutover overlays | `deploy/k8s/`, `docs/adr/0001` | `make k8s-validate` |

## Quick start
Requirements: Docker, Rust (stable, >= 1.85), cmake + libcurl dev headers (for rdkafka), Python 3, kustomize + kubeconform (for `k8s-validate`). Spark 3.5 + JDK 17 are needed only to regenerate goldens.
```bash
make demo   # rs-test, parity-replay (632/632), kafka-demo (crash + restart, 632/632, 0 dupes), k8s-validate
```

## Stubs and simplifications
- Single replica (one `transactional.id`). Multi-replica uses partition-pinned replicas.
- Redpanda stands in for MSK or Confluent.
- One job is shown; the pilot fans the playbook out across jobs.
