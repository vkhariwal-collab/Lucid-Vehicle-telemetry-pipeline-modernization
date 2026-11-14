# Spec: legacy `BatteryWindowAgg` (Scala/Spark Structured Streaming)

> **Reference output of Step 1.** Devin regenerates this live from `legacy/spark-jobs/`. All data is synthetic.
> Source: `legacy/spark-jobs/src/main/scala/lucid/telemetry/BatteryWindowAgg.scala`

## Behaviour (in evaluation order)
| # | Rule | Legacy code | Rust port |
|---|---|---|---|
| 1 | Drop rows with null `vin`, `event_id` or `event_ts_ms` (malformed JSON parses to nulls) | `transform` filter 1 | serde error → **DLQ** (intentional, TQ-04) |
| 2 | Drop late events: `ingest_ts_ms − event_ts_ms > 600 000` (exactly 600 000 is kept) | filter 2 | `agg.rs` `MAX_LATENESS_MS` |
| 3 | De-duplicate on `event_id`, **after** the late filter | `dropDuplicates("event_id")` | per-window seen-set (duplicates share `event_ts`) |
| 4 | Window = `floor(event_ts_ms / 300 000) × 300 000` (5-min tumbling, event time) | `window_start_ms` column | `div_euclid` |
| 5 | Per (VIN, window): count, avg/max cell temp, min SoC, max power, `energy = max − min`, last location by `(event_ts_ms, event_id)` | `groupBy.agg` | `WindowState` |
| 6 | Location rounded to 2 decimals, **HALF_UP on the shortest decimal form** (Spark `round`) | `F.round(lat, 2)` | `privacy::round2_half_up` |
| 7 | `thermal_alert = max_cell_temp_c ≥ 55.0` | select | `THERMAL_ALERT_C` |
| 8 | Streaming sink: `outputMode("update")`, so each micro-batch re-emits partial window values | `StreamingMain` | **final value once** (TQ-05) |

## Legacy quirks (preserved unless approved)
| ID | Quirk | Impact | Fixture |
|---|---|---|---|
| TQ-01 | Future-dated events (bad vehicle clock) are kept and create future windows | Phantom windows | `battery_v1` (0.5% of events, +40 min) |
| TQ-02 | `energy = max − min` is wrong when the session counter resets inside a window | Over- or under-stated kWh in warranty data | `edge_cases` x007 |
| TQ-03 | `-128.0` sensor-fault sentinel is included in the average temperature | Average temperature dragged down; alert unaffected | `battery_v1` |
| TQ-04 | Malformed records silently dropped, with no DLQ | No visibility into data loss | `edge_cases` x011/x012 |
| TQ-05 | Update mode emits several versions of a window; consumers must keep the "last" one | Rust emits once, when final (latency +10 min). **Ask consumers** | n/a |

## Questions for the architect
1. Do consumers depend on partial updates (TQ-05), or only on final values?
2. Which of TQ-01…03 do service and warranty already correct downstream?
3. The Spark checkpoint can't be migrated. Is replaying from offset (window start − 10 min) acceptable at cutover?
