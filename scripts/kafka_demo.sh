#!/usr/bin/env bash
# Exactly-once under failure: load the replay into Kafka, crash the processor
# mid-transaction, restart it, then verify the committed output against the Spark golden.
set -uo pipefail
BIN=services/battery-agg-rs/target/release/battery-agg-rs
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
RUN=$(date +%s)
export INPUT_TOPIC=telemetry.battery.v1.$RUN OUTPUT_TOPIC=telemetry.battery-window-agg.v1.$RUN \
       CHANGELOG_TOPIC=battery-agg-rs.changelog.$RUN DLQ_TOPIC=telemetry.battery.dlq.v1.$RUN \
       GROUP_ID=battery-agg-rs.$RUN TRANSACTIONAL_ID=battery-agg-rs-0.$RUN BATCH_SIZE=${BATCH_SIZE:-250}
echo "== 1. create topics (run $RUN)"; $BIN setup 2>&1 | grep -c created | xargs echo "topics created:"
echo "== 2. produce replay ($(wc -l < fixtures/replays/battery_v1.jsonl) records, keyed by VIN, 6 partitions)"; $BIN produce fixtures/replays/battery_v1.jsonl
echo "== 3. run with crash injected during transaction #5"
CRASH_AFTER_TXNS=4 $BIN run 2>&1 | grep -E "txn committed|CRASH" ; echo "exit code: ${PIPESTATUS[0]} (134 = abort)"
echo "== 4. restart: fences the zombie txn, restores state from changelog, resumes from committed offsets"
DRAIN_AFTER_IDLE_SECS=5 $BIN run 2>&1 | grep -E "changelog read|started|drained" 
echo "== 5. read committed output and DLQ"
$BIN dump "$OUTPUT_TOPIC" reports/kafka_output.jsonl
$BIN dump "$DLQ_TOPIC" reports/kafka_dlq.jsonl
echo "DLQ records: $(wc -l < reports/kafka_dlq.jsonl) (no location, no raw VIN):"; head -2 reports/kafka_dlq.jsonl
echo "== 6. parity vs legacy Spark golden"
python3 tools/parity.py fixtures/golden/battery_v1.expected.jsonl reports/kafka_output.jsonl \
  --report reports/parity_kafka_crash.md --title "Kafka exactly-once parity after injected crash"
