RS := services/battery-agg-rs
BIN := $(RS)/target/release/battery-agg-rs
SPARK_HOME ?= /opt/spark-dl/spark-3.5.3-bin-hadoop3
.PHONY: help golden rs-test parity-replay up kafka-demo k8s-validate demo down

help:          ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

golden:        ## (optional, needs Spark) regenerate goldens by running the LEGACY Scala/Spark job on the replays
	cd legacy/spark-jobs && mkdir -p target/classes && J=$(SPARK_HOME)/jars && \
	java -cp "$$J/scala-compiler-2.12.18.jar:$$J/scala-library-2.12.18.jar:$$J/scala-reflect-2.12.18.jar" scala.tools.nsc.Main -usejavacp -classpath "$$J/*" -d target/classes src/main/scala/lucid/telemetry/BatteryWindowAgg.scala && \
	(cd target/classes && jar cf ../legacy-spark-jobs.jar .) && \
	for f in battery_v1 edge_cases spark_semantics; do $(SPARK_HOME)/bin/spark-submit --class lucid.telemetry.ReplayGolden --master 'local[2]' target/legacy-spark-jobs.jar ../../fixtures/replays/$$f.jsonl ../../fixtures/golden/$$f.expected.jsonl; done

rs-test:       ## fmt, clippy -D warnings, unit + golden tests (same fixtures as Spark)
	cd $(RS) && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release

parity-replay: ## offline: Rust on the recorded replay vs Spark golden
	cd $(RS) && cargo build --release -q
	$(BIN) replay fixtures/replays/battery_v1.jsonl > reports/rust_replay.jsonl
	python3 tools/parity.py fixtures/golden/battery_v1.expected.jsonl reports/rust_replay.jsonl --report reports/parity_replay.md --title "Offline replay parity: Spark vs Rust"
	$(BIN) replay fixtures/replays/spark_semantics.jsonl > reports/rust_spark_semantics.jsonl
	python3 tools/parity.py fixtures/golden/spark_semantics.expected.jsonl reports/rust_spark_semantics.jsonl --report reports/parity_spark_semantics.md --title "Offline parity on Spark-semantics edge cases (nulls, wrong types, cross-window dedup, -0.0)"

up:            ## start Redpanda (Kafka API) locally
	docker compose up -d --wait

kafka-demo: up ## exactly-once under failure: crash mid-transaction, restart, verify parity + no duplicates
	cd $(RS) && cargo build --release -q
	./scripts/kafka_demo.sh

k8s-validate:  ## render base + every overlay with kustomize, then schema-validate (CRDs skipped)
	@for d in deploy/k8s/base deploy/k8s/overlays/*; do echo "== $$d"; out=$$(kustomize build $$d) || exit 1; printf '%s\n' "$$out" | kubeconform -strict -summary -ignore-missing-schemas - || exit 1; done

demo: rs-test parity-replay kafka-demo k8s-validate ## everything, in demo order
	@echo "\nAll demo checks passed. Reports in reports/."

down:
	docker compose down -v
