//! Kafka runtime: read_committed consumer + transactional producer.
//! Each batch commits outputs, DLQ records, the changed state (changelog) and the
//! input offsets in ONE transaction, so a crash at any point is exactly-once.

use crate::agg::{PartitionState, WindowEntry};
use crate::model::Event;
use crate::privacy::{dlq_record, log, vin_token, LogFields};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Write;
use std::time::{Duration, Instant};

pub type Res<T> = Result<T, Box<dyn Error>>;
/// (partition, key, value) of one committed record; `None` value = tombstone.
type Record = (i32, Option<Vec<u8>>, Option<Vec<u8>>);

pub struct Config {
    pub bootstrap: String,
    pub input: String,
    pub output: String,
    pub changelog: String,
    pub dlq: String,
    pub group: String,
    pub transactional_id: String,
    pub batch_size: usize,
    pub crash_after_txns: Option<usize>,
    pub drain_after_idle: Option<Duration>,
    /// Required by `run`; there is deliberately no default key.
    pub hmac_key: Option<Vec<u8>>,
    /// Cutover: windows starting before this are left to the previous producer, and
    /// partitions without committed offsets start at `emit_from_ms - start_lookback_ms`.
    pub emit_from_ms: Option<i64>,
    pub start_lookback_ms: i64,
    pub restore_timeout: Duration,
}

fn env_parse<T: std::str::FromStr>(k: &str) -> Res<Option<T>> {
    match std::env::var(k) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| format!("{k}: cannot parse {v:?}").into()),
        _ => Ok(None),
    }
}

impl Config {
    pub fn from_env() -> Res<Self> {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Ok(Config {
            bootstrap: env("KAFKA_BOOTSTRAP", "localhost:9092"),
            input: env("INPUT_TOPIC", "telemetry.battery.v1"),
            output: env("OUTPUT_TOPIC", "telemetry.battery-window-agg.v1"),
            changelog: env("CHANGELOG_TOPIC", "battery-agg-rs.changelog"),
            dlq: env("DLQ_TOPIC", "telemetry.battery.dlq.v1"),
            group: env("GROUP_ID", "battery-agg-rs"),
            transactional_id: env("TRANSACTIONAL_ID", "battery-agg-rs-0"),
            batch_size: env_parse("BATCH_SIZE")?.unwrap_or(250),
            crash_after_txns: env_parse("CRASH_AFTER_TXNS")?,
            drain_after_idle: env_parse("DRAIN_AFTER_IDLE_SECS")?.map(Duration::from_secs),
            hmac_key: std::env::var("VIN_TOKEN_KEY")
                .ok()
                .filter(|k| !k.is_empty())
                .map(String::into_bytes),
            emit_from_ms: env_parse("CUTOVER_EMIT_FROM_MS")?,
            start_lookback_ms: env_parse("START_LOOKBACK_MS")?.unwrap_or(3_600_000),
            restore_timeout: Duration::from_secs(env_parse("RESTORE_TIMEOUT_SECS")?.unwrap_or(300)),
        })
    }
}

pub fn setup(c: &Config) -> Res<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .create()?;
    let shadow = format!("{}.shadow", c.output);
    let topics = [
        NewTopic::new(&c.input, 6, TopicReplication::Fixed(1)),
        NewTopic::new(&c.output, 6, TopicReplication::Fixed(1)),
        NewTopic::new(&shadow, 6, TopicReplication::Fixed(1)),
        NewTopic::new(&c.changelog, 1, TopicReplication::Fixed(1)).set("cleanup.policy", "compact"),
        NewTopic::new(&c.dlq, 1, TopicReplication::Fixed(1)).set("retention.ms", "259200000"),
    ];
    for r in futures::executor::block_on(admin.create_topics(&topics, &AdminOptions::new()))? {
        match r {
            Ok(t) => log(LogFields {
                msg: "topic created",
                detail: Some(t),
                ..Default::default()
            }),
            Err((t, e)) => log(LogFields {
                msg: "topic exists or failed",
                detail: Some(format!("{t}: {e}")),
                ..Default::default()
            }),
        }
    }
    Ok(())
}

pub fn produce(c: &Config, path: &str) -> Res<()> {
    let p: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("enable.idempotence", "true")
        .create()?;
    let mut n = 0;
    for line in std::fs::read_to_string(path)?
        .lines()
        .filter(|l| !l.trim().is_empty())
    {
        let v = serde_json::from_str::<serde_json::Value>(line).ok();
        let field = |k: &str| v.as_ref().and_then(|v| v.get(k));
        let key = field("vin")
            .and_then(|x| x.as_str())
            .unwrap_or("malformed")
            .to_string();
        // Record timestamp = ingest time, as the ingest gateway would write it.
        let ts = field("ingest_ts_ms").and_then(|x| x.as_i64());
        send_at(&p, &c.input, Some(&key), Some(line), ts)?;
        n += 1;
    }
    p.flush(Duration::from_secs(30))?;
    log(LogFields {
        msg: "produced",
        count: Some(n),
        ..Default::default()
    });
    Ok(())
}

/// Reads every committed record of `topic`. Fails (instead of returning a partial read)
/// if the end of every partition is not reached within `timeout`.
fn read_all(c: &Config, topic: &str, idle: Duration, timeout: Duration) -> Res<Vec<Record>> {
    let cons: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("group.id", format!("reader-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()?;
    let md = cons.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let parts: Vec<i32> = md.topics()[0].partitions().iter().map(|p| p.id()).collect();
    let mut tpl = TopicPartitionList::new();
    for p in &parts {
        tpl.add_partition_offset(topic, *p, Offset::Beginning)?;
    }
    cons.assign(&tpl)?;
    let caught_up = || -> Res<bool> {
        let pos = cons.position()?;
        for p in &parts {
            let (low, high) = cons.fetch_watermarks(topic, *p, Duration::from_secs(5))?;
            let at = pos
                .find_partition(topic, *p)
                .and_then(|e| match e.offset() {
                    Offset::Offset(o) => Some(o),
                    _ => None,
                });
            if low < high && at.is_none_or(|o| o < high) {
                return Ok(false);
            }
        }
        Ok(true)
    };
    let mut out = Vec::new();
    let deadline = Instant::now() + timeout;
    let mut last = Instant::now();
    loop {
        if let Some(m) = cons.poll(Duration::from_millis(200)) {
            let m = m?;
            out.push((
                m.partition(),
                m.key().map(|k| k.to_vec()),
                m.payload().map(|v| v.to_vec()),
            ));
            last = Instant::now();
            continue;
        }
        if last.elapsed() >= idle && caught_up()? {
            return Ok(out);
        }
        if Instant::now() >= deadline {
            return Err(format!("{topic}: end of topic not reached within {timeout:?}").into());
        }
    }
}

/// Writes committed records of `topic` as JSON lines (partition order preserved).
pub fn dump(c: &Config, topic: &str, path: &str) -> Res<()> {
    let recs = read_all(c, topic, Duration::from_secs(4), Duration::from_secs(60))?;
    let mut f = std::fs::File::create(path)?;
    let mut n = 0;
    for v in recs.iter().filter_map(|r| r.2.as_ref()) {
        f.write_all(v)?;
        f.write_all(b"\n")?;
        n += 1;
    }
    log(LogFields {
        msg: "dumped",
        count: Some(n),
        detail: Some(topic.into()),
        ..Default::default()
    });
    Ok(())
}

fn meta_key(input: &str, p: i32) -> String {
    format!("{input}|{p}|meta")
}

fn window_key(input: &str, p: i32, start: i64, vin: &str) -> String {
    format!("{input}|{p}|{start}|{vin}")
}

/// Rebuilds partition states from the changelog: one meta record per partition plus one
/// record per window key (`Open`, `Closed`, or a tombstone).
fn load_state(c: &Config) -> Res<HashMap<i32, PartitionState>> {
    let mut st: HashMap<i32, PartitionState> = HashMap::new();
    let recs = read_all(c, &c.changelog, Duration::from_secs(3), c.restore_timeout)?;
    log(LogFields {
        msg: "changelog read",
        count: Some(recs.len()),
        ..Default::default()
    });
    for (_, k, v) in recs {
        let key = String::from_utf8(k.unwrap_or_default())?;
        let mut parts = key.splitn(4, '|');
        let (Some(topic), Some(p), Some(a)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(format!(
                "unrecognised changelog key format (partition {:?})",
                key.split('|').nth(1)
            )
            .into());
        };
        if topic != c.input {
            continue;
        }
        let state = st
            .entry(p.parse()?)
            .or_insert_with(|| PartitionState::new(c.emit_from_ms));
        match (a, parts.next(), v) {
            ("meta", None, Some(v)) => state.restore_meta(serde_json::from_slice(&v)?),
            (start, Some(vin), v) => {
                let entry: Option<WindowEntry> =
                    v.map(|v| serde_json::from_slice(&v)).transpose()?;
                state.restore_window((start.parse()?, vin.to_string()), entry);
            }
            _ => return Err("malformed changelog record".into()),
        }
    }
    Ok(st)
}

/// Sends the changelog delta of partition `p` (inside the current transaction).
fn write_changes(producer: &BaseProducer, c: &Config, p: i32, st: &mut PartitionState) -> Res<()> {
    let (meta, changes) = st.take_changes();
    for ((start, vin), entry) in changes {
        let value = entry.map(|e| serde_json::to_string(&e)).transpose()?;
        send(
            producer,
            &c.changelog,
            Some(&window_key(&c.input, p, start, &vin)),
            value.as_deref(),
        )?;
    }
    send(
        producer,
        &c.changelog,
        Some(&meta_key(&c.input, p)),
        Some(&serde_json::to_string(&meta)?),
    )
}

/// Assigns every input partition: committed offset if any, else the cutover start
/// (by timestamp) if configured, else the beginning.
fn assign_input(consumer: &BaseConsumer, c: &Config) -> Res<()> {
    let md = consumer.fetch_metadata(Some(&c.input), Duration::from_secs(10))?;
    let mut tpl = TopicPartitionList::new();
    for p in md.topics()[0].partitions() {
        tpl.add_partition(&c.input, p.id());
    }
    let committed = consumer.committed_offsets(tpl, Duration::from_secs(10))?;
    let mut assign = TopicPartitionList::new();
    let mut by_time = TopicPartitionList::new();
    for e in committed.elements() {
        match (e.offset(), c.emit_from_ms) {
            (Offset::Offset(o), _) => {
                assign.add_partition_offset(&c.input, e.partition(), Offset::Offset(o))?
            }
            (_, Some(from)) => by_time.add_partition_offset(
                &c.input,
                e.partition(),
                Offset::Offset(from - c.start_lookback_ms),
            )?,
            (_, None) => assign.add_partition_offset(&c.input, e.partition(), Offset::Beginning)?,
        }
    }
    if by_time.count() > 0 {
        for e in consumer
            .offsets_for_times(by_time, Duration::from_secs(10))?
            .elements()
        {
            let off = match e.offset() {
                Offset::Offset(o) => Offset::Offset(o),
                _ => Offset::End,
            };
            assign.add_partition_offset(&c.input, e.partition(), off)?;
            log(LogFields {
                msg: "cutover start offset",
                partition: Some(e.partition()),
                offset: off.to_raw(),
                ..Default::default()
            });
        }
    }
    consumer.assign(&assign)?;
    Ok(())
}

pub fn run(c: &Config) -> Res<()> {
    let hmac_key = c.hmac_key.as_deref().ok_or("VIN_TOKEN_KEY must be set")?;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("group.id", &c.group)
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("auto.offset.reset", "earliest")
        .create()?;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("transactional.id", &c.transactional_id)
        .set("enable.idempotence", "true")
        .set("transaction.timeout.ms", "60000")
        .create()?;
    producer.init_transactions(Duration::from_secs(30))?; // fences and aborts any txn left by a crashed instance

    let mut states = load_state(c)?;
    for st in states.values_mut() {
        st.set_emit_from(c.emit_from_ms);
    }
    assign_input(&consumer, c)?;
    log(LogFields {
        msg: "started",
        count: Some(states.len()),
        detail: Some("restored partition states".into()),
        ..Default::default()
    });

    let mut txns = 0usize;
    let mut idle_since = Instant::now();
    loop {
        let mut batch = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(500);
        while batch.len() < c.batch_size && Instant::now() < deadline {
            if let Some(m) = consumer.poll(Duration::from_millis(100)) {
                let m = m?;
                batch.push((
                    m.partition(),
                    m.offset(),
                    m.payload().unwrap_or_default().to_vec(),
                ));
            }
        }
        if batch.is_empty() {
            if c.drain_after_idle
                .is_some_and(|d| idle_since.elapsed() >= d)
            {
                producer.begin_transaction()?;
                let mut n = 0;
                for (p, st) in states.iter_mut() {
                    for w in st.drain() {
                        send(
                            &producer,
                            &c.output,
                            Some(&w.vin),
                            Some(&serde_json::to_string(&w)?),
                        )?;
                        n += 1;
                    }
                    write_changes(&producer, c, *p, st)?;
                }
                producer.commit_transaction(Duration::from_secs(30))?;
                log(LogFields {
                    msg: "drained and exiting",
                    count: Some(n),
                    ..Default::default()
                });
                return Ok(());
            }
            continue;
        }
        idle_since = Instant::now();

        let mut outputs = Vec::new();
        let mut dlq = Vec::new();
        let mut next: HashMap<i32, i64> = HashMap::new();
        let mut touched = BTreeSet::new();
        for (p, off, payload) in &batch {
            let st = states
                .entry(*p)
                .or_insert_with(|| PartitionState::new(c.emit_from_ms));
            match serde_json::from_slice::<Event>(payload) {
                Ok(e) => {
                    if e.cell_temp_c
                        .is_some_and(|t| t >= crate::agg::THERMAL_ALERT_C)
                    {
                        log(LogFields {
                            msg: "hot sample",
                            vin_token: Some(vin_token(&e.vin, hmac_key)),
                            partition: Some(*p),
                            offset: Some(*off),
                            ..Default::default()
                        });
                    }
                    outputs.extend(st.process(&e));
                }
                Err(err) => dlq.push(dlq_record(payload, &err, hmac_key)),
            }
            next.insert(*p, off + 1);
            touched.insert(*p);
        }

        producer.begin_transaction()?;
        for w in &outputs {
            send(
                &producer,
                &c.output,
                Some(&w.vin),
                Some(&serde_json::to_string(w)?),
            )?;
        }
        for d in &dlq {
            send(&producer, &c.dlq, None, Some(d))?;
        }
        for p in &touched {
            if let Some(st) = states.get_mut(p) {
                write_changes(&producer, c, *p, st)?;
            }
        }
        if c.crash_after_txns == Some(txns) {
            log(LogFields {
                msg: "CRASH INJECTED mid-transaction (outputs sent, not committed)",
                count: Some(outputs.len()),
                ..Default::default()
            });
            std::process::abort();
        }
        let mut otpl = TopicPartitionList::new();
        for (p, o) in &next {
            otpl.add_partition_offset(&c.input, *p, Offset::Offset(*o))?;
        }
        let gm = consumer.group_metadata().ok_or("no group metadata")?;
        producer.send_offsets_to_transaction(&otpl, &gm, Duration::from_secs(30))?;
        producer.commit_transaction(Duration::from_secs(30))?;
        txns += 1;
        log(LogFields {
            msg: "txn committed",
            count: Some(outputs.len()),
            detail: Some(format!("txn={txns} in={} dlq={}", batch.len(), dlq.len())),
            ..Default::default()
        });
    }
}

/// Enqueues one record; waits only while the local queue is full. Any other error
/// (e.g. message too large) is returned instead of being retried forever.
fn send(p: &BaseProducer, topic: &str, key: Option<&str>, payload: Option<&str>) -> Res<()> {
    send_at(p, topic, key, payload, None)
}

fn send_at(
    p: &BaseProducer,
    topic: &str,
    key: Option<&str>,
    payload: Option<&str>,
    timestamp_ms: Option<i64>,
) -> Res<()> {
    let mut rec: BaseRecord<str, str> = BaseRecord::to(topic);
    if let Some(ts) = timestamp_ms {
        rec = rec.timestamp(ts);
    }
    if let Some(k) = key {
        rec = rec.key(k);
    }
    if let Some(v) = payload {
        rec = rec.payload(v);
    }
    loop {
        match p.send(rec) {
            Ok(()) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                p.poll(Duration::from_millis(50));
                rec = r;
            }
            Err((e, _)) => return Err(format!("produce to {topic} failed: {e}").into()),
        }
    }
}
