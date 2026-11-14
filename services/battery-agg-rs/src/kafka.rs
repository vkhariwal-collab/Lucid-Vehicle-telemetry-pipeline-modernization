//! Kafka runtime: read_committed consumer + transactional producer.
//! Each batch commits outputs, DLQ records, a state snapshot (changelog) and the
//! input offsets in ONE transaction, so a crash at any point is exactly-once.

use crate::agg::PartitionState;
use crate::model::Event;
use crate::privacy::{dlq_record, log, vin_token, LogFields};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Write;
use std::time::{Duration, Instant};

pub type Res<T> = Result<T, Box<dyn Error>>;
/// (partition, key, value) of one committed record.
type Record = (i32, Option<Vec<u8>>, Vec<u8>);

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
    pub hmac_key: Vec<u8>,
}

impl Config {
    pub fn from_env() -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Config {
            bootstrap: env("KAFKA_BOOTSTRAP", "localhost:9092"),
            input: env("INPUT_TOPIC", "telemetry.battery.v1"),
            output: env("OUTPUT_TOPIC", "telemetry.battery-window-agg.v1"),
            changelog: env("CHANGELOG_TOPIC", "battery-agg-rs.changelog"),
            dlq: env("DLQ_TOPIC", "telemetry.battery.dlq.v1"),
            group: env("GROUP_ID", "battery-agg-rs"),
            transactional_id: env("TRANSACTIONAL_ID", "battery-agg-rs-0"),
            batch_size: env("BATCH_SIZE", "250").parse().unwrap_or(250),
            crash_after_txns: std::env::var("CRASH_AFTER_TXNS")
                .ok()
                .and_then(|v| v.parse().ok()),
            drain_after_idle: std::env::var("DRAIN_AFTER_IDLE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(Duration::from_secs),
            hmac_key: env("VIN_TOKEN_KEY", "demo-only-key").into_bytes(),
        }
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
        let key = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("vin").and_then(|x| x.as_str()).map(String::from))
            .unwrap_or_else(|| "malformed".into());
        let mut rec = BaseRecord::to(&c.input).key(&key).payload(line);
        loop {
            match p.send(rec) {
                Ok(()) => break,
                Err((_, r)) => {
                    p.poll(Duration::from_millis(50));
                    rec = r;
                }
            }
        }
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

fn read_all(c: &Config, topic: &str, idle: Duration) -> Res<Vec<Record>> {
    let cons: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &c.bootstrap)
        .set("group.id", format!("reader-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()?;
    let md = cons.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let mut tpl = TopicPartitionList::new();
    for p in md.topics()[0].partitions() {
        tpl.add_partition_offset(topic, p.id(), Offset::Beginning)?;
    }
    cons.assign(&tpl)?;
    let mut out = Vec::new();
    // Read until every partition's position reaches its high watermark (bounded), then until idle.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = Instant::now();
    loop {
        if let Some(m) = cons.poll(Duration::from_millis(200)) {
            let m = m?;
            out.push((
                m.partition(),
                m.key().map(|k| k.to_vec()),
                m.payload().unwrap_or_default().to_vec(),
            ));
            last = Instant::now();
            continue;
        }
        let caught_up = md.topics()[0].partitions().iter().all(|p| {
            let high = cons
                .fetch_watermarks(topic, p.id(), Duration::from_secs(5))
                .map(|w| w.1)
                .unwrap_or(0);
            let pos = cons
                .position()
                .ok()
                .and_then(|t| {
                    t.find_partition(topic, p.id())
                        .and_then(|e| e.offset().to_raw())
                })
                .unwrap_or(0);
            high == 0 || pos >= high
        });
        if (caught_up && last.elapsed() >= idle) || Instant::now() >= deadline {
            break;
        }
    }
    Ok(out)
}

/// Writes committed records of `topic` as JSON lines (partition order preserved).
pub fn dump(c: &Config, topic: &str, path: &str) -> Res<()> {
    let recs = read_all(c, topic, Duration::from_secs(4))?;
    let mut f = std::fs::File::create(path)?;
    for (_, _, v) in &recs {
        f.write_all(v)?;
        f.write_all(b"\n")?;
    }
    log(LogFields {
        msg: "dumped",
        count: Some(recs.len()),
        detail: Some(topic.into()),
        ..Default::default()
    });
    Ok(())
}

fn load_state(c: &Config) -> Res<HashMap<i32, PartitionState>> {
    let mut st = HashMap::new();
    let recs = read_all(c, &c.changelog, Duration::from_secs(3))?;
    log(LogFields {
        msg: "changelog read",
        count: Some(recs.len()),
        ..Default::default()
    });
    for (_, k, v) in recs {
        let key = String::from_utf8(k.unwrap_or_default())?;
        if let Some(p) = key.rsplit('-').next().and_then(|p| p.parse::<i32>().ok()) {
            st.insert(p, serde_json::from_slice(&v)?);
        }
    }
    Ok(st)
}

pub fn run(c: &Config) -> Res<()> {
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
    let md = consumer.fetch_metadata(Some(&c.input), Duration::from_secs(10))?;
    let mut tpl = TopicPartitionList::new();
    for p in md.topics()[0].partitions() {
        tpl.add_partition(&c.input, p.id());
    }
    let committed = consumer.committed_offsets(tpl, Duration::from_secs(10))?;
    let mut assign = TopicPartitionList::new();
    for e in committed.elements() {
        let off = match e.offset() {
            Offset::Offset(o) => Offset::Offset(o),
            _ => Offset::Beginning,
        };
        assign.add_partition_offset(&c.input, e.partition(), off)?;
    }
    consumer.assign(&assign)?;
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
                            &serde_json::to_string(&w)?,
                        );
                        n += 1;
                    }
                    send(
                        &producer,
                        &c.changelog,
                        Some(&format!("{}-{p}", c.input)),
                        &serde_json::to_string(st)?,
                    );
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
            let st = states.entry(*p).or_default();
            match serde_json::from_slice::<Event>(payload) {
                Ok(e) => {
                    if e.cell_temp_c >= crate::agg::THERMAL_ALERT_C {
                        log(LogFields {
                            msg: "hot sample",
                            vin_token: Some(vin_token(&e.vin, &c.hmac_key)),
                            partition: Some(*p),
                            offset: Some(*off),
                            ..Default::default()
                        });
                    }
                    outputs.extend(st.process(&e));
                }
                Err(err) => dlq.push(dlq_record(payload, &err.to_string(), &c.hmac_key)),
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
                &serde_json::to_string(w)?,
            );
        }
        for d in &dlq {
            send(&producer, &c.dlq, None, d);
        }
        for p in &touched {
            send(
                &producer,
                &c.changelog,
                Some(&format!("{}-{p}", c.input)),
                &serde_json::to_string(&states[p])?,
            );
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

fn send(p: &BaseProducer, topic: &str, key: Option<&str>, payload: &str) {
    let mut rec = BaseRecord::to(topic).payload(payload);
    if let Some(k) = key {
        rec = rec.key(k);
    }
    loop {
        match p.send(rec) {
            Ok(()) => return,
            Err((_, r)) => {
                p.poll(Duration::from_millis(50));
                rec = r;
            }
        }
    }
}
