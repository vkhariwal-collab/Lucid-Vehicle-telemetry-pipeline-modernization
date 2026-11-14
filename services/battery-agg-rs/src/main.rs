//! battery-agg-rs: replay | setup | produce | run | dump
use battery_agg_rs::{agg, kafka, model::Event, privacy};
use std::io::{BufRead, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg = kafka::Config::from_env();
    let r = match args.get(1).map(String::as_str) {
        Some("replay") => replay(args.get(2).expect("replay <file.jsonl>")),
        Some("setup") => kafka::setup(&cfg),
        Some("produce") => kafka::produce(&cfg, args.get(2).expect("produce <file.jsonl>")),
        Some("run") => kafka::run(&cfg),
        Some("dump") => kafka::dump(
            &cfg,
            args.get(2).expect("dump <topic> <out>"),
            args.get(3).expect("dump <topic> <out>"),
        ),
        _ => {
            eprintln!("usage: battery-agg-rs replay <file> | setup | produce <file> | run | dump <topic> <out>");
            std::process::exit(2)
        }
    };
    if let Err(e) = r {
        privacy::log(privacy::LogFields {
            msg: "fatal",
            detail: Some(e.to_string()),
            ..Default::default()
        });
        std::process::exit(1);
    }
}

/// Offline replay: reads a topic dump, writes final windows (sorted like the legacy golden) to stdout.
fn replay(path: &str) -> kafka::Res<()> {
    let f = std::io::BufReader::new(std::fs::File::open(path)?);
    let mut events = Vec::new();
    let mut bad = 0;
    for line in f.lines() {
        let line = line?;
        match serde_json::from_str::<Event>(&line) {
            Ok(e) => events.push(e),
            Err(_) if line.trim().is_empty() => {}
            Err(_) => bad += 1,
        }
    }
    let out = agg::aggregate_all(&events);
    let mut w = std::io::BufWriter::new(std::io::stdout());
    for a in &out {
        writeln!(w, "{}", serde_json::to_string(a)?)?;
    }
    privacy::log(privacy::LogFields {
        msg: "replay done",
        count: Some(out.len()),
        detail: Some(format!("dlq={bad}")),
        ..Default::default()
    });
    Ok(())
}
