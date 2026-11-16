//! battery-agg-rs: replay | setup | produce | run | dump
use battery_agg_rs::{agg, kafka, model::Event, privacy};
use std::io::{BufRead, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, usage: &str| -> kafka::Res<String> {
        args.get(i).cloned().ok_or_else(|| usage.into())
    };
    let r = match args.get(1).map(String::as_str) {
        Some("replay") => arg(2, "replay <file.jsonl>").and_then(|f| replay(&f)),
        Some(cmd @ ("setup" | "produce" | "run" | "dump")) => {
            kafka::Config::from_env().and_then(|cfg| match cmd {
                "setup" => kafka::setup(&cfg),
                "produce" => arg(2, "produce <file.jsonl>").and_then(|f| kafka::produce(&cfg, &f)),
                "run" => kafka::run(&cfg),
                _ => {
                    let topic = arg(2, "dump <topic> <out>")?;
                    kafka::dump(&cfg, &topic, &arg(3, "dump <topic> <out>")?)
                }
            })
        }
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
