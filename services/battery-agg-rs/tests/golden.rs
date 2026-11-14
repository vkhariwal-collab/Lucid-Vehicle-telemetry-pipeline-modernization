//! Runs the Rust port on the SAME recorded replays the legacy Spark job produced goldens from.
use battery_agg_rs::{
    agg::aggregate_all,
    model::{Event, WindowAgg},
};

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

fn check(name: &str) {
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures");
    let events: Vec<Event> = std::fs::read_to_string(format!("{base}/replays/{name}.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let golden: Vec<WindowAgg> =
        std::fs::read_to_string(format!("{base}/golden/{name}.expected.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
    let got = aggregate_all(&events);
    assert_eq!(got.len(), golden.len(), "{name}: window count");
    for (g, w) in got.iter().zip(&golden) {
        let ok = g.vin == w.vin
            && g.window_start_ms == w.window_start_ms
            && g.window_end_ms == w.window_end_ms
            && g.events == w.events
            && g.thermal_alert == w.thermal_alert
            && [
                (g.avg_cell_temp_c, w.avg_cell_temp_c),
                (g.max_cell_temp_c, w.max_cell_temp_c),
                (g.min_soc_pct, w.min_soc_pct),
                (g.max_power_kw, w.max_power_kw),
                (g.energy_kwh, w.energy_kwh),
                (g.last_lat, w.last_lat),
                (g.last_lon, w.last_lon),
            ]
            .iter()
            .all(|(a, b)| close(*a, *b));
        assert!(ok, "{name}: mismatch\n got: {g:?}\nwant: {w:?}");
    }
}

#[test]
fn golden_edge_cases() {
    check("edge_cases");
}

#[test]
fn golden_full_replay() {
    check("battery_v1");
}
