//! Pure, deterministic port of `BatteryWindowAgg.transform` (legacy Scala/Spark).
//! No I/O, no clock: results depend only on the input events.

use crate::model::{Event, WindowAgg};
use crate::privacy::round2_half_up;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const WINDOW_MS: i64 = 300_000;
pub const MAX_LATENESS_MS: i64 = 600_000;
pub const THERMAL_ALERT_C: f64 = 55.0;

type WindowKey = (String, i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowState {
    events: i64,
    sum_temp: f64,
    max_temp: f64,
    min_soc: f64,
    max_power: f64,
    min_energy: f64,
    max_energy: f64,
    last_ts: i64,
    last_id: String,
    last_lat: f64,
    last_lon: f64,
    seen: BTreeSet<String>,
}

impl WindowState {
    fn new(e: &Event) -> Self {
        WindowState {
            events: 0,
            sum_temp: 0.0,
            max_temp: f64::MIN,
            min_soc: f64::MAX,
            max_power: f64::MIN,
            min_energy: f64::MAX,
            max_energy: f64::MIN,
            last_ts: i64::MIN,
            last_id: String::new(),
            last_lat: 0.0,
            last_lon: 0.0,
            seen: BTreeSet::from([e.event_id.clone()]),
        }
    }

    fn add(&mut self, e: &Event) {
        self.events += 1;
        self.sum_temp += e.cell_temp_c; // legacy quirk TQ-03: -128.0 sensor sentinel is averaged in
        self.max_temp = self.max_temp.max(e.cell_temp_c);
        self.min_soc = self.min_soc.min(e.soc_pct);
        self.max_power = self.max_power.max(e.power_kw);
        self.min_energy = self.min_energy.min(e.energy_kwh);
        self.max_energy = self.max_energy.max(e.energy_kwh);
        if (e.event_ts_ms, e.event_id.as_str()) > (self.last_ts, self.last_id.as_str()) {
            self.last_ts = e.event_ts_ms;
            self.last_id = e.event_id.clone();
            self.last_lat = round2_half_up(e.lat);
            self.last_lon = round2_half_up(e.lon);
        }
    }

    fn finish(&self, vin: &str, start: i64) -> WindowAgg {
        WindowAgg {
            vin: vin.to_string(),
            window_start_ms: start,
            window_end_ms: start + WINDOW_MS,
            events: self.events,
            avg_cell_temp_c: self.sum_temp / self.events as f64,
            max_cell_temp_c: self.max_temp,
            min_soc_pct: self.min_soc,
            max_power_kw: self.max_power,
            energy_kwh: self.max_energy - self.min_energy, // legacy quirk TQ-02: counter reset
            last_lat: self.last_lat,
            last_lon: self.last_lon,
            thermal_alert: self.max_temp >= THERMAL_ALERT_C,
        }
    }
}

/// Aggregation state for one input partition. Serializable so it can be
/// checkpointed transactionally to the changelog topic.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionState {
    #[serde(with = "window_map")]
    windows: BTreeMap<WindowKey, WindowState>,
    ingest_watermark: i64,
    pub late_dropped: u64,
    pub duplicates: u64,
}

impl PartitionState {
    /// Applies one event; returns windows that became final (none can change any more).
    pub fn process(&mut self, e: &Event) -> Vec<WindowAgg> {
        self.ingest_watermark = self.ingest_watermark.max(e.ingest_ts_ms);
        if e.ingest_ts_ms - e.event_ts_ms > MAX_LATENESS_MS {
            self.late_dropped += 1; // legacy: filter before dropDuplicates
        } else {
            let start = e.event_ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS;
            let key = (e.vin.clone(), start);
            match self.windows.get_mut(&key) {
                Some(w) if w.seen.contains(&e.event_id) => self.duplicates += 1,
                Some(w) => {
                    w.seen.insert(e.event_id.clone());
                    w.add(e);
                }
                None => {
                    let mut w = WindowState::new(e);
                    w.add(e);
                    self.windows.insert(key, w);
                }
            }
        }
        self.finalize(false)
    }

    /// A window is final once ingest time has passed its end plus the lateness bound:
    /// any later event for it would be dropped as late.
    fn finalize(&mut self, all: bool) -> Vec<WindowAgg> {
        let wm = self.ingest_watermark;
        let done: Vec<WindowKey> = self
            .windows
            .keys()
            .filter(|(_, s)| all || s + WINDOW_MS + MAX_LATENESS_MS <= wm)
            .cloned()
            .collect();
        let mut out: Vec<WindowAgg> = done
            .into_iter()
            .map(|k| {
                let w = self.windows.remove(&k).expect("key exists");
                w.finish(&k.0, k.1)
            })
            .collect();
        out.sort_by(|a, b| (a.window_start_ms, &a.vin).cmp(&(b.window_start_ms, &b.vin)));
        out
    }

    /// Emits every open window (end of replay / graceful drain).
    pub fn drain(&mut self) -> Vec<WindowAgg> {
        self.finalize(true)
    }

    pub fn open_windows(&self) -> usize {
        self.windows.len()
    }
}

/// JSON object keys must be strings, so the window map is (de)serialized as a list of entries.
mod window_map {
    use super::{WindowKey, WindowState};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(
        m: &BTreeMap<WindowKey, WindowState>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        m.iter().collect::<Vec<_>>().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<WindowKey, WindowState>, D::Error> {
        Ok(Vec::<(WindowKey, WindowState)>::deserialize(d)?
            .into_iter()
            .collect())
    }
}

/// Batch helper used by replay mode and tests: same result as the legacy Spark job.
pub fn aggregate_all<'a>(events: impl IntoIterator<Item = &'a Event>) -> Vec<WindowAgg> {
    let mut st = PartitionState::default();
    let mut out = Vec::new();
    for e in events {
        out.extend(st.process(e));
    }
    out.extend(st.drain());
    out.sort_by(|a, b| (&a.vin, a.window_start_ms).cmp(&(&b.vin, b.window_start_ms)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(id: &str, ts: i64, ingest: i64, temp: f64) -> Event {
        Event {
            event_id: id.into(),
            vin: "V1".into(),
            event_ts_ms: ts,
            ingest_ts_ms: ingest,
            cell_temp_c: temp,
            soc_pct: 50.0,
            power_kw: 10.0,
            energy_kwh: 1.0,
            lat: 37.0,
            lon: -122.0,
        }
    }

    #[test]
    fn streaming_emission_equals_batch() {
        let evs: Vec<Event> = (0..200)
            .map(|i| ev(&format!("e{i}"), i * 30_000, i * 30_000 + 1_000, 30.0))
            .collect();
        let mut st = PartitionState::default();
        let mut streamed: Vec<WindowAgg> = evs.iter().flat_map(|e| st.process(e)).collect();
        streamed.extend(st.drain());
        assert_eq!(streamed, aggregate_all(&evs));
        assert!(streamed
            .windows(2)
            .all(|w| w[0].window_start_ms < w[1].window_start_ms));
    }

    #[test]
    fn late_and_duplicate_rules() {
        let evs = vec![
            ev("a", 0, 600_000, 30.0),
            ev("b", 1, 600_002, 99.0),
            ev("a", 0, 600_003, 30.0),
        ];
        let out = aggregate_all(&evs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].events, 1);
        assert_eq!(out[0].max_cell_temp_c, 30.0);
    }

    #[test]
    fn state_roundtrips_through_json() {
        let mut st = PartitionState::default();
        st.process(&ev("a", 0, 1_000, 30.0));
        let restored: PartitionState =
            serde_json::from_str(&serde_json::to_string(&st).unwrap()).unwrap();
        assert_eq!(restored.open_windows(), 1);
    }
}
