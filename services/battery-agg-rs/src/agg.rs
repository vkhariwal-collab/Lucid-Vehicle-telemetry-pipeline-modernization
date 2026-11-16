//! Pure, deterministic port of `BatteryWindowAgg.transform` (legacy Scala/Spark).
//! No I/O, no clock: results depend only on the input events.

use crate::model::{Event, WindowAgg};
use crate::privacy::round2_half_up;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const WINDOW_MS: i64 = 300_000;
pub const MAX_LATENESS_MS: i64 = 600_000;
pub const THERMAL_ALERT_C: f64 = 55.0;
/// How long (in ingest time) a final window is remembered, so a record with a skewed
/// ingest timestamp cannot re-open it and emit a second record for the same window.
pub const CLOSED_RETENTION_MS: i64 = 3_600_000;

/// `(window_start_ms, vin)`: ordered by window start, so finalization pops from the front.
pub type WindowKey = (i64, String);

/// Open aggregate of one window. Opaque; persisted to the changelog as a `WindowEntry`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowState {
    events: i64,
    sum_temp: f64,
    n_temp: i64,
    max_temp: Option<f64>,
    min_soc: Option<f64>,
    max_power: Option<f64>,
    min_energy: Option<f64>,
    max_energy: Option<f64>,
    last_ts: i64,
    last_id: String,
    last_lat: Option<f64>,
    last_lon: Option<f64>,
    seen: BTreeSet<String>,
}

/// Spark `max`/`min`: nulls are ignored; the result is null only if every input is null.
fn fold(acc: Option<f64>, x: Option<f64>, pick: fn(f64, f64) -> f64) -> Option<f64> {
    match (acc, x) {
        (Some(a), Some(b)) => Some(pick(a, b)),
        (a, b) => a.or(b),
    }
}

impl WindowState {
    fn new() -> Self {
        WindowState {
            events: 0,
            sum_temp: 0.0,
            n_temp: 0,
            max_temp: None,
            min_soc: None,
            max_power: None,
            min_energy: None,
            max_energy: None,
            last_ts: i64::MIN,
            last_id: String::new(),
            last_lat: None,
            last_lon: None,
            seen: BTreeSet::new(),
        }
    }

    fn add(&mut self, e: &Event) {
        self.seen.insert(e.event_id.clone());
        self.events += 1;
        if let Some(t) = e.cell_temp_c {
            self.sum_temp += t; // legacy quirk TQ-03: -128.0 sensor sentinel is averaged in
            self.n_temp += 1;
        }
        self.max_temp = fold(self.max_temp, e.cell_temp_c, f64::max);
        self.min_soc = fold(self.min_soc, e.soc_pct, f64::min);
        self.max_power = fold(self.max_power, e.power_kw, f64::max);
        self.min_energy = fold(self.min_energy, e.energy_kwh, f64::min);
        self.max_energy = fold(self.max_energy, e.energy_kwh, f64::max);
        // Spark: max(struct(event_ts, event_id, lat, lon)) -> location of the latest event, even if null.
        if (e.event_ts_ms, e.event_id.as_str()) > (self.last_ts, self.last_id.as_str()) {
            self.last_ts = e.event_ts_ms;
            self.last_id = e.event_id.clone();
            self.last_lat = e.lat.map(round2_half_up);
            self.last_lon = e.lon.map(round2_half_up);
        }
    }

    fn finish(&self, vin: &str, start: i64) -> WindowAgg {
        WindowAgg {
            vin: vin.to_string(),
            window_start_ms: start,
            window_end_ms: start + WINDOW_MS,
            events: self.events,
            avg_cell_temp_c: (self.n_temp > 0).then(|| self.sum_temp / self.n_temp as f64),
            max_cell_temp_c: self.max_temp,
            min_soc_pct: self.min_soc,
            max_power_kw: self.max_power,
            // legacy quirk TQ-02: counter reset
            energy_kwh: self.max_energy.zip(self.min_energy).map(|(hi, lo)| hi - lo),
            last_lat: self.last_lat,
            last_lon: self.last_lon,
            thermal_alert: self.max_temp.map(|t| t >= THERMAL_ALERT_C),
        }
    }
}

/// Changelog value for one window key. A tombstone (no value) means "forgotten".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WindowEntry {
    Open(WindowState),
    /// Emitted window; keeps its event ids so later resends are still deduplicated.
    Closed(BTreeSet<String>),
}

/// Per-partition watermark and counters, persisted as one small changelog record.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PartitionMeta {
    pub ingest_watermark: i64,
    pub late_dropped: u64,
    pub duplicates: u64,
    #[serde(default)]
    pub closed_dropped: u64,
    #[serde(default)]
    pub before_emit_from: u64,
}

/// Changes since the last `take_changes`: the partition meta plus every window key whose
/// changelog entry changed (`None` = tombstone).
pub type Changes = (PartitionMeta, Vec<(WindowKey, Option<WindowEntry>)>);

/// Aggregation state for one input partition. Checkpointed incrementally (only changed
/// windows) to the changelog topic, in the same transaction as outputs and offsets.
#[derive(Debug, Clone, Default)]
pub struct PartitionState {
    windows: BTreeMap<WindowKey, WindowState>,
    closed: BTreeMap<WindowKey, BTreeSet<String>>,
    /// event_id -> window it was counted in, for every open and remembered closed window.
    /// Spark's `dropDuplicates("event_id")` is not scoped to a VIN or window.
    ids: HashMap<String, WindowKey>,
    meta: PartitionMeta,
    emit_from_ms: Option<i64>,
    dirty: BTreeSet<WindowKey>,
}

impl PartitionState {
    /// `emit_from_ms`: windows starting earlier are owned by the previous producer (cutover).
    pub fn new(emit_from_ms: Option<i64>) -> Self {
        PartitionState {
            emit_from_ms,
            ..Default::default()
        }
    }

    pub fn set_emit_from(&mut self, emit_from_ms: Option<i64>) {
        self.emit_from_ms = emit_from_ms;
    }

    /// Applies one event; returns windows that became final (none can change any more).
    pub fn process(&mut self, e: &Event) -> Vec<WindowAgg> {
        let m = &mut self.meta;
        m.ingest_watermark = m.ingest_watermark.max(e.ingest_ts_ms);
        if e.ingest_ts_ms - e.event_ts_ms > MAX_LATENESS_MS {
            m.late_dropped += 1; // legacy: filter before dropDuplicates
            return self.finalize(false);
        }
        let start = e.event_ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS;
        let key = (start, e.vin.clone());
        if self.emit_from_ms.is_some_and(|f| start < f) {
            m.before_emit_from += 1;
        } else if self.closed.contains_key(&key)
            || start + WINDOW_MS + MAX_LATENESS_MS + CLOSED_RETENTION_MS <= m.ingest_watermark
        {
            m.closed_dropped += 1; // window already emitted: never emit it twice
        } else if self.ids.contains_key(&e.event_id) {
            m.duplicates += 1;
        } else {
            self.ids.insert(e.event_id.clone(), key.clone());
            self.windows
                .entry(key.clone())
                .or_insert_with(WindowState::new)
                .add(e);
            self.dirty.insert(key);
        }
        self.finalize(false)
    }

    /// A window is final once ingest time has passed its end plus the lateness bound:
    /// any later event for it would be dropped as late. Output is in (window_start, vin) order.
    fn finalize(&mut self, all: bool) -> Vec<WindowAgg> {
        let wm = self.meta.ingest_watermark;
        let mut out = Vec::new();
        while let Some(entry) = self.windows.first_entry() {
            if !all && entry.key().0 + WINDOW_MS + MAX_LATENESS_MS > wm {
                break;
            }
            let (key, w) = entry.remove_entry();
            out.push(w.finish(&key.1, key.0));
            self.dirty.insert(key.clone());
            self.closed.insert(key, w.seen);
        }
        while let Some(entry) = self.closed.first_entry() {
            if entry.key().0 + WINDOW_MS + MAX_LATENESS_MS + CLOSED_RETENTION_MS > wm {
                break;
            }
            let (k, seen) = entry.remove_entry();
            for id in seen {
                self.ids.remove(&id);
            }
            self.dirty.insert(k);
        }
        out
    }

    /// Emits every open window (end of replay / graceful drain).
    pub fn drain(&mut self) -> Vec<WindowAgg> {
        self.finalize(true)
    }

    pub fn open_windows(&self) -> usize {
        self.windows.len()
    }

    pub fn meta(&self) -> &PartitionMeta {
        &self.meta
    }

    /// Returns and clears the changelog delta since the previous call.
    pub fn take_changes(&mut self) -> Changes {
        let changes = std::mem::take(&mut self.dirty)
            .into_iter()
            .map(|k| {
                let entry = match self.windows.get(&k) {
                    Some(w) => Some(WindowEntry::Open(w.clone())),
                    None => self.closed.get(&k).cloned().map(WindowEntry::Closed),
                };
                (k, entry)
            })
            .collect();
        (self.meta.clone(), changes)
    }

    /// Applies one changelog window record (later records for a key replace earlier ones).
    pub fn restore_window(&mut self, key: WindowKey, entry: Option<WindowEntry>) {
        let old = match self.windows.remove(&key) {
            Some(w) => Some(w.seen),
            None => self.closed.remove(&key),
        };
        for id in old.into_iter().flatten() {
            self.ids.remove(&id);
        }
        let seen = match entry {
            Some(WindowEntry::Open(w)) => self.windows.entry(key.clone()).or_insert(w).seen.clone(),
            Some(WindowEntry::Closed(seen)) => {
                self.closed.entry(key.clone()).or_insert(seen).clone()
            }
            None => return,
        };
        for id in seen {
            self.ids.insert(id, key.clone());
        }
    }

    pub fn restore_meta(&mut self, meta: PartitionMeta) {
        self.meta = meta;
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
            cell_temp_c: Some(temp),
            soc_pct: Some(50.0),
            power_kw: Some(10.0),
            energy_kwh: Some(1.0),
            lat: Some(37.0),
            lon: Some(-122.0),
        }
    }

    fn series(n: i64) -> Vec<Event> {
        (0..n)
            .map(|i| ev(&format!("e{i}"), i * 30_000, i * 30_000 + 1_000, 30.0))
            .collect()
    }

    #[test]
    fn streaming_emission_equals_batch() {
        let evs = series(200);
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
        assert_eq!(out[0].max_cell_temp_c, Some(30.0));
    }

    #[test]
    fn duplicates_are_dropped_across_windows_like_spark() {
        let out = aggregate_all(&[ev("a", 0, 1_000, 30.0), ev("a", 400_000, 401_000, 40.0)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].window_start_ms, 0);
    }

    #[test]
    fn null_measurements_are_counted_but_not_aggregated() {
        let mut nulls = ev("b", 10, 1_010, 0.0);
        nulls.cell_temp_c = None;
        nulls.lat = None;
        let out = aggregate_all(&[ev("a", 0, 1_000, 30.0), nulls]);
        assert_eq!(out[0].events, 2);
        assert_eq!(out[0].avg_cell_temp_c, Some(30.0));
        assert_eq!(out[0].last_lat, None);
        let only_nulls = Event {
            cell_temp_c: None,
            soc_pct: None,
            power_kw: None,
            energy_kwh: None,
            lat: None,
            lon: None,
            ..ev("c", 0, 1_000, 0.0)
        };
        let w = &aggregate_all(&[only_nulls])[0];
        assert_eq!(w.events, 1);
        assert_eq!(serde_json::to_string(w).unwrap().matches(':').count(), 4);
    }

    #[test]
    fn skewed_ingest_never_emits_a_window_twice() {
        let mut st = PartitionState::default();
        let mut out = st.process(&ev("a", 0, 1_000, 30.0));
        out.extend(st.process(&ev("z", 2_000_000, 2_000_000, 30.0)));
        out.extend(st.process(&ev("b", 10_000, 20_000, 30.0)));
        out.extend(st.drain());
        assert_eq!(out.iter().filter(|w| w.window_start_ms == 0).count(), 1);
        assert_eq!(st.meta().closed_dropped, 1);
    }

    #[test]
    fn closed_windows_are_forgotten_after_retention() {
        let mut st = PartitionState::default();
        st.process(&ev("a", 0, 1_000, 30.0));
        st.process(&ev("z", 1_000_000, 1_000_000, 30.0));
        assert_eq!(st.closed.len(), 1);
        st.process(&ev("y", 10_000_000, 10_000_000, 30.0));
        assert!(!st.closed.contains_key(&(0, "V1".into())));
        assert!(!st.ids.contains_key("a"));
    }

    #[test]
    fn emit_from_skips_windows_owned_by_previous_producer() {
        let mut st = PartitionState::new(Some(3_000_000));
        let mut out: Vec<WindowAgg> = series(200).iter().flat_map(|e| st.process(e)).collect();
        out.extend(st.drain());
        assert!(out.iter().all(|w| w.window_start_ms >= 3_000_000));
        assert_eq!(st.meta().before_emit_from, 100);
    }

    #[test]
    fn restore_from_changelog_mid_stream_equals_uninterrupted() {
        let evs = series(200);
        let mut changelog = Vec::new();
        let mut meta = PartitionMeta::default();
        let mut st = PartitionState::default();
        let mut out = Vec::new();
        for (i, e) in evs.iter().enumerate() {
            out.extend(st.process(e));
            if i % 7 == 0 || i == 120 {
                let (m, ch) = st.take_changes();
                meta = m;
                changelog.extend(ch);
            }
            if i == 120 {
                break;
            }
        }
        let json = serde_json::to_string(&changelog).unwrap();
        let mut restored = PartitionState::default();
        restored
            .restore_meta(serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap());
        for (k, v) in serde_json::from_str::<Vec<(WindowKey, Option<WindowEntry>)>>(&json).unwrap()
        {
            restored.restore_window(k, v);
        }
        for e in &evs[121..] {
            out.extend(restored.process(e));
        }
        out.extend(restored.drain());
        out.sort_by(|a, b| (&a.vin, a.window_start_ms).cmp(&(&b.vin, b.window_start_ms)));
        assert_eq!(out, aggregate_all(&evs));
    }
}
