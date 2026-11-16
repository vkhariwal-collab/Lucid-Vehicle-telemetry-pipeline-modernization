//! Pure, deterministic port of `BatteryWindowAgg.transform` (legacy Scala/Spark).
//! No I/O, no clock: results depend only on the input events.

use crate::model::{Event, WindowAgg};
use crate::privacy::round2_half_up;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

/// Changelog value for one window key. A tombstone (no value) means "forgotten".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WindowEntry {
    Open(WindowState),
    Closed,
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
    closed: BTreeSet<WindowKey>,
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
        } else if self.closed.contains(&key)
            || start + WINDOW_MS + MAX_LATENESS_MS + CLOSED_RETENTION_MS <= m.ingest_watermark
        {
            m.closed_dropped += 1; // window already emitted: never emit it twice
        } else {
            match self.windows.get_mut(&key) {
                Some(w) if w.seen.contains(&e.event_id) => m.duplicates += 1,
                Some(w) => {
                    w.seen.insert(e.event_id.clone());
                    w.add(e);
                    self.dirty.insert(key);
                }
                None => {
                    let mut w = WindowState::new(e);
                    w.add(e);
                    self.windows.insert(key.clone(), w);
                    self.dirty.insert(key);
                }
            }
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
            self.closed.insert(key);
        }
        while self
            .closed
            .first()
            .is_some_and(|k| k.0 + WINDOW_MS + MAX_LATENESS_MS + CLOSED_RETENTION_MS <= wm)
        {
            if let Some(k) = self.closed.pop_first() {
                self.dirty.insert(k);
            }
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
                    None if self.closed.contains(&k) => Some(WindowEntry::Closed),
                    None => None,
                };
                (k, entry)
            })
            .collect();
        (self.meta.clone(), changes)
    }

    /// Applies one changelog window record (later records for a key replace earlier ones).
    pub fn restore_window(&mut self, key: WindowKey, entry: Option<WindowEntry>) {
        self.windows.remove(&key);
        self.closed.remove(&key);
        match entry {
            Some(WindowEntry::Open(w)) => {
                self.windows.insert(key, w);
            }
            Some(WindowEntry::Closed) => {
                self.closed.insert(key);
            }
            None => {}
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
            cell_temp_c: temp,
            soc_pct: 50.0,
            power_kw: 10.0,
            energy_kwh: 1.0,
            lat: 37.0,
            lon: -122.0,
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
        assert_eq!(out[0].max_cell_temp_c, 30.0);
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
        assert!(!st.closed.contains(&(0, "V1".into())));
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
