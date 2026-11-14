//! Wire types. Field names match contracts/battery_telemetry.v1.json and
//! contracts/battery_window_agg.v1.json (and the legacy Spark schema).

use serde::{Deserialize, Serialize};

/// One telemetry sample from a vehicle. Unknown fields are ignored, like Spark's explicit schema.
#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub vin: String,
    pub event_ts_ms: i64,
    pub ingest_ts_ms: i64,
    pub cell_temp_c: f64,
    pub soc_pct: f64,
    pub power_kw: f64,
    pub energy_kwh: f64,
    pub lat: f64,
    pub lon: f64,
}

/// Final aggregate for one VIN and one 5-minute event-time window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowAgg {
    pub vin: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub events: i64,
    pub avg_cell_temp_c: f64,
    pub max_cell_temp_c: f64,
    pub min_soc_pct: f64,
    pub max_power_kw: f64,
    pub energy_kwh: f64,
    pub last_lat: f64,
    pub last_lon: f64,
    pub thermal_alert: bool,
}
