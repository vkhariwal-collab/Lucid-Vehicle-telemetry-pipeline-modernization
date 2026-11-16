//! Wire types. Field names match contracts/battery_telemetry.v1.json and
//! contracts/battery_window_agg.v1.json (and the legacy Spark schema).

use serde::{Deserialize, Deserializer, Serialize};

/// One telemetry sample from a vehicle. Unknown fields are ignored, like Spark's explicit schema.
/// Key fields are required (missing or wrong type -> DLQ, TQ-04). Measurements behave like
/// Spark's PERMISSIVE reader: missing, `null` or wrong-typed values become `None` and the
/// event is still counted.
#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub vin: String,
    pub event_ts_ms: i64,
    pub ingest_ts_ms: i64,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub cell_temp_c: Option<f64>,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub soc_pct: Option<f64>,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub power_kw: Option<f64>,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub energy_kwh: Option<f64>,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub lat: Option<f64>,
    #[serde(default, deserialize_with = "lenient_f64")]
    pub lon: Option<f64>,
}

fn lenient_f64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    Ok(serde_json::Value::deserialize(d)?.as_f64())
}

/// Final aggregate for one VIN and one 5-minute event-time window. Like Spark, an aggregate
/// over only-null inputs is null and omitted from the JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowAgg {
    pub vin: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub events: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg_cell_temp_c: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cell_temp_c: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_soc_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_power_kw: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub energy_kwh: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_lat: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_lon: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thermal_alert: Option<bool>,
}
