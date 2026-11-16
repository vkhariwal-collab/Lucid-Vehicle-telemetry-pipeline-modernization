//! Privacy template (privacy/template.yaml), implemented once and reviewed once.
//! Every job built on this crate inherits these rules:
//! - P1 location is coarsened to 2 decimals (~1 km) before it enters state or output;
//! - P2 logs never contain VIN or location: only an HMAC token of the VIN;
//! - P3 dead-letter records keep no location and no raw VIN.

use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::error::Category;
use sha2::{Digest, Sha256};

/// Rounds to 2 decimals with Spark's semantics: HALF_UP on the shortest decimal
/// representation of the double (Scala `BigDecimal(d).setScale(2, HALF_UP)`).
pub fn round2_half_up(x: f64) -> f64 {
    let s = format!("{x}");
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.as_str()),
    };
    let (int_part, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if frac.len() <= 2 {
        return x + 0.0; // -0.0 + 0.0 == +0.0
    }
    let mut cents: i64 = format!("{int_part}{}", &frac[..2]).parse().unwrap_or(0);
    if frac.as_bytes()[2] >= b'5' {
        cents += 1;
    }
    let v = cents as f64 / 100.0;
    let v: f64 = format!("{v:.2}").parse().unwrap_or(v);
    // BigDecimal has no negative zero: Spark writes round(-0.004, 2) as 0.0.
    if neg && v != 0.0 {
        -v
    } else {
        v
    }
}

/// Stable pseudonymous token for logs and DLQ (P2/P3).
pub fn vin_token(vin: &str, key: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(vin.as_bytes());
    hex::encode(&mac.finalize().into_bytes()[..6])
}

/// The only fields that may be logged. There is deliberately no VIN or location field.
#[derive(Debug, Default, Serialize)]
pub struct LogFields<'a> {
    pub msg: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vin_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

pub fn log(f: LogFields) {
    eprintln!("{}", serde_json::to_string(&f).unwrap_or_default());
}

/// Parse-error description that is safe for the DLQ (P3). serde_json messages can echo the
/// offending value (e.g. `invalid type: string "37.123456"`), so only the error category,
/// position and our own schema field names are kept.
pub fn safe_parse_error(e: &serde_json::Error) -> String {
    let msg = e.to_string();
    let schema_msg = ["missing field `", "unknown field `", "duplicate field `"]
        .iter()
        .find_map(|p| {
            let rest = msg.strip_prefix(p)?;
            let field = &rest[..rest.find('`')?];
            Some(format!("{p}{field}`"))
        });
    let what = schema_msg.unwrap_or_else(|| {
        match e.classify() {
            Category::Io => "io error",
            Category::Syntax => "syntax error",
            Category::Data => "invalid value",
            Category::Eof => "unexpected end of input",
        }
        .to_string()
    });
    format!("{what} at line {} column {}", e.line(), e.column())
}

/// Builds a dead-letter record for an unparseable payload (P3): no location, no raw VIN.
pub fn dlq_record(payload: &[u8], err: &serde_json::Error, key: &[u8]) -> String {
    let token = serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|v| {
            v.get("vin")
                .and_then(|x| x.as_str())
                .map(|vin| vin_token(vin, key))
        });
    serde_json::json!({
        "error": safe_parse_error(err),
        "payload_sha256": hex::encode(Sha256::digest(payload)),
        "payload_len": payload.len(),
        "vin_token": token,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn half_up_matches_spark() {
        assert_eq!(round2_half_up(37.545), 37.55);
        assert_eq!(round2_half_up(-121.985), -121.99);
        assert_eq!(round2_half_up(37.5449), 37.54);
        assert_eq!(round2_half_up(37.1), 37.1);
        assert!(round2_half_up(-0.004).is_sign_positive());
        assert!(round2_half_up(-0.0).is_sign_positive());
        assert_eq!(round2_half_up(-0.005), -0.01);
    }

    #[test]
    fn dlq_has_no_location_or_raw_vin() {
        let p = br#"{"vin":"5L1SYN0EDGE000004","lat":37.123456,"lon":-122.1,"event_id":"x"}"#;
        let err = serde_json::from_slice::<crate::model::Event>(p).unwrap_err();
        let r = dlq_record(p, &err, b"k");
        assert!(!r.contains("5L1SYN0EDGE000004") && !r.contains("37.12") && !r.contains("lat"));
        assert!(r.contains("vin_token") && r.contains("missing field `event_ts_ms`"));
    }

    #[test]
    fn dlq_error_does_not_echo_values() {
        let p = br#"{"event_id":"x","vin":"5L1SYN0EDGE000004","event_ts_ms":"37.123456","ingest_ts_ms":1,"lat":37.123456}"#;
        let err = serde_json::from_slice::<crate::model::Event>(p).unwrap_err();
        assert!(err.to_string().contains("37.123456"));
        let r = dlq_record(p, &err, b"k");
        assert!(
            !r.contains("37.12") && !r.contains("5L1SYN0EDGE000004"),
            "{r}"
        );
        assert!(r.contains("invalid value at line 1 column"));
    }

    #[test]
    fn log_fields_cannot_carry_location() {
        let s = serde_json::to_string(&LogFields {
            msg: "x",
            vin_token: Some("ab".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(s, r#"{"msg":"x","vin_token":"ab"}"#);
    }
}
