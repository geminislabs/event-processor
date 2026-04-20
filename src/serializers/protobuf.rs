use anyhow::{anyhow, Context, Result};
use chrono::{TimeZone, Utc};
use prost::Message;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::models::IncomingMessage;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/siscom.v1.rs"));
}

pub fn deserialize_incoming_message(payload: &[u8]) -> Result<IncomingMessage> {
    let parsed = proto::KafkaMessage::decode(payload)
        .map_err(|error| anyhow!("failed to decode protobuf payload: {error}"))?;

    kafka_message_to_incoming(parsed)
}

fn kafka_message_to_incoming(message: proto::KafkaMessage) -> Result<IncomingMessage> {
    let data = message.data;

    let msg_class = first_present(
        &data,
        &["msg_class", "MSG_CLASS", "message_class", "MESSAGE_CLASS"],
    )
    .ok_or_else(|| anyhow!("missing msg_class in protobuf data map"))?;

    let latitude = parse_f64_opt(first_present(
        &data,
        &["latitude", "lat", "LATITUDE", "LAT", "LATITUD"],
    ))
    .context("invalid latitude value")?;

    let longitude = parse_f64_opt(first_present(
        &data,
        &[
            "longitude",
            "lng",
            "lon",
            "LONGITUDE",
            "LNG",
            "LON",
            "LONGITUD",
        ],
    ))
    .context("invalid longitude value")?;

    let gps_epoch = parse_i64_opt(first_present(
        &data,
        &["gps_epoch", "GPS_EPOCH", "epoch", "EPOCH"],
    ))
    .context("invalid gps_epoch value")?;

    let mut extra = Map::new();
    for (key, value) in data {
        if let Some(trimmed) = normalize_opt(&value) {
            extra.insert(key, Value::String(trimmed.to_string()));
        }
    }

    Ok(IncomingMessage {
        message_id: parse_uuid_opt(normalize_opt(&message.uuid).map(ToString::to_string))
            .context("invalid uuid in protobuf payload")?,
        device_id: first_present(&extra_string_map(&extra), &["device_id", "DEVICE_ID"]),
        msg_class,
        alert: first_present(&extra_string_map(&extra), &["alert", "ALERT"]),
        fix_status: first_present(&extra_string_map(&extra), &["fix_status", "FIX_STATUS"]),
        gps_epoch,
        received_at: message
            .metadata
            .and_then(|meta| epoch_to_datetime(meta.received_epoch)),
        source_id: parse_uuid_opt(first_present(
            &extra_string_map(&extra),
            &["source_id", "SOURCE_ID"],
        ))
        .context("invalid source_id in protobuf data")?,
        unit_id: parse_uuid_opt(first_present(
            &extra_string_map(&extra),
            &["unit_id", "UNIT_ID"],
        ))
        .context("invalid unit_id in protobuf data")?,
        occurred_at: None,
        latitude,
        longitude,
        extra,
    })
}

fn extra_string_map(extra: &Map<String, Value>) -> std::collections::HashMap<String, String> {
    extra
        .iter()
        .filter_map(|(key, value)| value.as_str().map(|v| (key.clone(), v.to_string())))
        .collect()
}

fn normalize_opt(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn first_present(
    data: &std::collections::HashMap<String, String>,
    keys: &[&str],
) -> Option<String> {
    keys.iter().find_map(|key| {
        data.get(*key)
            .and_then(|value| normalize_opt(value).map(ToString::to_string))
    })
}

fn parse_f64_opt(raw: Option<String>) -> Result<Option<f64>> {
    match raw {
        Some(value) => value
            .parse::<f64>()
            .map(Some)
            .map_err(|error| anyhow!("{error}")),
        None => Ok(None),
    }
}

fn parse_i64_opt(raw: Option<String>) -> Result<Option<i64>> {
    match raw {
        Some(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|error| anyhow!("{error}")),
        None => Ok(None),
    }
}

fn parse_uuid_opt(raw: Option<String>) -> Result<Option<Uuid>> {
    match raw {
        Some(value) => Uuid::parse_str(&value)
            .map(Some)
            .map_err(|error| anyhow!("{error}")),
        None => Ok(None),
    }
}

fn epoch_to_datetime(raw: u64) -> Option<chrono::DateTime<Utc>> {
    let raw = i64::try_from(raw).ok()?;

    if raw > 9_999_999_999 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

#[cfg(test)]
mod tests {
    use super::proto::KafkaMessage;
    use super::{deserialize_incoming_message, proto};
    use chrono::{TimeZone, Utc};
    use prost::Message;

    #[test]
    fn decodes_kafka_message_to_incoming_message() {
        let mut data = std::collections::HashMap::new();
        data.insert("device_id".to_string(), "0848086072".to_string());
        data.insert("msg_class".to_string(), "ALERT".to_string());
        data.insert("latitude".to_string(), "19.216813".to_string());
        data.insert("longitude".to_string(), "-102.575137".to_string());
        data.insert("gps_epoch".to_string(), "1700000000".to_string());

        let encoded = KafkaMessage {
            uuid: "bc6f8cef-38b5-5ee7-ba2a-9cfadff3d474".to_string(),
            decoded: None,
            data,
            metadata: Some(proto::Metadata {
                worker_id: 0,
                received_epoch: 1_700_000_001,
                decoded_epoch: 0,
                bytes: 0,
                client_ip: "127.0.0.1".to_string(),
                client_port: 9999,
            }),
            raw: "raw".to_string(),
        }
        .encode_to_vec();

        let parsed = deserialize_incoming_message(&encoded).expect("payload should decode");

        assert_eq!(parsed.msg_class, "ALERT");
        assert_eq!(parsed.device_id.as_deref(), Some("0848086072"));
        assert_eq!(parsed.latitude, Some(19.216813));
        assert_eq!(parsed.longitude, Some(-102.575137));
        assert_eq!(parsed.gps_epoch, Some(1_700_000_000));
        assert_eq!(
            parsed.received_at,
            Some(Utc.timestamp_opt(1_700_000_001, 0).single().unwrap())
        );
    }

    #[test]
    fn fails_when_msg_class_is_missing() {
        let encoded = KafkaMessage {
            uuid: "bc6f8cef-38b5-5ee7-ba2a-9cfadff3d474".to_string(),
            decoded: None,
            data: std::collections::HashMap::new(),
            metadata: None,
            raw: "raw".to_string(),
        }
        .encode_to_vec();

        let error = deserialize_incoming_message(&encoded).expect_err("msg_class must be required");
        assert!(error.to_string().contains("missing msg_class"));
    }
}
