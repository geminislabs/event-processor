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
    if let Ok(parsed) = proto::KafkaMessage::decode(payload) {
        // Some upstream producers still emit the legacy Communication contract.
        // In that case, KafkaMessage decode may succeed but without a usable msg_class.
        if has_msg_class_key(&parsed.data) {
            tracing::debug!(
                protobuf_contract = "kafka_message",
                "protobuf payload decoded"
            );
            return kafka_message_to_incoming(parsed);
        }

        if let Ok(legacy) = proto::Communication::decode(payload) {
            if let Ok(mapped) = communication_to_incoming(legacy) {
                tracing::info!(
                    protobuf_contract = "legacy_communication",
                    "protobuf payload decoded"
                );
                return Ok(mapped);
            }
        }

        tracing::debug!(
            protobuf_contract = "kafka_message",
            "protobuf payload decoded with missing contract fields"
        );
        return kafka_message_to_incoming(parsed);
    }

    if let Ok(legacy) = proto::Communication::decode(payload) {
        tracing::info!(
            protobuf_contract = "legacy_communication",
            "protobuf payload decoded"
        );
        return communication_to_incoming(legacy);
    }

    Err(anyhow!(
        "failed to decode protobuf payload as KafkaMessage or Communication"
    ))
}

fn kafka_message_to_incoming(message: proto::KafkaMessage) -> Result<IncomingMessage> {
    let data = message.data;

    let msg_class = first_present(
        &data,
        &[
            "msg_class",
            "MSG_CLASS",
            "msgClass",
            "message_class",
            "MESSAGE_CLASS",
            "messageClass",
        ],
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

fn communication_to_incoming(message: proto::Communication) -> Result<IncomingMessage> {
    let Some(data) = message.data else {
        return Err(anyhow!("missing data in legacy Communication payload"));
    };

    let msg_class = message_class_from_normalized(&data)
        .ok_or_else(|| anyhow!("missing msg_class in legacy Communication payload"))?;

    let mut extra = Map::new();
    for (key, value) in &data.additional_fields {
        if let Some(trimmed) = normalize_opt(value) {
            extra.insert(key.clone(), Value::String(trimmed.to_string()));
        }
    }

    let latitude = parse_f64_opt(first_present(
        &extra_string_map(&extra),
        &["latitude", "lat", "LATITUDE", "LAT", "LATITUD"],
    ))
    .context("invalid latitude value")?
    .or_else(|| (data.latitude != 0.0).then_some(data.latitude));

    let longitude = parse_f64_opt(first_present(
        &extra_string_map(&extra),
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
    .context("invalid longitude value")?
    .or_else(|| (data.longitude != 0.0).then_some(data.longitude));

    let gps_epoch = parse_i64_opt(first_present(
        &extra_string_map(&extra),
        &["gps_epoch", "GPS_EPOCH", "epoch", "EPOCH"],
    ))
    .context("invalid gps_epoch value")?
    .or_else(|| i64::try_from(data.gps_epoch).ok());

    Ok(IncomingMessage {
        message_id: parse_uuid_opt(normalize_opt(&message.uuid).map(ToString::to_string))
            .context("invalid uuid in legacy Communication payload")?,
        device_id: if data.device_id.trim().is_empty() {
            first_present(&extra_string_map(&extra), &["device_id", "DEVICE_ID"])
        } else {
            Some(data.device_id)
        },
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
        .context("invalid source_id in legacy Communication payload")?,
        unit_id: parse_uuid_opt(first_present(
            &extra_string_map(&extra),
            &["unit_id", "UNIT_ID"],
        ))
        .context("invalid unit_id in legacy Communication payload")?,
        occurred_at: None,
        latitude,
        longitude,
        extra,
    })
}

fn has_msg_class_key(data: &std::collections::HashMap<String, String>) -> bool {
    first_present(
        data,
        &[
            "msg_class",
            "MSG_CLASS",
            "msgClass",
            "message_class",
            "MESSAGE_CLASS",
            "messageClass",
        ],
    )
    .is_some()
}

fn message_class_from_normalized(data: &proto::NormalizedData) -> Option<String> {
    let class = proto::MessageClass::try_from(data.msg_class).ok()?;
    match class {
        proto::MessageClass::Status => Some("STATUS".to_string()),
        proto::MessageClass::Event => Some("EVENT".to_string()),
        proto::MessageClass::Alert => Some("ALERT".to_string()),
        proto::MessageClass::MsgUnknown => None,
    }
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

    #[test]
    fn decodes_legacy_communication_payload() {
        let encoded = proto::Communication {
            uuid: "bc6f8cef-38b5-5ee7-ba2a-9cfadff3d474".to_string(),
            vendor: proto::Vendor::Suntech as i32,
            data: Some(proto::NormalizedData {
                device_id: "0848086072".to_string(),
                latitude: 19.216813,
                longitude: -102.575137,
                speed: 0.0,
                course: 0.0,
                engine_on: false,
                satellites: 0,
                msg_class: proto::MessageClass::Alert as i32,
                gps_epoch: 1_700_000_000,
                main_battery_voltage: 0.0,
                backup_battery_voltage: 0.0,
                odometer_mts: 0,
                trip_distance_mts: 0,
                additional_fields: std::collections::HashMap::new(),
            }),
            metadata: Some(proto::Metadata {
                worker_id: 0,
                received_epoch: 1_700_000_001,
                decoded_epoch: 0,
                bytes: 0,
                client_ip: "127.0.0.1".to_string(),
                client_port: 9999,
            }),
            decoded_payload: vec![],
            decoded_content_type: "application/json".to_string(),
            raw: "raw".to_string(),
        }
        .encode_to_vec();

        let parsed = deserialize_incoming_message(&encoded).expect("legacy payload should decode");

        assert_eq!(parsed.msg_class, "ALERT");
        assert_eq!(parsed.device_id.as_deref(), Some("0848086072"));
        assert_eq!(parsed.latitude, Some(19.216813));
        assert_eq!(parsed.longitude, Some(-102.575137));
        assert_eq!(parsed.gps_epoch, Some(1_700_000_000));
    }
}
