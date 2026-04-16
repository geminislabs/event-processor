use chrono::{DateTime, TimeZone, Utc};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

const SOURCE_NAMESPACE: Uuid = Uuid::from_u128(0x8d3f8f87_26b3_4f31_97af_889391c4aa1e);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingMessage {
    #[serde(default, alias = "uuid")]
    pub message_id: Option<Uuid>,
    #[serde(default, alias = "deviceId")]
    pub device_id: Option<String>,
    #[serde(alias = "msgClass")]
    pub msg_class: String,
    #[serde(default)]
    pub alert: Option<String>,
    #[serde(
        default,
        alias = "fixStatus",
        deserialize_with = "deserialize_string_like_opt"
    )]
    pub fix_status: Option<String>,
    #[serde(default, alias = "gpsEpoch", deserialize_with = "deserialize_i64_opt")]
    pub gps_epoch: Option<i64>,
    #[serde(
        default,
        alias = "receivedAt",
        deserialize_with = "deserialize_datetime_opt"
    )]
    pub received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_id: Option<Uuid>,
    #[serde(default)]
    pub unit_id: Option<Uuid>,
    #[serde(
        default,
        alias = "timestamp",
        alias = "occurredAt",
        alias = "ts",
        deserialize_with = "deserialize_datetime_opt"
    )]
    pub occurred_at: Option<DateTime<Utc>>,
    #[serde(default, alias = "lat")]
    #[serde(deserialize_with = "deserialize_f64_opt")]
    pub latitude: Option<f64>,
    #[serde(default, alias = "lng", alias = "lon")]
    #[serde(deserialize_with = "deserialize_f64_opt")]
    pub longitude: Option<f64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl IncomingMessage {
    pub fn routing_key(&self) -> &str {
        self.msg_class.as_str()
    }

    pub fn resolved_source_id(&self) -> Uuid {
        if let Some(source_id) = self.source_id {
            return source_id;
        }

        if let Some(device_id) = &self.device_id {
            return Uuid::new_v5(&SOURCE_NAMESPACE, device_id.as_bytes());
        }

        self.message_id.unwrap_or_else(Uuid::new_v4)
    }

    pub fn device_source_id(&self) -> String {
        if let Some(device_id) = &self.device_id {
            return device_id.clone();
        }

        self.resolved_source_id().to_string()
    }

    pub fn effective_occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at.unwrap_or_else(Utc::now)
    }

    pub fn event_occurred_at(&self) -> DateTime<Utc> {
        if self.fix_status.as_deref() == Some("1") {
            if let Some(epoch) = self.gps_epoch {
                if let Some(timestamp) = datetime_from_unix(epoch) {
                    return timestamp;
                }
            }
        }

        self.received_at
            .unwrap_or_else(|| self.effective_occurred_at())
    }

    pub fn source_epoch(&self) -> Option<i64> {
        self.gps_epoch
    }

    pub fn payload(&self) -> Value {
        let mut payload = Map::new();

        if let Some(message_id) = self.message_id {
            payload.insert("uuid".to_string(), Value::String(message_id.to_string()));
        }

        if let Some(device_id) = &self.device_id {
            payload.insert("device_id".to_string(), Value::String(device_id.clone()));
        }

        if let Some(alert) = &self.alert {
            payload.insert("alert".to_string(), Value::String(alert.clone()));
        }

        if let Some(latitude) = self.latitude {
            if let Some(number) = serde_json::Number::from_f64(latitude) {
                payload.insert("latitude".to_string(), Value::Number(number));
            }
        }

        if let Some(longitude) = self.longitude {
            if let Some(number) = serde_json::Number::from_f64(longitude) {
                payload.insert("longitude".to_string(), Value::Number(number));
            }
        }

        payload.insert(
            "msg_class".to_string(),
            Value::String(self.msg_class.clone()),
        );

        if let Some(fix_status) = &self.fix_status {
            payload.insert("fix_status".to_string(), Value::String(fix_status.clone()));
        }

        for key in [
            "engine_status",
            "main_battery_voltage",
            "backup_batery_voltage",
        ] {
            if let Some(value) = self.extra.get(key) {
                payload.insert(key.to_string(), value.clone());
            }
        }

        // Normalize satellites field: accept both misspelled `stellites` and `satellites`,
        // prefer numeric representation when possible and expose as `satellites`.
        for key in ["stellites", "satellites"] {
            if let Some(value) = self.extra.get(key) {
                match value {
                    Value::Number(n) => {
                        payload.insert("satellites".to_string(), Value::Number(n.clone()));
                        break;
                    }
                    Value::String(s) => {
                        let trimmed = s.trim();
                        if let Ok(i) = trimmed.parse::<i64>() {
                            payload.insert(
                                "satellites".to_string(),
                                Value::Number(serde_json::Number::from(i)),
                            );
                            break;
                        }

                        if let Ok(f) = trimmed.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(f) {
                                payload.insert("satellites".to_string(), Value::Number(num));
                                break;
                            }
                        }

                        payload
                            .insert("satellites".to_string(), Value::String(trimmed.to_string()));
                        break;
                    }
                    other => {
                        payload.insert("satellites".to_string(), other.clone());
                        break;
                    }
                }
            }
        }

        Value::Object(payload)
    }

    pub fn payload_with_geofence(&self, geofence_id: Uuid) -> Value {
        let mut payload = self.payload();

        if let Value::Object(ref mut map) = payload {
            map.insert(
                "geofence_id".to_string(),
                Value::String(geofence_id.to_string()),
            );
        }

        payload
    }
}

fn datetime_from_unix(raw: i64) -> Option<DateTime<Utc>> {
    if raw > 9_999_999_999 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

fn deserialize_datetime_opt<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => DateTime::parse_from_rfc3339(&raw)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(de::Error::custom),
        Some(Value::Number(number)) => {
            let raw = number
                .as_i64()
                .ok_or_else(|| de::Error::custom("timestamp must fit in i64"))?;

            let timestamp = if raw > 9_999_999_999 {
                Utc.timestamp_millis_opt(raw).single()
            } else {
                Utc.timestamp_opt(raw, 0).single()
            };

            timestamp
                .map(Some)
                .ok_or_else(|| de::Error::custom("invalid unix timestamp"))
        }
        Some(other) => Err(de::Error::custom(format!(
            "unsupported timestamp format: {other}"
        ))),
    }
}

fn deserialize_f64_opt<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_f64()
            .map(Some)
            .ok_or_else(|| de::Error::custom("numeric value cannot be represented as f64")),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            trimmed.parse::<f64>().map(Some).map_err(|error| {
                de::Error::custom(format!("invalid float value '{trimmed}': {error}"))
            })
        }
        Some(other) => Err(de::Error::custom(format!(
            "unsupported float format: {other}"
        ))),
    }
}

fn deserialize_i64_opt<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .map(Some)
            .ok_or_else(|| de::Error::custom("numeric value cannot be represented as i64")),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            trimmed.parse::<i64>().map(Some).map_err(|error| {
                de::Error::custom(format!("invalid integer value '{trimmed}': {error}"))
            })
        }
        Some(other) => Err(de::Error::custom(format!(
            "unsupported integer format: {other}"
        ))),
    }
}

fn deserialize_string_like_opt<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(Value::Number(number)) => Ok(Some(number.to_string())),
        Some(Value::Bool(value)) => Ok(Some(value.to_string())),
        Some(other) => Err(de::Error::custom(format!(
            "unsupported string-like format: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::IncomingMessage;

    #[test]
    fn parses_lat_lng_from_string_payload() {
        let payload = r#"{
            "uuid":"9a13482f-3ee5-588e-a13a-cc35e52f0a71",
            "device_id":"12312312312",
            "msg_class":"STATUS",
            "latitude":"+20.574605",
            "longitude":"-100.359826",
            "alert":""
        }"#;

        let parsed: IncomingMessage = serde_json::from_str(payload).expect("message should parse");

        assert_eq!(parsed.msg_class, "STATUS");
        assert_eq!(parsed.latitude, Some(20.574605));
        assert_eq!(parsed.longitude, Some(-100.359826));
    }

    #[test]
    fn uses_gps_epoch_when_fix_status_is_one() {
        let payload = r#"{
            "device_id":"12312312312",
            "msg_class":"ALERT",
            "fix_status":"1",
            "gps_epoch":1700000000,
            "received_at":"2025-01-01T00:00:00Z"
        }"#;

        let parsed: IncomingMessage = serde_json::from_str(payload).expect("message should parse");

        assert_eq!(parsed.source_epoch(), Some(1700000000));
        assert_eq!(
            parsed.event_occurred_at(),
            Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()
        );
    }

    #[test]
    fn falls_back_to_received_at_when_fix_status_is_not_one() {
        let payload = r#"{
            "device_id":"12312312312",
            "msg_class":"ALERT",
            "fix_status":"0",
            "gps_epoch":1700000000,
            "received_at":"2025-01-01T00:00:00Z"
        }"#;

        let parsed: IncomingMessage = serde_json::from_str(payload).expect("message should parse");
        let expected = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).single().unwrap();

        assert_eq!(parsed.event_occurred_at(), expected);
    }

    #[test]
    fn payload_only_contains_allowed_fields() {
        let payload = r#"{
            "uuid":"bc6f8cef-38b5-5ee7-ba2a-9cfadff3d474",
            "device_id":"0848086072",
            "alert":"Engine ON",
            "latitude":19.216813,
            "longitude":-102.575137,
            "msg_class":"ALERT",
            "stellites":"7",
            "fix_status":"1",
            "engine_status":"OFF",
            "main_battery_voltage":"13.69",
            "backup_batery_voltage":"4.2",
            "other":"should_not_be_present"
        }"#;

        let parsed: IncomingMessage = serde_json::from_str(payload).expect("message should parse");
        let payload = parsed.payload();
        let object = payload.as_object().expect("payload should be an object");

        assert!(object.contains_key("uuid"));
        assert!(object.contains_key("device_id"));
        assert!(object.contains_key("alert"));
        assert!(object.contains_key("latitude"));
        assert!(object.contains_key("longitude"));
        assert!(object.contains_key("msg_class"));
        assert!(object.contains_key("satellites"));
        assert!(object.contains_key("fix_status"));
        assert!(object.contains_key("engine_status"));
        assert!(object.contains_key("main_battery_voltage"));
        assert!(object.contains_key("backup_batery_voltage"));
        assert!(!object.contains_key("other"));
        assert_eq!(object.len(), 11);
    }
}
