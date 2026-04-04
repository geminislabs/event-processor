use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::models::IncomingMessage;

/// Event shape produced to Kafka (matches requested JSON schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id: Uuid,
    pub schema_version: i32,

    pub event_type: String,
    pub event_type_id: Uuid,

    pub source: Source,

    pub unit: Unit,

    pub source_epoch: Option<i64>,

    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,

    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    #[serde(rename = "type")]
    pub r#type: String,
    pub id: String,
    pub message_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unit {
    pub id: Uuid,
}

impl Event {
    /// Transform an IncomingMessage into an Event ready to be produced.
    #[allow(dead_code)]
    pub fn from_incoming(msg: &IncomingMessage) -> Self {
        let event_id = Uuid::new_v4();
        let received_at = Utc::now();

        let message_id = msg.message_id.unwrap_or_else(Uuid::new_v4);
        let source_id = msg
            .device_id
            .clone()
            .unwrap_or_else(|| msg.device_source_id());

        let unit_id = msg.unit_id.unwrap_or_else(Uuid::new_v4);

        Event {
            event_id,
            schema_version: 1,
            event_type: msg.msg_class.clone(),
            event_type_id: Uuid::new_v4(),
            source: Source {
                r#type: "device_message".to_string(),
                id: source_id,
                message_id,
            },
            unit: Unit { id: unit_id },
            source_epoch: msg.source_epoch(),
            occurred_at: msg.event_occurred_at(),
            received_at,
            payload: msg.payload(),
        }
    }
}
