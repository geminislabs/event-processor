use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub source_type: String,
    pub source_id: String,
    pub source_message_id: Option<Uuid>,
    pub unit_id: Option<Uuid>,
    pub event_type_id: Uuid,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
    pub source_epoch: Option<i64>,
}
