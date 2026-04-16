mod event;
mod geofence;
mod incoming_message;

use std::collections::HashMap;
use std::time::Instant;

use uuid::Uuid;

pub use event::Event;
pub use geofence::{Geofence, GeofenceWithCells};
pub use incoming_message::IncomingMessage;

/// Loaded once at startup from `SELECT id, code FROM event_types`.
pub type EventTypeRegistry = HashMap<String, Uuid>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CommitToken {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessEnvelope {
    pub message: IncomingMessage,
    pub token: CommitToken,
    pub received_at: Instant,
}

#[derive(Debug, Clone)]
pub struct PersistRequest {
    pub events: Vec<Event>,
    pub token: CommitToken,
}

#[derive(Debug, Clone)]
pub struct CompletionStatus {
    pub token: CommitToken,
    pub success: bool,
}
