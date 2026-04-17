use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::models::{Geofence, GeofenceWithCells};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum GeofenceUpdateEventType {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeofenceUpdateMessage {
    pub event_id: Uuid,
    pub event_type: GeofenceUpdateEventType,
    pub entity: String,
    pub timestamp: DateTime<Utc>,
    pub organization_id: Uuid,
    pub data: GeofenceUpdateData,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum GeofenceUpdateData {
    Upsert(GeofenceUpsertData),
    Delete(GeofenceDeleteData),
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeofenceUpsertData {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub config: Option<Value>,
    pub cells: Vec<u64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeofenceDeleteData {
    pub id: Uuid,
}

impl GeofenceUpdateMessage {
    pub fn into_store_update(self) -> Option<GeofenceStoreUpdate> {
        if !self.entity.eq_ignore_ascii_case("geofence") {
            return None;
        }

        match (self.event_type, self.data) {
            (GeofenceUpdateEventType::Upsert, GeofenceUpdateData::Upsert(data)) => {
                // `created_at` is not present in the Kafka contract. We keep the in-memory
                // shape compatible by using `updated_at` for both timestamps.
                let geofence = Geofence {
                    id: data.id,
                    organization_id: self.organization_id,
                    name: data.name,
                    description: data.description,
                    is_active: data.is_active,
                    config: data.config,
                    created_at: data.updated_at,
                    updated_at: data.updated_at,
                };

                Some(GeofenceStoreUpdate::Upsert(GeofenceWithCells {
                    geofence,
                    h3_indices: data.cells,
                }))
            }
            (GeofenceUpdateEventType::Delete, GeofenceUpdateData::Delete(data)) => {
                Some(GeofenceStoreUpdate::Delete {
                    geofence_id: data.id,
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum GeofenceStoreUpdate {
    Upsert(GeofenceWithCells),
    Delete { geofence_id: Uuid },
}
