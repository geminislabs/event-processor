use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum UnitDeviceUpdateEventType {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnitDeviceUpdateMessage {
    pub event_id: Uuid,
    pub event_type: UnitDeviceUpdateEventType,
    pub entity: String,
    pub timestamp: DateTime<Utc>,
    pub organization_id: Option<Uuid>,
    pub data: UnitDeviceUpdateData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnitDeviceUpdateData {
    pub device_id: String,
    pub unit_id: Option<Uuid>,
    #[serde(default)]
    pub previous_unit_id: Option<Uuid>,
    #[serde(default)]
    pub previous_organization_id: Option<Uuid>,
    #[serde(default)]
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitDeviceStoreUpdate {
    Assign { device_id: String, unit_id: Uuid },
    Unassign { device_id: String },
}

impl UnitDeviceUpdateMessage {
    pub fn into_store_update(self) -> Option<UnitDeviceStoreUpdate> {
        if !self.entity.eq_ignore_ascii_case("unit_device") {
            return None;
        }

        let device_id = self.data.device_id.trim().to_string();
        if device_id.is_empty() {
            return None;
        }

        let is_active = self.data.is_active.unwrap_or(true);

        match self.event_type {
            UnitDeviceUpdateEventType::Delete => {
                Some(UnitDeviceStoreUpdate::Unassign { device_id })
            }
            UnitDeviceUpdateEventType::Upsert if !is_active || self.data.unit_id.is_none() => {
                Some(UnitDeviceStoreUpdate::Unassign { device_id })
            }
            UnitDeviceUpdateEventType::Upsert => self
                .data
                .unit_id
                .map(|unit_id| UnitDeviceStoreUpdate::Assign { device_id, unit_id }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_active_assigns_unit() {
        let unit_id = Uuid::new_v4();
        let message: UnitDeviceUpdateMessage = serde_json::from_value(serde_json::json!({
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "unit_device",
            "timestamp": "2026-09-02T00:00:00Z",
            "organization_id": Uuid::new_v4(),
            "data": {
                "device_id": "0848072989",
                "unit_id": unit_id,
                "is_active": true
            }
        }))
        .expect("valid payload");

        assert_eq!(
            message.into_store_update(),
            Some(UnitDeviceStoreUpdate::Assign {
                device_id: "0848072989".to_string(),
                unit_id,
            })
        );
    }

    #[test]
    fn upsert_inactive_or_delete_unassigns() {
        let inactive: UnitDeviceUpdateMessage = serde_json::from_value(serde_json::json!({
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "unit_device",
            "timestamp": "2026-09-02T00:00:00Z",
            "organization_id": null,
            "data": {
                "device_id": "0848072989",
                "unit_id": null,
                "is_active": false
            }
        }))
        .expect("valid payload");

        assert_eq!(
            inactive.into_store_update(),
            Some(UnitDeviceStoreUpdate::Unassign {
                device_id: "0848072989".to_string(),
            })
        );

        let delete: UnitDeviceUpdateMessage = serde_json::from_value(serde_json::json!({
            "event_id": Uuid::new_v4(),
            "event_type": "DELETE",
            "entity": "unit_device",
            "timestamp": "2026-09-02T00:00:00Z",
            "organization_id": null,
            "data": { "device_id": "0848072989" }
        }))
        .expect("valid payload");

        assert_eq!(
            delete.into_store_update(),
            Some(UnitDeviceStoreUpdate::Unassign {
                device_id: "0848072989".to_string(),
            })
        );
    }

    #[test]
    fn ignores_unknown_entity() {
        let message: UnitDeviceUpdateMessage = serde_json::from_value(serde_json::json!({
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "geofence",
            "timestamp": "2026-09-02T00:00:00Z",
            "organization_id": Uuid::new_v4(),
            "data": {
                "device_id": "0848072989",
                "unit_id": Uuid::new_v4(),
                "is_active": true
            }
        }))
        .expect("valid payload");

        assert_eq!(message.into_store_update(), None);
    }
}
