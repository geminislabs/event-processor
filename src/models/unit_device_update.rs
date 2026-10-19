//! Eventos `unit_device` de `control-plane-events`.
//!
//! El contrato vive en siscom-admin-api (`docs/architecture/eventos-de-control.md`,
//! ADR-008). El topic lleva varias entidades; este servicio solo aplica
//! `unit_device` y descarta el resto sin quejarse.
//!
//! Cada evento trae el estado completo: `UPSERT` con la unidad de la asignación
//! vigente, o `DELETE` cuando el equipo dejó de tener una.

use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum UnitDeviceUpdateEventType {
    Upsert,
    Delete,
}

/// El sobre común de `control-plane-events`. `data` se interpreta según `entity`.
#[derive(Debug, Clone, Deserialize)]
pub struct UnitDeviceUpdateMessage {
    pub event_id: Uuid,
    pub event_type: UnitDeviceUpdateEventType,
    pub entity: String,
    #[serde(default)]
    pub schema_version: Option<u32>,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct UnitDeviceData {
    device_id: String,
    #[serde(default)]
    unit_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitDeviceStoreUpdate {
    Assign { device_id: String, unit_id: Uuid },
    Unassign { device_id: String },
}

/// Versión de `unit_device` que este servicio entiende.
const SCHEMA_VERSION: u32 = 1;

impl UnitDeviceUpdateMessage {
    /// `None` si el evento no es de `unit_device`, es de una versión que no
    /// se conoce o le falta lo necesario.
    pub fn into_store_update(self) -> Option<UnitDeviceStoreUpdate> {
        if !self.entity.eq_ignore_ascii_case("unit_device") {
            return None;
        }
        if self.schema_version.is_some_and(|v| v != SCHEMA_VERSION) {
            return None;
        }

        let data: UnitDeviceData = serde_json::from_value(self.data).ok()?;
        let device_id = data.device_id.trim().to_string();
        if device_id.is_empty() {
            return None;
        }

        match (self.event_type, data.unit_id) {
            (UnitDeviceUpdateEventType::Upsert, Some(unit_id)) => {
                Some(UnitDeviceStoreUpdate::Assign { device_id, unit_id })
            }
            (UnitDeviceUpdateEventType::Upsert, None) | (UnitDeviceUpdateEventType::Delete, _) => {
                Some(UnitDeviceStoreUpdate::Unassign { device_id })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mensaje(value: serde_json::Value) -> UnitDeviceUpdateMessage {
        serde_json::from_value(value).expect("valid payload")
    }

    #[test]
    fn upsert_assigns_unit() {
        let unit_id = Uuid::new_v4();
        let message = mensaje(serde_json::json!({
            "schema_version": 1,
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "unit_device",
            "timestamp": "2026-10-19T00:00:00Z",
            "organization_id": Uuid::new_v4(),
            "data": {
                "device_id": "0848072989",
                "unit_id": unit_id,
                "organization_id": Uuid::new_v4()
            }
        }));

        assert_eq!(
            message.into_store_update(),
            Some(UnitDeviceStoreUpdate::Assign {
                device_id: "0848072989".to_string(),
                unit_id,
            })
        );
    }

    #[test]
    fn delete_carries_only_the_key_and_unassigns() {
        let message = mensaje(serde_json::json!({
            "schema_version": 1,
            "event_id": Uuid::new_v4(),
            "event_type": "DELETE",
            "entity": "unit_device",
            "timestamp": "2026-10-19T00:00:00Z",
            "organization_id": null,
            "data": { "device_id": "0848072989" }
        }));

        assert_eq!(
            message.into_store_update(),
            Some(UnitDeviceStoreUpdate::Unassign {
                device_id: "0848072989".to_string(),
            })
        );
    }

    #[test]
    fn other_entities_of_the_topic_are_ignored() {
        // Un `organization_user` no tiene `device_id`: no debe ni intentar leerlo.
        let message = mensaje(serde_json::json!({
            "schema_version": 1,
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "organization_user",
            "timestamp": "2026-10-19T00:00:00Z",
            "organization_id": Uuid::new_v4(),
            "data": {
                "organization_id": Uuid::new_v4(),
                "user_id": Uuid::new_v4(),
                "role": "member",
                "status": "ACTIVE"
            }
        }));

        assert_eq!(message.into_store_update(), None);
    }

    #[test]
    fn unknown_schema_version_is_ignored() {
        let message = mensaje(serde_json::json!({
            "schema_version": 2,
            "event_id": Uuid::new_v4(),
            "event_type": "UPSERT",
            "entity": "unit_device",
            "timestamp": "2026-10-19T00:00:00Z",
            "organization_id": null,
            "data": { "device_id": "0848072989", "unit_id": Uuid::new_v4() }
        }));

        assert_eq!(message.into_store_update(), None);
    }
}
