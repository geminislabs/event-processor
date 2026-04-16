use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Una geofence con su información completa.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Geofence {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub config: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Uma geofence con sus celdas H3 precargadas.
/// Es la estructura que mantenemos en memoria.
#[derive(Debug, Clone)]
pub struct GeofenceWithCells {
    pub geofence: Geofence,
    /// Conjunto de índices H3 que pertenecen a esta geofence.
    /// Se usan u64 porque SQLx retorna BIGINT como i64, pero los índices H3
    /// en realidad son u64. Los convertimos para mayor claridad.
    pub h3_indices: Vec<u64>,
}

/// Un índice H3 asociado a una geofence.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct GeofenceCell {
    pub geofence_id: Uuid,
    pub h3_index: u64,
}
