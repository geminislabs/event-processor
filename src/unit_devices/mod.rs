use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::Database;

pub type UnitDeviceMap = HashMap<String, Uuid>;

/// `device_id → unit_id` de las asignaciones vigentes.
///
/// Arranca de la foto de `control_plane.unit_devices` y después solo cambia con
/// los eventos `unit_device` de `control-plane-events` (ADR-008 de
/// siscom-admin-api). El mapa está completo: un equipo que no aparece no tiene
/// asignación vigente, y no se pregunta a la base. Antes cada mensaje de un
/// equipo sin asignar costaba una consulta, y una reasignación no se veía
/// hasta reiniciar porque lo cacheado nunca se invalidaba.
pub struct UnitDeviceResolver {
    cache: RwLock<UnitDeviceMap>,
}

impl UnitDeviceResolver {
    pub async fn load(db: Arc<Database>) -> Result<Self, sqlx::Error> {
        let cache = db.load_unit_devices().await?;
        Ok(Self::from_map(cache))
    }

    pub fn from_map(cache: UnitDeviceMap) -> Self {
        Self {
            cache: RwLock::new(cache),
        }
    }

    pub async fn resolve_by_device_id(&self, device_id: &str) -> Option<Uuid> {
        self.cache.read().await.get(device_id).copied()
    }

    pub async fn apply(&self, device_id: &str, unit_id: Option<Uuid>) {
        let mut cache = self.cache.write().await;
        match unit_id {
            Some(unit_id) => {
                cache.insert(device_id.to_string(), unit_id);
            }
            None => {
                cache.remove(device_id);
            }
        }
    }

    pub async fn len(&self) -> usize {
        self.cache.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reassignment_replaces_the_unit_and_unassign_forgets_it() {
        let vieja = Uuid::new_v4();
        let nueva = Uuid::new_v4();
        let resolver =
            UnitDeviceResolver::from_map(HashMap::from([("0848072989".to_string(), vieja)]));

        resolver.apply("0848072989", Some(nueva)).await;
        assert_eq!(
            resolver.resolve_by_device_id("0848072989").await,
            Some(nueva)
        );

        resolver.apply("0848072989", None).await;
        assert_eq!(resolver.resolve_by_device_id("0848072989").await, None);
    }
}
