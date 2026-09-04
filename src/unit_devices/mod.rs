use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::db::Database;

pub type UnitDeviceMap = HashMap<String, Uuid>;

pub struct UnitDeviceResolver {
    db: Arc<Database>,
    cache: RwLock<UnitDeviceMap>,
}

impl UnitDeviceResolver {
    pub async fn load(db: Arc<Database>) -> Result<Self, sqlx::Error> {
        let cache = db.load_unit_devices().await?;
        Ok(Self {
            db,
            cache: RwLock::new(cache),
        })
    }

    pub async fn resolve_by_device_id(&self, device_id: &str) -> Result<Option<Uuid>, sqlx::Error> {
        if let Some(unit_id) = self.cache.read().await.get(device_id).copied() {
            return Ok(Some(unit_id));
        }

        let fetched = self.db.find_unit_id_by_device(device_id).await?;

        if let Some(unit_id) = fetched {
            self.apply(device_id, Some(unit_id)).await;
            return Ok(Some(unit_id));
        }

        Ok(None)
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
