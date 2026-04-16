use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::config::PostgresConfig;
use crate::models::{EventTypeRegistry, Geofence, GeofenceWithCells};

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn connect(config: &PostgresConfig) -> Result<Self, sqlx::Error> {
        let connection_string = format!(
            "postgres://{}:{}@{}:{}/{}",
            config.user, config.password, config.host, config.port, config.db_name
        );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&connection_string)
            .await?;

        Ok(Self { pool })
    }

    // `insert_events` removed: events are no longer persisted to the DB.

    pub async fn health_check(&self) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }

    pub async fn load_event_types(&self) -> Result<EventTypeRegistry, sqlx::Error> {
        let rows = sqlx::query("SELECT id, code FROM event_types")
            .fetch_all(&self.pool)
            .await?;

        let registry = rows
            .into_iter()
            .map(|r| {
                let code: String = r.get("code");
                let id: Uuid = r.get("id");
                (code, id)
            })
            .collect();

        Ok(registry)
    }

    pub async fn load_unit_devices(
        &self,
    ) -> Result<std::collections::HashMap<String, Uuid>, sqlx::Error> {
        let rows = sqlx::query("SELECT unit_id, device_id FROM unit_devices")
            .fetch_all(&self.pool)
            .await?;

        let registry = rows
            .into_iter()
            .map(|r| {
                let device_id: String = r.get("device_id");
                let unit_id: Uuid = r.get("unit_id");
                (device_id, unit_id)
            })
            .collect();

        Ok(registry)
    }

    pub async fn find_unit_id_by_device(
        &self,
        device_id: &str,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        let row = sqlx::query("SELECT unit_id, device_id FROM unit_devices WHERE device_id = $1")
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get("unit_id")))
    }

    /// Carga todas las geofences activas junto con sus índices H3.
    /// Optimizado para leer una sola vez y mantener en memoria.
    pub async fn load_active_geofences(&self) -> Result<Vec<GeofenceWithCells>, sqlx::Error> {
        // Primero cargamos todas las geofences activas
        let geofences = sqlx::query(
            r#"
            SELECT id, organization_id, name, description, is_active, config, created_at, updated_at
            FROM public.geofences
            WHERE is_active = true
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut result = Vec::new();

        // Para cada geofence, cargamos sus celdas H3
        for geofence_row in geofences {
            let geofence_id: Uuid = geofence_row.get("id");

            let cells =
                sqlx::query("SELECT h3_index FROM public.geofence_cells WHERE geofence_id = $1")
                    .bind(geofence_id)
                    .fetch_all(&self.pool)
                    .await?;

            let h3_indices: Vec<u64> = cells
                .into_iter()
                .map(|r| {
                    let bigint: i64 = r.get("h3_index");
                    bigint as u64
                })
                .collect();

            let geofence = Geofence {
                id: geofence_id,
                organization_id: geofence_row.get("organization_id"),
                name: geofence_row.get("name"),
                description: geofence_row.get("description"),
                is_active: geofence_row.get("is_active"),
                config: geofence_row.get("config"),
                created_at: geofence_row.get("created_at"),
                updated_at: geofence_row.get("updated_at"),
            };

            result.push(GeofenceWithCells {
                geofence,
                h3_indices,
            });
        }

        Ok(result)
    }
}
