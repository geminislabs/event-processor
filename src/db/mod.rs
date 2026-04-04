use sqlx::postgres::{PgPoolOptions, PgQueryResult};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::config::PostgresConfig;
use crate::models::{Event, EventTypeRegistry};

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

    pub async fn insert_events(&self, events: &[Event]) -> Result<PgQueryResult, sqlx::Error> {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO events (id, source_type, source_id, source_message_id, unit_id, event_type_id, payload, occurred_at, source_epoch) ",
        );

        query_builder.push_values(events, |mut row, event| {
            row.push_bind(event.id)
                .push_bind(&event.source_type)
                .push_bind(&event.source_id)
                .push_bind(event.source_message_id)
                .push_bind(event.unit_id)
                .push_bind(event.event_type_id)
                .push_bind(&event.payload)
                .push_bind(event.occurred_at)
                .push_bind(event.source_epoch);
        });

        query_builder.build().execute(&self.pool).await
    }

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
}
