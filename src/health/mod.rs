use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::circuit_breaker::CircuitBreaker;
use crate::db::Database;

const KAFKA_HEALTH_STALE_AFTER: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct HealthTracker {
    inner: Mutex<HealthSnapshot>,
}

#[derive(Default)]
struct HealthSnapshot {
    db_ok: bool,
    kafka_ok: bool,
    last_kafka_success_at: Option<Instant>,
}

#[derive(Clone)]
struct HealthState {
    database: Arc<Database>,
    tracker: Arc<HealthTracker>,
    kafka_breaker: Arc<CircuitBreaker>,
    db_breaker: Arc<CircuitBreaker>,
}

#[derive(Serialize)]
struct HealthResponse {
    db: ComponentStatus,
    kafka: KafkaStatus,
    circuit_breakers: CircuitBreakerStatus,
}

#[derive(Serialize)]
struct ComponentStatus {
    ok: bool,
}

#[derive(Serialize)]
struct KafkaStatus {
    ok: bool,
    last_success_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct CircuitBreakerStatus {
    db: &'static str,
    kafka: &'static str,
}

impl HealthTracker {
    pub fn mark_db_ok(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.db_ok = true;
    }

    pub fn mark_db_error(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.db_ok = false;
    }

    pub fn mark_kafka_ok(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.kafka_ok = true;
        snapshot.last_kafka_success_at = Some(Instant::now());
    }

    pub fn mark_kafka_error(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.kafka_ok = false;
    }

    fn snapshot(&self) -> (bool, bool, Option<DateTime<Utc>>) {
        let snapshot = self.inner.lock().expect("health tracker poisoned");
        let last_success_at = snapshot.last_kafka_success_at.map(instant_to_utc);
        let kafka_ok = snapshot.kafka_ok
            && snapshot
                .last_kafka_success_at
                .map(|last_success_at| last_success_at.elapsed() <= KAFKA_HEALTH_STALE_AFTER)
                .unwrap_or(false);

        (snapshot.db_ok, kafka_ok, last_success_at)
    }
}

pub async fn serve_health(
    bind_addr: String,
    database: Arc<Database>,
    tracker: Arc<HealthTracker>,
    kafka_breaker: Arc<CircuitBreaker>,
    db_breaker: Arc<CircuitBreaker>,
    shutdown: CancellationToken,
) -> Result<()> {
    let state = HealthState {
        database,
        tracker,
        kafka_breaker,
        db_breaker,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(state);

    let addr: SocketAddr = bind_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "health endpoint listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;

    Ok(())
}

async fn health_handler(State(state): State<HealthState>) -> Json<HealthResponse> {
    let db_live = state.database.health_check().await;
    let (db_ok, kafka_ok, last_success_at) = state.tracker.snapshot();

    Json(HealthResponse {
        db: ComponentStatus {
            ok: db_ok && db_live,
        },
        kafka: KafkaStatus {
            ok: kafka_ok,
            last_success_at,
        },
        circuit_breakers: CircuitBreakerStatus {
            db: state.db_breaker.state().as_str(),
            kafka: state.kafka_breaker.state().as_str(),
        },
    })
}

fn instant_to_utc(instant: Instant) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(instant.elapsed()).unwrap_or_default()
}
