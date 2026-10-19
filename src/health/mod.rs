use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
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
    control_plane: ControlPlaneSnapshot,
}

/// Arranca en `ok: false`: solo es `true` desde que el consumidor de
/// `control-plane-events` queda asignado. Un topic de control puede pasar
/// horas sin mensajes, así que no hay «demasiado tiempo sin leer»: solo cuenta
/// si la última lectura falló.
#[derive(Default, Clone, Copy)]
struct ControlPlaneSnapshot {
    ok: bool,
    applied_offset: Option<i64>,
    lag: Option<i64>,
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
    control_plane: ControlPlaneStatus,
    circuit_breakers: CircuitBreakerStatus,
}

#[derive(Serialize)]
struct ControlPlaneStatus {
    ok: bool,
    applied_offset: Option<i64>,
    /// Mensajes publicados aún sin aplicar, según el último sondeo al broker.
    lag: Option<i64>,
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
    // DB health marking removed: events are no longer persisted to DB so these
    // explicit setters are no longer needed. Keep kafka setters only.

    pub fn mark_kafka_ok(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.kafka_ok = true;
        snapshot.last_kafka_success_at = Some(Instant::now());
    }

    pub fn mark_kafka_error(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.kafka_ok = false;
    }

    /// El broker respondió; `lag` es lo publicado que falta por aplicar.
    pub fn mark_control_plane_ok(&self, lag: Option<i64>) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.control_plane.ok = true;
        snapshot.control_plane.lag = lag;
    }

    pub fn mark_control_plane_applied(&self, offset: i64) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.control_plane.ok = true;
        snapshot.control_plane.applied_offset = Some(offset);
    }

    pub fn mark_control_plane_error(&self) {
        let mut snapshot = self.inner.lock().expect("health tracker poisoned");
        snapshot.control_plane.ok = false;
    }

    fn control_plane(&self) -> ControlPlaneSnapshot {
        self.inner
            .lock()
            .expect("health tracker poisoned")
            .control_plane
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

/// Responde 503 si el consumidor de control no puede leer (ADR-008 §9 de
/// siscom-admin-api): el `HEALTHCHECK` del Dockerfile usa `curl -f`, que solo
/// falla con un código de error. Hasta ahora siempre respondía 200, y por eso
/// el contenedor seguía `healthy` con un consumidor que no leía nada.
///
/// `db` y `kafka` siguen siendo informativos: no cambian el código, para no
/// cambiar a la vez el comportamiento del health de todo lo demás.
async fn health_handler(State(state): State<HealthState>) -> (StatusCode, Json<HealthResponse>) {
    let db_live = state.database.health_check().await;
    let (db_ok, kafka_ok, last_success_at) = state.tracker.snapshot();
    let control_plane = state.tracker.control_plane();
    let status = if control_plane.ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(HealthResponse {
            db: ComponentStatus {
                ok: db_ok && db_live,
            },
            kafka: KafkaStatus {
                ok: kafka_ok,
                last_success_at,
            },
            control_plane: ControlPlaneStatus {
                ok: control_plane.ok,
                applied_offset: control_plane.applied_offset,
                lag: control_plane.lag,
            },
            circuit_breakers: CircuitBreakerStatus {
                db: state.db_breaker.state().as_str(),
                kafka: state.kafka_breaker.state().as_str(),
            },
        }),
    )
}

fn instant_to_utc(instant: Instant) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(instant.elapsed()).unwrap_or_default()
}
