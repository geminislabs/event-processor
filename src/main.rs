mod buffer;
mod circuit_breaker;
mod config;
mod db;
mod dispatcher;
mod evaluators;
mod health;
mod kafka;
mod models;
mod processors;
mod unit_devices;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::buffer::spawn_buffer_writer;
use crate::circuit_breaker::CircuitBreaker;
use crate::config::AppConfig;
use crate::db::Database;
use crate::dispatcher::{spawn_evaluation_pipeline, Dispatcher};
use crate::evaluators::{EvaluatorContext, GeofenceEvaluator, GeofenceStore, IgnitionEvaluator};
use crate::health::{serve_health, HealthTracker};
use crate::kafka::run_consumer;
use crate::models::{CompletionStatus, PersistRequest, ProcessEnvelope};
use crate::unit_devices::UnitDeviceResolver;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    init_tracing(&config.app.log_level)?;

    let db = Arc::new(Database::connect(&config.postgres).await?);

    let event_types = db.load_event_types().await?;
    tracing::info!(count = event_types.len(), "event_types registry loaded");

    // Cargamos los geofences activos en memoria
    let geofences = db.load_active_geofences().await?;
    tracing::info!(count = geofences.len(), "active geofences loaded");

    let geofence_store = GeofenceStore::new();
    geofence_store.load(geofences);

    // invert registry for id -> code lookup used when producing events to Kafka
    let event_type_lookup = Arc::new(
        event_types
            .iter()
            .map(|(code, id)| (*id, code.clone()))
            .collect::<std::collections::HashMap<uuid::Uuid, String>>(),
    );

    let unit_devices = Arc::new(UnitDeviceResolver::load(db.clone()).await?);
    tracing::info!(
        count = unit_devices.len().await,
        "unit_devices registry loaded"
    );
    let evaluator_context = Arc::new(EvaluatorContext::new(unit_devices));

    let db_breaker = Arc::new(CircuitBreaker::new(
        "postgres",
        config.app.circuit_breaker_failure_threshold,
        config.app.circuit_breaker_reset_timeout,
    ));
    let kafka_breaker = Arc::new(CircuitBreaker::new(
        "kafka",
        config.app.circuit_breaker_failure_threshold,
        config.app.circuit_breaker_reset_timeout,
    ));
    let health = Arc::new(HealthTracker::default());

    // Kafka producer (may use producer-specific credentials/topic)
    let producer_opt = match crate::processors::producer::ProducerService::new(&config.kafka) {
        Ok(p) => Some(Arc::new(p)),
        Err(err) => {
            tracing::warn!(error=%err, "failed to create kafka producer; kafka production disabled");
            None
        }
    };

    let channel_capacity = (config.app.batch_size * 4).max(1024);
    let (process_tx, process_rx) = mpsc::channel::<ProcessEnvelope>(channel_capacity);
    let (persist_tx, persist_rx) = mpsc::channel::<PersistRequest>(channel_capacity);
    let (completion_tx, completion_rx) = mpsc::channel::<CompletionStatus>(channel_capacity);

    let dispatcher = Arc::new(
        Dispatcher::builder()
            .with_context(evaluator_context)
            .register_for_class("ALERT", Arc::new(IgnitionEvaluator::new(&event_types)?))
            .register_global(Arc::new(GeofenceEvaluator::new(
                &event_types,
                geofence_store,
            )))
            .build(),
    );

    let shutdown = CancellationToken::new();
    let mut health_handle = tokio::spawn(serve_health(
        config.app.health_bind_addr.clone(),
        db.clone(),
        health.clone(),
        kafka_breaker.clone(),
        db_breaker.clone(),
        shutdown.clone(),
    ));
    let mut pipeline_handle =
        spawn_evaluation_pipeline(dispatcher, process_rx, persist_tx, completion_tx.clone());
    let mut writer_handle = spawn_buffer_writer(
        config.app.batch_size,
        config.app.batch_timeout,
        db.clone(),
        db_breaker.clone(),
        health.clone(),
        persist_rx,
        completion_tx.clone(),
        producer_opt.clone(),
        Some(event_type_lookup.clone()),
    );
    let mut consumer_handle = tokio::spawn(run_consumer(
        config.kafka.clone(),
        kafka_breaker.clone(),
        health.clone(),
        process_tx,
        completion_rx,
        shutdown.clone(),
    ));

    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
    shutdown.cancel();

    await_result_task_shutdown("kafka consumer", &mut consumer_handle).await?;
    drop(completion_tx);
    await_unit_task_shutdown("evaluation pipeline", &mut pipeline_handle).await?;
    await_unit_task_shutdown("buffer writer", &mut writer_handle).await?;
    await_result_task_shutdown("health server", &mut health_handle).await?;

    Ok(())
}

async fn await_result_task_shutdown<T>(
    name: &str,
    handle: &mut JoinHandle<Result<T>>,
) -> Result<()> {
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    tokio::select! {
        result = &mut *handle => {
            result??;
        }
        _ = &mut timeout => {
            warn!(task = name, "task did not stop after shutdown signal; aborting");
            handle.abort();

            match handle.await {
                Ok(result) => {
                    result?;
                }
                Err(join_error) if join_error.is_cancelled() => {
                    info!(task = name, "task aborted during shutdown");
                }
                Err(join_error) => return Err(join_error.into()),
            }
        }
    }

    Ok(())
}

async fn await_unit_task_shutdown(name: &str, handle: &mut JoinHandle<()>) -> Result<()> {
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    tokio::select! {
        result = &mut *handle => {
            result?;
        }
        _ = &mut timeout => {
            warn!(task = name, "task did not stop after shutdown signal; aborting");
            handle.abort();

            match handle.await {
                Ok(()) => {}
                Err(join_error) if join_error.is_cancelled() => {
                    info!(task = name, "task aborted during shutdown");
                }
                Err(join_error) => return Err(join_error.into()),
            }
        }
    }

    Ok(())
}

fn init_tracing(log_level: &str) -> Result<()> {
    let filter = EnvFilter::try_new(log_level).or_else(|_| EnvFilter::try_new("info"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();

    Ok(())
}
