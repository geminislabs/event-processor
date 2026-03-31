use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::db::Database;
use crate::health::HealthTracker;
use crate::models::{CompletionStatus, Event, PersistRequest};

pub fn spawn_buffer_writer(
    batch_size: usize,
    batch_timeout: Duration,
    db: Arc<Database>,
    db_breaker: Arc<CircuitBreaker>,
    health: Arc<HealthTracker>,
    mut persist_rx: mpsc::Receiver<PersistRequest>,
    completion_tx: mpsc::Sender<CompletionStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(batch_timeout);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut pending = Vec::new();
        let mut buffered_events = 0usize;

        loop {
            tokio::select! {
                maybe_request = persist_rx.recv() => {
                    match maybe_request {
                        Some(request) => {
                            buffered_events += request.events.len();
                            pending.push(request);

                            if buffered_events >= batch_size {
                                flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events).await;
                            }
                        }
                        None => {
                            if !pending.is_empty() {
                                flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events).await;
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !pending.is_empty() {
                        flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events).await;
                    }
                }
            }
        }
    })
}

async fn flush_pending(
    db: &Database,
    db_breaker: &CircuitBreaker,
    health: &HealthTracker,
    completion_tx: &mpsc::Sender<CompletionStatus>,
    pending: &mut Vec<PersistRequest>,
    buffered_events: &mut usize,
) {
    if pending.is_empty() {
        return;
    }

    if !db_breaker.allow_request() {
        warn!(
            pending_messages = pending.len(),
            "database circuit breaker is open; deferring commits"
        );
        fail_pending(completion_tx, pending.drain(..).collect()).await;
        *buffered_events = 0;
        health.mark_db_error();
        return;
    }

    let drained: Vec<PersistRequest> = pending.drain(..).collect();
    let event_count = drained
        .iter()
        .map(|request| request.events.len())
        .sum::<usize>();
    let flattened = drained
        .iter()
        .flat_map(|request| request.events.iter().cloned())
        .collect::<Vec<Event>>();

    match db.insert_events(&flattened).await {
        Ok(_) => {
            db_breaker.record_success();
            health.mark_db_ok();
            info!(
                batch_messages = drained.len(),
                batch_events = event_count,
                "persisted event batch"
            );
            for request in drained {
                let _ = completion_tx
                    .send(CompletionStatus {
                        token: request.token,
                        success: true,
                    })
                    .await;
            }
        }
        Err(error) => {
            db_breaker.record_failure();
            health.mark_db_error();
            error!(error = %error, batch_messages = drained.len(), batch_events = event_count, "failed to persist event batch");
            fail_pending(completion_tx, drained).await;
        }
    }

    *buffered_events = 0;
}

async fn fail_pending(
    completion_tx: &mpsc::Sender<CompletionStatus>,
    requests: Vec<PersistRequest>,
) {
    for request in requests {
        let _ = completion_tx
            .send(CompletionStatus {
                token: request.token,
                success: false,
            })
            .await;
    }
}
