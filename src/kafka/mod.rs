use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::config::KafkaConfig;
use crate::evaluators::GeofenceStore;
use crate::health::HealthTracker;
use crate::models::{
    CommitToken, CompletionStatus, GeofenceStoreUpdate, GeofenceUpdateMessage, IncomingMessage,
    ProcessEnvelope,
};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OffsetStatus {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Default)]
struct PartitionState {
    pending: BTreeMap<i64, OffsetStatus>,
}

impl PartitionState {
    fn insert_pending(&mut self, offset: i64) {
        self.pending.insert(offset, OffsetStatus::Pending);
    }

    fn mark(&mut self, offset: i64, success: bool) {
        self.pending.insert(
            offset,
            if success {
                OffsetStatus::Succeeded
            } else {
                OffsetStatus::Failed
            },
        );
    }

    fn committable_offset(&self) -> Option<(i64, usize)> {
        let mut count = 0usize;
        let mut last_offset = None;

        for (offset, status) in &self.pending {
            match status {
                OffsetStatus::Succeeded => {
                    count += 1;
                    last_offset = Some(*offset);
                }
                OffsetStatus::Pending | OffsetStatus::Failed => break,
            }
        }

        last_offset.map(|offset| (offset + 1, count))
    }

    fn drop_committed(&mut self, count: usize) {
        for _ in 0..count {
            let next_offset = self.pending.iter().next().map(|(offset, _)| *offset);
            if let Some(offset) = next_offset {
                if self.pending.remove(&offset) != Some(OffsetStatus::Succeeded) {
                    break;
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn has_pending(&self) -> bool {
        self.pending
            .values()
            .any(|status| matches!(status, OffsetStatus::Pending))
    }
}

pub async fn run_consumer(
    config: KafkaConfig,
    breaker: std::sync::Arc<CircuitBreaker>,
    health: std::sync::Arc<HealthTracker>,
    process_tx: mpsc::Sender<ProcessEnvelope>,
    mut completion_rx: mpsc::Receiver<CompletionStatus>,
    shutdown: CancellationToken,
) -> Result<()> {
    let consumer = build_consumer(
        &config,
        &config.group_id,
        &config.auto_offset_reset,
        "event-processor-consumer",
    )?;
    consumer.subscribe(&[&config.topic])?;
    tracing::info!(brokers = %config.brokers, topic = %config.topic, group_id = %config.group_id, "kafka consumer started");

    let mut partition_states: HashMap<TopicPartition, PartitionState> = HashMap::new();
    let mut shutting_down = false;

    loop {
        if shutting_down && !partition_states.values().any(PartitionState::has_pending) {
            let uncommitted_offsets = partition_states
                .values()
                .map(|state| state.pending.len())
                .sum::<usize>();

            if uncommitted_offsets > 0 {
                warn!(
                    uncommitted_offsets,
                    "stopping kafka consumer with uncommitted failed offsets"
                );
            }

            break;
        }

        if !shutting_down && !breaker.allow_request() {
            health.mark_kafka_error();
            tokio::select! {
                _ = shutdown.cancelled() => {
                    shutting_down = true;
                }
                maybe_completion = completion_rx.recv(), if !partition_states.is_empty() => {
                    if let Some(completion) = maybe_completion {
                        handle_completion(&consumer, &breaker, &health, completion, &mut partition_states).await;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }

            if shutting_down {
                drop(process_tx.clone());
            }
            continue;
        }

        tokio::select! {
            _ = shutdown.cancelled(), if !shutting_down => {
                info!("kafka consumer stopping after shutdown signal");
                shutting_down = true;
                drop(process_tx.clone());
            }
            maybe_completion = completion_rx.recv(), if !partition_states.is_empty() => {
                if let Some(completion) = maybe_completion {
                    handle_completion(&consumer, &breaker, &health, completion, &mut partition_states).await;
                }
            }
            message = consumer.recv(), if !shutting_down => {
                match message {
                    Ok(message) => {
                        breaker.record_success();
                        health.mark_kafka_ok();
                        handle_message(message, &process_tx, &mut partition_states, &consumer, &breaker, &health).await;
                    }
                    Err(error) => {
                        breaker.record_failure();
                        health.mark_kafka_error();
                        error!(error = %error, "kafka receive failed");
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn run_geofence_updates_consumer(
    config: KafkaConfig,
    store: GeofenceStore,
    breaker: std::sync::Arc<CircuitBreaker>,
    health: std::sync::Arc<HealthTracker>,
    shutdown: CancellationToken,
) -> Result<()> {
    let geofence_group_id = config.group_id.clone();
    let consumer = build_consumer(
        &config,
        &geofence_group_id,
        "earliest",
        "event-processor-geofence-updates-consumer",
    )?;

    consumer.subscribe(&[&config.geofences_update_topic])?;
    info!(
        brokers = %config.brokers,
        topic = %config.geofences_update_topic,
        group_id = %geofence_group_id,
        "geofence updates consumer started"
    );

    loop {
        if !breaker.allow_request() {
            health.mark_kafka_error();

            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("geofence updates consumer stopping after shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }

            continue;
        }

        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("geofence updates consumer stopping after shutdown signal");
                break;
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        breaker.record_success();
                        health.mark_kafka_ok();
                        handle_geofence_update_message(message, &store);
                    }
                    Err(error) => {
                        breaker.record_failure();
                        health.mark_kafka_error();
                        error!(error = %error, "geofence updates kafka receive failed");
                    }
                }
            }
        }
    }

    Ok(())
}

fn build_consumer(
    config: &KafkaConfig,
    group_id: &str,
    auto_offset_reset: &str,
    client_id: &str,
) -> Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    // Distinguish consumers in broker logs and librdkafka messages.
    client.set("client.id", client_id);
    client
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", auto_offset_reset)
        .set("enable.partition.eof", "false")
        .set("socket.keepalive.enable", "true")
        .set("session.timeout.ms", "6000");

    if config.enable_auto_commit {
        warn!("KAFKA_ENABLE_AUTO_COMMIT=true was ignored; manual commits are enforced");
    }

    if let Some(protocol) = &config.security_protocol {
        client.set("security.protocol", protocol);
    }
    if let Some(mechanism) = &config.sasl_mechanism {
        client.set("sasl.mechanism", mechanism);
    }
    if let Some(username) = &config.username {
        client.set("sasl.username", username);
    }
    if let Some(password) = &config.password {
        client.set("sasl.password", password);
    }

    client.create().context("failed to create kafka consumer")
}

fn handle_geofence_update_message(
    message: rdkafka::message::BorrowedMessage<'_>,
    store: &GeofenceStore,
) {
    let payload = match message.payload_view::<str>() {
        Some(Ok(payload)) => payload,
        Some(Err(error)) => {
            warn!(
                error = %error,
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "received invalid UTF-8 geofence update payload"
            );
            return;
        }
        None => {
            warn!(
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "received empty geofence update payload"
            );
            return;
        }
    };

    let update_message = match serde_json::from_str::<GeofenceUpdateMessage>(payload) {
        Ok(message) => message,
        Err(error) => {
            warn!(
                error = %error,
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "malformed geofence update message skipped"
            );
            return;
        }
    };

    let event_id = update_message.event_id;
    let event_timestamp = update_message.timestamp;
    let Some(update) = update_message.into_store_update() else {
        warn!(
            topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            event_id = %event_id,
            "geofence update message ignored due to unsupported event contract"
        );
        return;
    };

    match update {
        GeofenceStoreUpdate::Upsert(geofence_with_cells) => {
            let geofence_id = geofence_with_cells.geofence.id;
            let is_active = geofence_with_cells.geofence.is_active;
            let cell_count = geofence_with_cells.h3_indices.len();

            if !is_active {
                store.remove(geofence_id);
                info!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    event_id = %event_id,
                    event_timestamp = %event_timestamp,
                    geofence_id = %geofence_id,
                    "geofence upsert marked inactive; removed from in-memory store"
                );
            } else {
                store.upsert(geofence_with_cells);
                info!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    event_id = %event_id,
                    event_timestamp = %event_timestamp,
                    geofence_id = %geofence_id,
                    cell_count,
                    "geofence upsert applied to in-memory store"
                );
            }
        }
        GeofenceStoreUpdate::Delete { geofence_id } => {
            store.remove(geofence_id);
            info!(
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                event_id = %event_id,
                event_timestamp = %event_timestamp,
                geofence_id = %geofence_id,
                "geofence delete applied to in-memory store"
            );
        }
    }
}

async fn handle_message(
    message: rdkafka::message::BorrowedMessage<'_>,
    process_tx: &mpsc::Sender<ProcessEnvelope>,
    partition_states: &mut HashMap<TopicPartition, PartitionState>,
    consumer: &StreamConsumer,
    breaker: &CircuitBreaker,
    health: &HealthTracker,
) {
    let topic_partition = TopicPartition {
        topic: message.topic().to_string(),
        partition: message.partition(),
    };
    let offset = message.offset();
    partition_states
        .entry(topic_partition.clone())
        .or_default()
        .insert_pending(offset);

    let token = CommitToken {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
        offset,
        device_id: None,
    };

    let payload = match message.payload_view::<str>() {
        Some(Ok(payload)) => payload,
        Some(Err(error)) => {
            warn!(error = %error, topic = message.topic(), partition = message.partition(), offset, "received invalid UTF-8 payload");
            mark_and_commit(
                consumer,
                partition_states,
                CompletionStatus {
                    token,
                    success: true,
                },
                breaker,
                health,
            )
            .await;
            return;
        }
        None => {
            warn!(
                topic = message.topic(),
                partition = message.partition(),
                offset,
                "received empty payload"
            );
            mark_and_commit(
                consumer,
                partition_states,
                CompletionStatus {
                    token,
                    success: true,
                },
                breaker,
                health,
            )
            .await;
            return;
        }
    };

    match serde_json::from_str::<IncomingMessage>(payload) {
        Ok(parsed) => {
            let token = CommitToken {
                device_id: parsed.device_id.clone(),
                ..token
            };

            if process_tx
                .send(ProcessEnvelope {
                    message: parsed,
                    token,
                    received_at: std::time::Instant::now(),
                })
                .await
                .is_err()
            {
                error!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset,
                    "failed to enqueue message for evaluation"
                );
                mark_and_commit(
                    consumer,
                    partition_states,
                    CompletionStatus {
                        token: CommitToken {
                            topic: topic_partition.topic,
                            partition: topic_partition.partition,
                            offset,
                            device_id: None,
                        },
                        success: false,
                    },
                    breaker,
                    health,
                )
                .await;
            }
        }
        Err(error) => {
            warn!(error = %error, topic = message.topic(), partition = message.partition(), offset, "malformed message skipped");
            mark_and_commit(
                consumer,
                partition_states,
                CompletionStatus {
                    token,
                    success: true,
                },
                breaker,
                health,
            )
            .await;
        }
    }
}

async fn handle_completion(
    consumer: &StreamConsumer,
    breaker: &CircuitBreaker,
    health: &HealthTracker,
    completion: CompletionStatus,
    partition_states: &mut HashMap<TopicPartition, PartitionState>,
) {
    if completion.success {
        info!(
            device_id = completion.token.device_id.as_deref().unwrap_or("unknown"),
            partition = completion.token.partition,
            offset = completion.token.offset,
            "message processing completed"
        );
    } else {
        warn!(
            device_id = completion.token.device_id.as_deref().unwrap_or("unknown"),
            partition = completion.token.partition,
            offset = completion.token.offset,
            "message processing failed and will remain uncommitted"
        );
    }

    mark_and_commit(consumer, partition_states, completion, breaker, health).await;
    if !breaker.allow_request() {
        warn!("kafka circuit breaker remains open after completion processing");
    }
}

async fn mark_and_commit(
    consumer: &StreamConsumer,
    partition_states: &mut HashMap<TopicPartition, PartitionState>,
    completion: CompletionStatus,
    breaker: &CircuitBreaker,
    health: &HealthTracker,
) {
    let key = TopicPartition {
        topic: completion.token.topic.clone(),
        partition: completion.token.partition,
    };

    if let Some(state) = partition_states.get_mut(&key) {
        state.mark(completion.token.offset, completion.success);
    }

    if let Some((next_offset, count)) = partition_states
        .get(&key)
        .and_then(PartitionState::committable_offset)
    {
        let mut topic_partition_list = TopicPartitionList::new();
        if let Err(error) = topic_partition_list.add_partition_offset(
            &completion.token.topic,
            completion.token.partition,
            Offset::Offset(next_offset),
        ) {
            breaker.record_failure();
            health.mark_kafka_error();
            error!(error = %error, topic = completion.token.topic, partition = completion.token.partition, offset = next_offset, "failed to build commit request");
            return;
        }

        if let Err(error) = consumer.commit(&topic_partition_list, CommitMode::Async) {
            breaker.record_failure();
            health.mark_kafka_error();
            error!(error = %error, topic = completion.token.topic, partition = completion.token.partition, offset = next_offset, "failed to commit processed offsets");
            return;
        }

        breaker.record_success();
        health.mark_kafka_ok();

        if let Some(state) = partition_states.get_mut(&key) {
            state.drop_committed(count);
            if state.is_empty() {
                partition_states.remove(&key);
            }
        }
    }
}
