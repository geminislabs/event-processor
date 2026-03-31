use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info};

use crate::evaluators::{Evaluator, EvaluatorContext};
use crate::models::{CompletionStatus, Event, IncomingMessage, PersistRequest, ProcessEnvelope};

pub struct Dispatcher {
    grouped: HashMap<String, Vec<Arc<dyn Evaluator>>>,
    global: Vec<Arc<dyn Evaluator>>,
    context: Arc<EvaluatorContext>,
}

pub struct DispatcherBuilder {
    grouped: HashMap<String, Vec<Arc<dyn Evaluator>>>,
    global: Vec<Arc<dyn Evaluator>>,
    context: Option<Arc<EvaluatorContext>>,
}

pub struct DispatchOutcome {
    pub events: Vec<Event>,
    pub evaluators: Vec<&'static str>,
}

impl Dispatcher {
    pub fn builder() -> DispatcherBuilder {
        DispatcherBuilder {
            grouped: HashMap::new(),
            global: Vec::new(),
            context: None,
        }
    }

    pub async fn dispatch(&self, message: &IncomingMessage) -> DispatchOutcome {
        let mut events = Vec::new();
        let mut evaluators = Vec::new();

        if let Some(routed) = self.grouped.get(message.routing_key()) {
            for evaluator in routed {
                apply_evaluator(
                    evaluator.as_ref(),
                    message,
                    self.context.as_ref(),
                    &mut events,
                    &mut evaluators,
                )
                .await;
            }
        }

        for evaluator in &self.global {
            apply_evaluator(
                evaluator.as_ref(),
                message,
                self.context.as_ref(),
                &mut events,
                &mut evaluators,
            )
            .await;
        }

        DispatchOutcome { events, evaluators }
    }
}

impl DispatcherBuilder {
    pub fn register_for_class(mut self, key: &str, evaluator: Arc<dyn Evaluator>) -> Self {
        self.grouped
            .entry(key.to_string())
            .or_default()
            .push(evaluator);
        self
    }

    pub fn register_global(mut self, evaluator: Arc<dyn Evaluator>) -> Self {
        self.global.push(evaluator);
        self
    }

    pub fn with_context(mut self, context: Arc<EvaluatorContext>) -> Self {
        self.context = Some(context);
        self
    }

    pub fn build(self) -> Dispatcher {
        Dispatcher {
            grouped: self.grouped,
            global: self.global,
            context: self.context.unwrap_or_else(|| {
                panic!("dispatcher context is required; call with_context before build")
            }),
        }
    }
}

pub fn spawn_evaluation_pipeline(
    dispatcher: Arc<Dispatcher>,
    mut process_rx: mpsc::Receiver<ProcessEnvelope>,
    persist_tx: mpsc::Sender<PersistRequest>,
    completion_tx: mpsc::Sender<CompletionStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let concurrency = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(4);
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut join_set = JoinSet::new();

        while let Some(envelope) = process_rx.recv().await {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let dispatcher = dispatcher.clone();
            let persist_tx = persist_tx.clone();
            let completion_tx = completion_tx.clone();

            join_set.spawn(async move {
                let _permit = permit;
                process_one(dispatcher, persist_tx, completion_tx, envelope).await;
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(join_error) = result {
                error!(error = %join_error, "evaluation worker panicked");
            }
        }
    })
}

async fn process_one(
    dispatcher: Arc<Dispatcher>,
    persist_tx: mpsc::Sender<PersistRequest>,
    completion_tx: mpsc::Sender<CompletionStatus>,
    envelope: ProcessEnvelope,
) {
    let outcome = dispatcher.dispatch(&envelope.message).await;
    let elapsed_ms = envelope.received_at.elapsed().as_millis() as u64;
    let device_id = envelope.token.device_id.as_deref().unwrap_or("unknown");

    if outcome.events.is_empty() {
        info!(
            device_id,
            evaluators = ?outcome.evaluators,
            processing_time_ms = elapsed_ms,
            "message processed without events"
        );
        let _ = completion_tx
            .send(CompletionStatus {
                token: envelope.token,
                success: true,
            })
            .await;
        return;
    }

    info!(
        device_id,
        evaluators = ?outcome.evaluators,
        processing_time_ms = elapsed_ms,
        produced_events = outcome.events.len(),
        "message evaluated"
    );

    if persist_tx
        .send(PersistRequest {
            events: outcome.events,
            token: envelope.token.clone(),
        })
        .await
        .is_err()
    {
        error!(device_id, "failed to enqueue events for persistence");
        let _ = completion_tx
            .send(CompletionStatus {
                token: envelope.token,
                success: false,
            })
            .await;
    }
}

async fn apply_evaluator(
    evaluator: &dyn Evaluator,
    message: &IncomingMessage,
    context: &EvaluatorContext,
    events: &mut Vec<Event>,
    evaluators: &mut Vec<&'static str>,
) {
    if !evaluator.can_handle(message) {
        return;
    }

    if let Some(mut produced) = evaluator.process(message, context).await {
        evaluators.push(evaluator.name());
        events.append(&mut produced);
    }
}
