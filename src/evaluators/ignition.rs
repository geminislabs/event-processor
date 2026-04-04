use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use tracing::warn;
use uuid::Uuid;

use crate::evaluators::{Evaluator, EvaluatorContext};
use crate::models::{Event, EventTypeRegistry, IncomingMessage};

// Canonicalize incoming alert text to a supported, canonical event name.
// Add aliases here (e.g. map "Turn Off" -> "Engine OFF") so they're
// easy to find and remove later.
fn canonical_alert(input: Option<&str>) -> Option<&'static str> {
    match input.map(|s| s.trim()) {
        Some(s) if s.eq_ignore_ascii_case("engine on") => Some("Engine ON"),
        Some(s) if s.eq_ignore_ascii_case("engine off") => Some("Engine OFF"),
        Some(s) if s.eq_ignore_ascii_case("turn off") => Some("Engine OFF"),
        Some(s) if s.eq_ignore_ascii_case("turn on") => Some("Engine ON"),
        _ => None,
    }
}

pub struct IgnitionEvaluator {
    engine_on_id: Uuid,
    engine_off_id: Uuid,
}

impl IgnitionEvaluator {
    pub fn new(registry: &EventTypeRegistry) -> Result<Self> {
        let engine_on_id = *registry
            .get("Engine ON")
            .ok_or_else(|| anyhow!("event_type 'Engine ON' not found in registry"))?;
        let engine_off_id = *registry
            .get("Engine OFF")
            .ok_or_else(|| anyhow!("event_type 'Engine OFF' not found in registry"))?;

        Ok(Self {
            engine_on_id,
            engine_off_id,
        })
    }
}

impl Evaluator for IgnitionEvaluator {
    fn name(&self) -> &'static str {
        "ignition"
    }

    fn can_handle(&self, msg: &IncomingMessage) -> bool {
        msg.routing_key() == "ALERT" && canonical_alert(msg.alert.as_deref()).is_some()
    }

    fn process<'a>(
        &'a self,
        msg: &'a IncomingMessage,
        context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>> {
        Box::pin(async move {
            let event_type_id = match canonical_alert(msg.alert.as_deref()) {
                Some("Engine ON") => self.engine_on_id,
                Some("Engine OFF") => self.engine_off_id,
                _ => return None,
            };

            let device_id = msg.device_id.as_deref()?;
            let unit_id = match context.unit_devices().resolve_by_device_id(device_id).await {
                Ok(value) => value,
                Err(error) => {
                    warn!(device_id, error = %error, "failed to resolve unit_id for device_id");
                    None
                }
            };

            Some(vec![Event {
                id: Uuid::new_v4(),
                source_type: "device_message".to_string(),
                source_id: msg.device_source_id(),
                source_message_id: msg.message_id,
                unit_id,
                event_type_id,
                payload: msg.payload(),
                occurred_at: msg.event_occurred_at(),
                source_epoch: msg.source_epoch(),
            }])
        })
    }
}
