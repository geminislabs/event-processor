use futures::future::BoxFuture;
use uuid::Uuid;

use crate::evaluators::{Evaluator, EvaluatorContext};
use crate::models::{Event, EventTypeRegistry, IncomingMessage};

pub struct GeofenceEvaluator {
    event_type_id: Option<Uuid>,
}

impl GeofenceEvaluator {
    pub fn new(registry: &EventTypeRegistry) -> Self {
        Self {
            event_type_id: registry.get("Geofence").copied(),
        }
    }
}

impl Evaluator for GeofenceEvaluator {
    fn name(&self) -> &'static str {
        "geofence"
    }

    fn can_handle(&self, _msg: &IncomingMessage) -> bool {
        false
    }

    fn process<'a>(
        &'a self,
        msg: &'a IncomingMessage,
        _context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>> {
        Box::pin(async move {
            let event_type_id = self.event_type_id?;

            if msg.latitude.is_none() || msg.longitude.is_none() {
                return None;
            }

            Some(vec![Event {
                id: Uuid::new_v4(),
                source_type: "device_message".to_string(),
                source_id: msg.device_source_id(),
                source_message_id: msg.message_id,
                unit_id: msg.unit_id,
                event_type_id,
                payload: msg.payload(),
                occurred_at: msg.event_occurred_at(),
                source_epoch: msg.source_epoch(),
            }])
        })
    }
}
