mod geofence;
mod ignition;

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::models::{Event, IncomingMessage};
use crate::unit_devices::UnitDeviceResolver;

pub use geofence::{GeofenceEvaluator, GeofenceStore};
pub use ignition::IgnitionEvaluator;

pub struct EvaluatorContext {
    unit_devices: Arc<UnitDeviceResolver>,
}

impl EvaluatorContext {
    pub fn new(unit_devices: Arc<UnitDeviceResolver>) -> Self {
        Self { unit_devices }
    }

    pub fn unit_devices(&self) -> Arc<UnitDeviceResolver> {
        self.unit_devices.clone()
    }
}

pub trait Evaluator: Send + Sync {
    fn name(&self) -> &'static str;
    fn can_handle(&self, msg: &IncomingMessage) -> bool;
    fn process<'a>(
        &'a self,
        msg: &'a IncomingMessage,
        context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>>;
}
