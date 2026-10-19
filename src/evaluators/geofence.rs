use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use futures::future::BoxFuture;
use h3o::{LatLng, Resolution};
use uuid::Uuid;

use crate::evaluators::{Evaluator, EvaluatorContext};
use crate::models::{Event, EventTypeRegistry, GeofenceWithCells, IncomingMessage};

/// Almacena geofences en memoria para acceso rápido y sin bloqueos.
/// Usa DashMap para permitir lecturas concurrentes sin sincronización.
/// La clave es el geofence_id.
pub struct GeofenceStore {
    geofences: Arc<DashMap<Uuid, GeofenceWithCells>>,
}

impl GeofenceStore {
    /// Crea un nuevo GeofenceStore vacío.
    pub fn new() -> Self {
        Self {
            geofences: Arc::new(DashMap::new()),
        }
    }

    /// Carga geofences desde un vector (típicamente desde la BD).
    pub fn load(&self, geofences: Vec<GeofenceWithCells>) {
        self.geofences.clear();
        for gf in geofences {
            self.geofences.insert(gf.geofence.id, gf);
        }
    }

    /// Actualiza o agrega una geofence.
    #[allow(dead_code)]
    pub fn upsert(&self, geofence: GeofenceWithCells) {
        self.geofences.insert(geofence.geofence.id, geofence);
    }

    /// Elimina una geofence.
    #[allow(dead_code)]
    pub fn remove(&self, geofence_id: Uuid) {
        self.geofences.remove(&geofence_id);
    }

    /// Obtiene un clon de todas las geofences activas.
    #[allow(dead_code)]
    pub fn get_all(&self) -> Vec<GeofenceWithCells> {
        self.geofences
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Busca qué geofences contienen un punto lat/lng en una resolución H3 específica.
    /// Por defecto usamos resolución 10 que es un buen balance entre precisión y performance.
    pub fn find_geofences_for_point(&self, latitude: f64, longitude: f64) -> Vec<Uuid> {
        let resolution = Resolution::Ten;

        let lat_lng = match LatLng::new(latitude, longitude) {
            Ok(ll) => ll,
            Err(_) => return Vec::new(),
        };

        let cell = lat_lng.to_cell(resolution);
        let cell_index = u64::from(cell);

        let mut results = Vec::new();

        for entry in self.geofences.iter() {
            if entry.value().h3_indices.contains(&cell_index) {
                results.push(*entry.key());
            }
        }

        results
    }
}

impl Default for GeofenceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for GeofenceStore {
    fn clone(&self) -> Self {
        Self {
            geofences: Arc::clone(&self.geofences),
        }
    }
}

/// Mantiene el estado de qué geofences contiene cada unidad.
/// Clave: unit_id (como String para soportar device_id cuando unit_id es None).
/// Valor: conjunto de geofence_ids en los que estaba la última vez.
#[derive(Clone, Default)]
pub struct GeofenceStateTracker {
    state: Arc<DashMap<String, HashSet<Uuid>>>,
}

impl GeofenceStateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Actualiza el estado de la unidad y devuelve (entered, exited).
    pub fn update(&self, unit_key: &str, current: HashSet<Uuid>) -> (Vec<Uuid>, Vec<Uuid>) {
        let mut entry = self.state.entry(unit_key.to_string()).or_default();
        let previous = entry.value().clone();

        let entered: Vec<Uuid> = current.difference(&previous).copied().collect();
        let exited: Vec<Uuid> = previous.difference(&current).copied().collect();

        *entry = current;
        (entered, exited)
    }
}

pub struct GeofenceEvaluator {
    enter_event_type_id: Option<Uuid>,
    exit_event_type_id: Option<Uuid>,
    store: GeofenceStore,
    state_tracker: GeofenceStateTracker,
}

impl GeofenceEvaluator {
    pub fn new(
        registry: &EventTypeRegistry,
        store: GeofenceStore,
        state_tracker: GeofenceStateTracker,
    ) -> Self {
        Self {
            enter_event_type_id: registry.get("geofence_enter").copied(),
            exit_event_type_id: registry.get("geofence_exit").copied(),
            store,
            state_tracker,
        }
    }
}

impl Evaluator for GeofenceEvaluator {
    fn name(&self) -> &'static str {
        "geofence"
    }

    fn can_handle(&self, msg: &IncomingMessage) -> bool {
        // Evaluamos si el mensaje tiene coordenadas validadas
        msg.latitude.is_some() && msg.longitude.is_some()
    }

    fn process<'a>(
        &'a self,
        msg: &'a IncomingMessage,
        context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>> {
        Box::pin(async move {
            let latitude = msg.latitude?;
            let longitude = msg.longitude?;

            let resolved_unit_id = if let Some(unit_id) = msg.unit_id {
                Some(unit_id)
            } else if let Some(device_id) = msg.device_id.as_deref() {
                context.unit_devices().resolve_by_device_id(device_id).await
            } else {
                None
            };

            // Clave de estado: preferimos unit_id, si no device_id, si no UUID del mensaje
            let unit_key = resolved_unit_id
                .map(|u| u.to_string())
                .or_else(|| msg.device_id.clone())
                .unwrap_or_else(|| "unknown".to_string());

            // Geofences que contienen el punto actual
            let current_ids: HashSet<Uuid> = self
                .store
                .find_geofences_for_point(latitude, longitude)
                .into_iter()
                .collect();

            // Calcular entradas y salidas respecto al estado anterior
            let (entered, exited) = self.state_tracker.update(&unit_key, current_ids);

            if entered.is_empty() && exited.is_empty() {
                return None;
            }

            let occurred_at = msg.event_occurred_at();
            let source_epoch = msg.source_epoch();
            let source_id = msg.device_source_id();
            let mut events: Vec<Event> = Vec::new();

            if let Some(enter_type_id) = self.enter_event_type_id {
                for geofence_id in &entered {
                    events.push(Event {
                        id: Uuid::new_v4(),
                        source_type: "device_message".to_string(),
                        source_id: source_id.clone(),
                        source_message_id: msg.message_id,
                        unit_id: resolved_unit_id,
                        event_type_id: enter_type_id,
                        payload: msg.payload_with_geofence(*geofence_id),
                        occurred_at,
                        source_epoch,
                    });
                }
            }

            if let Some(exit_type_id) = self.exit_event_type_id {
                for geofence_id in &exited {
                    events.push(Event {
                        id: Uuid::new_v4(),
                        source_type: "device_message".to_string(),
                        source_id: source_id.clone(),
                        source_message_id: msg.message_id,
                        unit_id: resolved_unit_id,
                        event_type_id: exit_type_id,
                        payload: msg.payload_with_geofence(*geofence_id),
                        occurred_at,
                        source_epoch,
                    });
                }
            }

            if events.is_empty() {
                None
            } else {
                Some(events)
            }
        })
    }
}
