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

pub struct GeofenceEvaluator {
    event_type_id: Option<Uuid>,
    store: GeofenceStore,
}

impl GeofenceEvaluator {
    pub fn new(registry: &EventTypeRegistry, store: GeofenceStore) -> Self {
        Self {
            event_type_id: registry.get("Geofence").copied(),
            store,
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
        _context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>> {
        Box::pin(async move {
            let event_type_id = self.event_type_id?;
            let latitude = msg.latitude?;
            let longitude = msg.longitude?;

            // Buscamos qué geofences contienen este punto
            let matching_geofences = self.store.find_geofences_for_point(latitude, longitude);

            if matching_geofences.is_empty() {
                return None;
            }

            // Generamos un evento por cada geofence que contiene el punto
            let events = matching_geofences
                .into_iter()
                .map(|geofence_id| Event {
                    id: Uuid::new_v4(),
                    source_type: "device_message".to_string(),
                    source_id: msg.device_source_id(),
                    source_message_id: msg.message_id,
                    unit_id: msg.unit_id,
                    event_type_id,
                    payload: msg.payload_with_geofence(geofence_id),
                    occurred_at: msg.event_occurred_at(),
                    source_epoch: msg.source_epoch(),
                })
                .collect();

            Some(events)
        })
    }
}
