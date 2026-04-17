# event-processor

Servicio Rust que consume mensajes de Kafka, los enruta a través de evaluadores para generar eventos semánticos y los publica en Kafka (topic `unit-events`).

## Resumen del cambio

Este servicio ya no persiste eventos en la tabla `events` de PostgreSQL. El flujo actual: los mensajes entrantes se evalúan y, si producen eventos, se emiten a Kafka. La base de datos se usa únicamente en tiempo de arranque para cargar datos iniciales (catalogo `event_types` y `unit_devices`) y para consultas de resolución que los evaluadores puedan necesitar.

## Objetivo

Procesar mensajes entrantes desde Kafka, generar eventos semánticos mediante evaluadores y publicar esos eventos en el topic `unit-events`. El diseño prioriza latencia y simplicidad operativa: la publicación a Kafka es best-effort (no transaccional con la BD).

## Arquitectura

```text
Kafka (input)
    │
    ▼
KafkaConsumer  ──►  Dispatcher  ──►  Evaluator(es)
                                                                                    │
                                                                                    ▼
                                                                        BufferWriter  ──►  Kafka (unit-events)
```

Puntos clave:

- La inserción en PostgreSQL fue removida; ya no hay dual-write.
- Cada `Event` producido tiene `id` (UUID) generado por el servicio y se utiliza como `event_id` en Kafka.
- Se incluye `event_type_id` (UUID) en el payload Kafka para mantener la referencia al tipo canónico del evento.

## Flujo de procesamiento (detalle)

1. El consumidor Kafka lee mensajes del topic configurado y los convierte a `IncomingMessage`.
2. `Dispatcher` enruta el mensaje a uno o varios `Evaluator`s (por clase y globales).
3. Cada `Evaluator` puede producir cero o más `models::Event`. Estos `Event` ahora incluyen un `id: Uuid` generado en el evaluador.
4. `spawn_buffer_writer` agrupa y dispara la publicación a Kafka (en tareas separadas, comportamiento best-effort).
5. Cuando se encola la publicación, se envía un `CompletionStatus` para permitir que el consumidor confirme offsets (mensajes considerados procesados).

Garantías y limitaciones:

- No se garantiza la durabilidad en BD. Si Kafka falla y el mensaje no es re-entregado, los eventos pueden perderse.
- Producer configurado con idempotence y `acks=all` para reducir duplicados, pero no es equivalente a un outbox transaccional.

## Componentes

| Componente | Descripción |
| --- | --- |
| **KafkaConsumer** | Consume mensajes, deserializa y envía al pipeline. Control manual de offsets. |
| **Dispatcher** | Ejecuta evaluadores y recolecta eventos producidos. |
| **Evaluator** | Implementa la lógica de negocio para detectar y construir eventos. |
| **BufferWriter** | Agrupa eventos y los publica a Kafka (topic `unit-events`). |
| **ProducerService** | Abstracción sobre `rdkafka::FutureProducer` para enviar eventos JSON. |
| **HealthTracker** | Endpoint `/health` que refleja estado de dependencias (DB sólo para checks iniciales y health). |

## Evaluadores existentes

| Evaluador | Clase | Alertas manejadas |
| --- | --- | --- |
| `IgnitionEvaluator` | `ALERT` | `Engine ON`, `Engine OFF` |
| `GeofenceEvaluator` | global | Mensajes con `latitude` y `longitude` presentes |

### Ejemplos de eventos generados

#### IgnitionEvaluator: evento Engine ON

**Mensaje de entrada (Kafka)**:

```json
{
  "uuid": "550e8400-e29b-41d4-a716-446655440001",
  "deviceId": "device-001",
  "msgClass": "ALERT",
  "alert": "Engine ON",
  "timestamp": "2026-04-16T14:30:45Z",
  "lat": -33.8456,
  "lng": -56.1603
}
```

**Evento generado (payload del evento)**:

```json
{
  "id": "12345678-1234-5678-1234-567812345678",
  "source_type": "device_message",
  "source_id": "device-001",
  "source_message_id": "550e8400-e29b-41d4-a716-446655440001",
  "unit_id": "99999999-9999-9999-9999-999999999999",
  "event_type_id": "00000000-0000-0000-0000-000000000001",
  "payload": {
    "uuid": "550e8400-e29b-41d4-a716-446655440001",
    "device_id": "device-001",
    "alert": "Engine ON",
    "msg_class": "ALERT"
  },
  "occurred_at": "2026-04-16T14:30:45Z",
  "source_epoch": null
}
```

#### IgnitionEvaluator: evento Engine OFF

**Mensaje de entrada (Kafka)**:

```json
{
  "uuid": "550e8400-e29b-41d4-a716-446655440002",
  "deviceId": "device-001",
  "msgClass": "ALERT",
  "alert": "Engine OFF",
  "timestamp": "2026-04-16T14:35:20Z",
  "lat": -33.8456,
  "lng": -56.1603
}
```

**Evento generado**:

```json
{
  "id": "87654321-4321-8765-4321-876543218765",
  "source_type": "device_message",
  "source_id": "device-001",
  "source_message_id": "550e8400-e29b-41d4-a716-446655440002",
  "unit_id": "99999999-9999-9999-9999-999999999999",
  "event_type_id": "00000000-0000-0000-0000-000000000002",
  "payload": {
    "uuid": "550e8400-e29b-41d4-a716-446655440002",
    "device_id": "device-001",
    "alert": "Engine OFF",
    "msg_class": "ALERT"
  },
  "occurred_at": "2026-04-16T14:35:20Z",
  "source_epoch": null
}
```

#### GeofenceEvaluator: evento de detectar entrada a geocerca

**Mensaje de entrada (Kafka)**:

```json
{
  "uuid": "550e8400-e29b-41d4-a716-446655440003",
  "deviceId": "device-001",
  "msgClass": "POSITION",
  "timestamp": "2026-04-16T14:40:10Z",
  "lat": -33.8423,
  "lng": -56.1605
}
```

**Evento generado** (geofence_id como ejemplo):

```json
{
  "id": "aaaabbbb-cccc-dddd-eeee-ffff11112222",
  "source_type": "device_message",
  "source_id": "device-001",
  "source_message_id": "550e8400-e29b-41d4-a716-446655440003",
  "unit_id": "99999999-9999-9999-9999-999999999999",
  "event_type_id": "ffffffff-ffff-ffff-ffff-000000000003",
  "payload": {
    "uuid": "550e8400-e29b-41d4-a716-446655440003",
    "device_id": "device-001",
    "msg_class": "POSITION",
    "latitude": -33.8423,
    "longitude": -56.1605,
    "geofence_id": "550e8400-e29b-41d4-a716-446655440001"
  },
  "occurred_at": "2026-04-16T14:40:10Z",
  "source_epoch": null
}
```

**Nota**: Si el mismo mensaje intersecta múltiples geocercas activas, se genera un evento por cada una con diferentes `geofence_id` en el payload (y distinto `id` de evento).

### Propósito

Detectar cuándo un dispositivo entra o está dentro de una geocerca. Para cada mensaje con coordenadas, el evaluador:

1. Convierte el punto (lat/lng) a un índice H3
2. Busca qué geocercas contienen ese índice
3. Genera un evento por cada geocerca encontrada

### Flujo de carga inicial

```text
Startup
  │
  ▼
Database::load_active_geofences()
  │
  ├─ SELECT * FROM geofences WHERE is_active = true
  │ (Obtiene: id, organization_id, name, description, config, etc.)
  │
  └─ Para cada geofence:
      ├─ SELECT h3_index FROM geofence_cells WHERE geofence_id = ?
      │ (Precarga todos los índices H3 que definen esa geocerca)
      │
      └─ Construye GeofenceWithCells { geofence, h3_indices: Vec<u64> }
         │
         └─ Almacena en GeofenceStore (DashMap)
```

**Resultado**: Todas las geocercas activas están en memoria, listas para búsquedas rápidas.

### Estructura en memoria: GeofenceStore

```rust
pub struct GeofenceStore {
    geofences: Arc<DashMap<Uuid, GeofenceWithCells>>,
}
```

**Características**:

- **DashMap**: Mapa concurrente que permite lecturas simultáneas sin bloqueos mutex
- **Arc**: Permite compartir el almacén entre múltiples evaluadores y threads
- **GeofenceWithCells**: Contiene la geocerca completa + vector precargado de índices H3

**Métodos**:

- `new()` - Crea un almacén vacío
- `load(Vec<GeofenceWithCells>)` - Carga inicial desde BD (usada en `main()`)
- `find_geofences_for_point(lat, lng) -> Vec<Uuid>` - Búsqueda principal
- `upsert(GeofenceWithCells)` - Actualiza/agrega geocerca (para actualizaciones en caliente vía Kafka)
- `remove(Uuid)` - Elimina geocerca

### Proceso de evaluación: punto a geocerca

Cuando llega un mensaje con `latitude` y `longitude`:

```text
IncomingMessage { latitude: -33.8456, longitude: -56.1603, ... }
         │
         ▼
GeofenceEvaluator::process()
         │
         ├─ Extrae lat/lng
         │
         ├─ Convierte a índice H3 (Resolución 10)
         │  └─ LatLng::new(-33.8456, -56.1603) -> CellIndex (u64)
         │
         ├─ Busca en GeofenceStore.find_geofences_for_point()
         │  └─ Itera todas las geocercas
         │  └─ Compara índice H3 contra h3_indices de cada una
         │  └─ Retorna Vec<geofence_id> con coincidencias
         │
         ├─ Por cada geofence_id encontrado:
         │  └─ Crea Event con event_type_id = "Geofence"
         │  └─ Incluye geofence_id en el payload
         │
         └─ Retorna Option<Vec<Event>> (None si no hay coincidencias)
```

### Datos de entrada: tablas PostgreSQL

**Tabla `geofences`**:

```sql
CREATE TABLE public.geofences (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    created_by      UUID NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    config          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Tabla `geofence_cells`** (índices H3):

```sql
CREATE TABLE public.geofence_cells (
    geofence_id UUID NOT NULL,
    h3_index    BIGINT NOT NULL,
    PRIMARY KEY (geofence_id, h3_index),
    CONSTRAINT fk_geofence_cells_geofence
        FOREIGN KEY (geofence_id)
        REFERENCES public.geofences(id)
        ON DELETE CASCADE
);
```

**Ejemplo de datos**:

```sql
-- Una geocerca (ej: cerca de la Plaza de Mayo, Buenos Aires)
INSERT INTO geofences (id, organization_id, created_by, name, is_active)
VALUES ('550e8400-e29b-41d4-a716-446655440001', '...',  '...', 'Plaza Mayo', true);

-- Índices H3 (Resolución 10) que definen su perímetro
INSERT INTO geofence_cells (geofence_id, h3_index) VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 613825429990039551),
    ('550e8400-e29b-41d4-a716-446655440001', 613825429989915647),
    ('550e8400-e29b-41d4-a716-446655440001', 613825429989980415),
    ('550e8400-e29b-41d4-a716-446655440001', 613825429990104319),
    ...
```

### H3: ¿por qué esta indexación espacial?

H3 es un sistema de indexación hexagonal jerárquico de Uber. Características:

- **Resoluciones 0-15**: La resolución 10 = celdas de ~1-2 km² (buen balance para ciudades)
- **Ventaja**: Prebúsqueda O(1) en GeofenceStore en lugar de cálculos geométricos costosos
- **Limitación**: Menor precisión que geometría exacta, pero suficiente para casos de uso en campo (geofencing de vehículos)

### Optimizaciones de diseño

1. **Sin bloqueos en lectura**: DashMap permite miles de búsquedas simultáneas sin sincronización
2. **Datos precargados**: Una sola consulta BD al iniciar; acceso O(1) después
3. **Vectores de índices**: h3_indices es un Vec< u64 > (memoria contigua, búsqueda lineal rápida)
4. **Resolución fija**: Usar siempre Resolución 10 simplifica búsquedas y actualización en caliente

### Preparación para actualizaciones en caliente

El servicio ya consume `KAFKA_GEOFENCES_UPDATE_TOPIC` para aplicar cambios en caliente sobre `GeofenceStore` sin reinicio.

Contrato soportado:

- `event_type`: `UPSERT` o `DELETE`
- `entity`: debe ser `geofence`
- `UPSERT`: requiere `data.cells` (snapshot completo)
- `DELETE`: requiere `data.id`
- `config`: se conserva como JSONB (`serde_json::Value`) sin transformaciones
- `timestamp` y `data.updated_at`: formato UTC con sufijo `Z`

Comportamiento:

- `UPSERT` con `is_active=true`: `geofence_store.upsert(...)`
- `UPSERT` con `is_active=false`: se elimina del store en memoria
- `DELETE`: `geofence_store.remove(geofence_id)`

Esto permite cambios de cobertura geográfica en tiempo real mientras el consumidor principal sigue evaluando mensajes.

Ejemplo lógico de flujo:

- Un mensaje llega: `{ action: "upsert", geofence_id, h3_indices, ... }`
- El consumer llama `geofence_store.upsert(new_geofence_with_cells)`
- Sin downtime, sin bloqueos en evaluación

## Carga inicial de datos (BD)

Aunque ya no persistimos eventos, el servicio carga datos iniciales desde PostgreSQL al arrancar:

- `event_types` (mapa code -> UUID)
- `unit_devices` (resolución device_id -> unit_id)

Esto implica que la base de datos debe existir y contener estas tablas con los datos iniciales antes de arrancar el servicio. Puedes preparar scripts SQL/CSV para poblar estas tablas (ver sección siguiente).

## Variables de entorno (resumen)

Las variables principales están en `.env`.

### PostgreSQL (solo para carga inicial y resolución)

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_MAX_CONNECTIONS`

### Kafka (consumo y producción)

- `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GEOFENCES_UPDATE_TOPIC`, `KAFKA_GROUP_ID`, `KAFKA_PRODUCER_TOPIC` (por defecto `unit-events`), credenciales SASL si aplica.

### Aplicación

- `LOG_LEVEL`, `HEALTH_BIND_ADDR`, `BATCH_SIZE`, `BATCH_TIMEOUT_MS`, `CIRCUIT_BREAKER_FAILURE_THRESHOLD`, `CIRCUIT_BREAKER_RESET_TIMEOUT_MS`

## Scripts de carga inicial (recomendación)

Proporciona SQL o CSV para poblar `event_types` y `unit_devices`. Ejemplo mínimo SQL:

```sql
-- event_types
INSERT INTO event_types (id, code) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Engine ON'),
    ('00000000-0000-0000-0000-000000000002', 'Engine OFF');

-- unit_devices
INSERT INTO unit_devices (unit_id, device_id) VALUES
    ('11111111-1111-1111-1111-111111111111', 'device-123');
```

Coloca estos scripts en `/db/migrations` o un directorio similar y ejecútalos antes de arrancar el servicio.

## Desarrollo local

```bash
# Compilar
cargo build

# Ejecutar tests
cargo test

# Verificar formato
cargo fmt --check

# Linting
cargo clippy --all-targets -- -D warnings

# Ejecutar (requiere .env con las variables mínimas)
cargo run
```

## Health check

```text
GET /health
```

Ejemplo de respuesta:

```json
{
    "db":               { "ok": true },
    "kafka":            { "ok": true, "last_success_at": "2026-03-30T12:00:00Z" },
    "circuit_breakers": { "db": "closed", "kafka": "closed" }
}
```

## Producción a Kafka: detalles técnicos

- El `Event` producido incluye los campos: `event_id` (UUID), `event_type_id` (UUID), `event_type` (string), `source`, `unit`, `occurred_at`, `received_at`, `payload`, etc.
- `event_id` se genera en el momento de creación del `models::Event` y se usa como clave del mensaje en Kafka.
- El envío usa `rdkafka` con `enable.idempotence=true` y `acks=all` para minimizar duplicados; sin embargo, no existe una garantía transaccional con la BD tras la reciente eliminación de la persistencia.

## Deploy

El flujo de CI/CD permanece igual (ver workflows en `.github/workflows`). Asegúrate de que la BD de pre-producción esté poblada con `event_types` y `unit_devices` antes de desplegar.

## ¿Qué fue removido?

- Persistencia en `events` (tabla): el código de inserción fue eliminado.
- Módulo `outbox`: eliminado (no se usa actualmente).
- Funciones auxiliares relacionadas con marcaje de salud de BD y circuit breaker para escritura fueron eliminadas o simplificadas.

Si quieres, puedo:

- añadir scripts SQL/CSV para poblar `event_types` y `unit_devices`,
- generar un pequeño `README.seed.md` con instrucciones de carga,
- o crear un comando/ejemplo `cargo run --example seed_db` que haga la carga inicial.
