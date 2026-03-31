# event-processor

Servicio Rust que consume mensajes de Kafka, los enruta a través de evaluadores para generar eventos semánticos y persiste los resultados en PostgreSQL.

## Objetivo

Actuar como capa de procesamiento entre un broker Kafka y una base de datos de eventos. Cada mensaje entrante se enruta al evaluador correspondiente según su clase (`msg_class`). El evaluador decide si el mensaje genera uno o más eventos; si sí, el evento se encola para persistencia en batch.

## Arquitectura

```
Kafka
  │
  ▼
KafkaConsumer  ──►  Dispatcher  ──►  Evaluator(es)
                                          │
                                          ▼
                                    BufferWriter  ──►  PostgreSQL
```

| Componente | Descripción |
|---|---|
| **KafkaConsumer** | Lee mensajes del topic configurado, los deserializa a `IncomingMessage` y los pasa al pipeline. Gestiona offsets manualmente: confirma solo mensajes procesados con éxito. |
| **Dispatcher** | Enruta cada `IncomingMessage` a los evaluadores registrados para su `msg_class`. También corre evaluadores globales en todos los mensajes. |
| **Evaluator** | Trait que decide si un mensaje produce eventos. Cada evaluador implementa `can_handle` y `process`. |
| **BufferWriter** | Acumula eventos producidos por los evaluadores y los persiste en PostgreSQL en batch (por tamaño o por timeout). |
| **HealthTracker** | Mantiene el estado de salud de Kafka y PostgreSQL. Expuesto en `GET /health`. |
| **CircuitBreaker** | Protege Kafka y PostgreSQL. Si acumula `CIRCUIT_BREAKER_FAILURE_THRESHOLD` errores consecutivos, abre el circuito durante `CIRCUIT_BREAKER_RESET_TIMEOUT_MS` ms antes de reintentar. |

## Evaluadores existentes

| Evaluador | Clase | Alertas manejadas |
|---|---|---|
| `IgnitionEvaluator` | `ALERT` | `Engine ON`, `Engine OFF` |
| `GeofenceEvaluator` | global | Mensajes con `latitude` y `longitude` presentes |

## Cómo añadir un nuevo evaluador

**1. Crear el archivo** en `src/evaluators/`:

```rust
// src/evaluators/speed.rs
use futures::future::BoxFuture;
use crate::evaluators::{Evaluator, EvaluatorContext};
use crate::models::{Event, IncomingMessage};

pub struct SpeedEvaluator;

impl Evaluator for SpeedEvaluator {
    fn name(&self) -> &'static str {
        "speed"
    }

    fn can_handle(&self, msg: &IncomingMessage) -> bool {
        msg.routing_key() == "SPEED"
    }

    fn process<'a>(
        &'a self,
        msg: &'a IncomingMessage,
        _context: &'a EvaluatorContext,
    ) -> BoxFuture<'a, Option<Vec<Event>>> {
        Box::pin(async move {
            // construir y devolver eventos, o None si no aplica
            None
        })
    }
}
```

**2. Exportarlo** en `src/evaluators/mod.rs`:

```rust
mod speed;
pub use speed::SpeedEvaluator;
```

**3. Registrarlo** en `src/main.rs` dentro del `Dispatcher::builder()`:

```rust
// Para una clase específica de mensaje:
.register_for_class("SPEED", Arc::new(SpeedEvaluator))

// O para correrlo en todos los mensajes:
.register_global(Arc::new(SpeedEvaluator))
```

El evaluador aparecerá automáticamente en los logs de cada despacho.

## Variables de entorno

### PostgreSQL

| Variable | Requerida | Descripción |
|---|---|---|
| `DB_HOST` | ✅ | Host del servidor |
| `DB_PORT` | ✅ | Puerto (ej. `5432`) |
| `DB_NAME` | ✅ | Nombre de la base de datos |
| `DB_USER` | ✅ | Usuario |
| `DB_PASSWORD` | ✅ | Contraseña |
| `DB_MAX_CONNECTIONS` | ✅ | Tamaño del pool de conexiones |

### Kafka

| Variable | Requerida | Descripción |
|---|---|---|
| `KAFKA_BROKERS` | ✅ | Lista de brokers (`host:puerto`) |
| `KAFKA_TOPIC` | ✅ | Topic a consumir |
| `KAFKA_GROUP_ID` | ✅ | Consumer group ID |
| `KAFKA_USERNAME` | — | Usuario SASL |
| `KAFKA_PASSWORD` | — | Contraseña SASL |
| `KAFKA_SASL_MECHANISM` | — | Mecanismo SASL (ej. `PLAIN`) |
| `KAFKA_SECURITY_PROTOCOL` | — | Protocolo (ej. `SASL_SSL`) |
| `KAFKA_ENABLE_AUTO_COMMIT` | — | Default `false` |
| `KAFKA_AUTO_OFFSET_RESET` | — | Default `latest` |

### Aplicación

| Variable | Requerida | Default | Descripción |
|---|---|---|---|
| `LOG_LEVEL` | — | `info` | Nivel de log (tracing filter) |
| `HEALTH_BIND_ADDR` | — | `0.0.0.0:8080` | Dirección del servidor de health |
| `BATCH_SIZE` | ✅ | — | Máx. eventos por insert batch |
| `BATCH_TIMEOUT_MS` | ✅ | — | Flush forzado si no se llena el batch |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | ✅ | — | Errores consecutivos para abrir el circuito |
| `CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | ✅ | — | Tiempo antes de intentar cerrar el circuito |

Puedes copiar `.env.example` como punto de partida:

```bash
cp .env.example .env
```

## Desarrollo local

```bash
# Compilar
cargo build

# Ejecutar tests
cargo test

# Verificar formato
cargo fmt --check

# Linting
cargo clippy --all-targets --all-features -- -D warnings

# Ejecutar (requiere .env)
cargo run
```

## Health check

```
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

## Deploy

El deploy es automatizado via GitHub Actions (`build-and-test` → `deploy-to-ec2`).

El job de deploy corre **solo en tags** con el prefijo `v`.

### Crear y publicar un tag

```bash
# Crear tag
git tag v1.2.3

# Publicar tag (dispara el workflow)
git push origin v1.2.3

# Eliminar tag
git tag -d v1.0.0
git push origin :refs/tags/v1.0.0
```

El workflow realiza en orden:
1. Valida que exista `Dockerfile`
2. Corre `cargo fmt --check`, `clippy` y `cargo test`
3. Construye y publica la imagen Docker a GHCR
4. Conecta al EC2 por SSH, hace pull de la imagen y levanta el servicio con `docker compose`
5. Verifica el health endpoint antes de considerar el deploy exitoso

Para ver los secretos y variables de entorno requeridos en GitHub, consulta el workflow en [.github/workflows/docker-build.yml](.github/workflows/docker-build.yml).
