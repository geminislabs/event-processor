# Eventos de control en event-processor

El contrato de `control-plane-events` vive en siscom-admin-api:
`docs/architecture/eventos-de-control.md`, con la decisión en el ADR-008. Este
documento solo dice qué toma de ahí este servicio y cómo lo aplica.

## Qué lee

| Entidad | Para qué |
|---|---|
| `unit_device` | `device_id → unit_id` de la asignación vigente, para atribuir cada mensaje GPS a su unidad |

Las demás entidades del topic se descartan sin registrar nada. Un
`schema_version` distinto de `1` también se descarta.

## Cómo arranca

1. Anota el offset final de cada partición de `control-plane-events`. Si no
   puede leer el topic (no existe o falta la ACL), **el proceso no arranca**.
2. Carga la foto de `control_plane.unit_devices`, la vista que mantiene
   admin-api. No lee la tabla `unit_devices`.
3. Consume desde los offsets del paso 1, sin grupo y sin commit: cada arranque
   empieza de su foto. Lo publicado entre 1 y 2 se aplica dos veces, lo cual no
   hace daño porque cada evento trae el estado completo.

## Cómo aplica

- `UPSERT` con `unit_id`: el equipo queda en esa unidad (alta o reasignación).
- `DELETE`: el equipo deja de tener unidad.

El mapa está completo. Un equipo que no aparece no tiene asignación vigente, y
**no se consulta la base** por cada mensaje de un equipo sin asignar.

## Salud

`GET /health` incluye `control_plane: { ok, applied_offset, lag }` y responde
**503** si el consumidor de control no puede leer. El `HEALTHCHECK` del
Dockerfile usa `curl -f`, así que el contenedor pasa a `unhealthy`.

Como el topic puede pasar horas sin mensajes, cada 30 s se pregunta al broker
por el final de cada partición. Si responde, `ok` vuelve a `true` y `lag` dice
cuánto falta por aplicar. Es una consulta al broker, no a la base.

El consumidor de control no comparte el circuit breaker de Kafka con el de
mensajes GPS: un fallo suyo no pausa el procesamiento.

## Despliegue

- Variable `KAFKA_CONTROL_PLANE_TOPIC` (por defecto `control-plane-events`).
- ACL de lectura sobre el topic para el usuario `events-processor`. No hace
  falta ACL de grupo.
- `SELECT` sobre `control_plane.unit_devices` para el usuario de base (`siscom`).
- Requiere siscom-admin-api con la migración `044_vistas_del_plano_de_control`.

## Geocercas

No forman parte de este contrato: siguen en `geofences-updates` (plural), con
su propio grupo, `KAFKA_GEOFENCES_GROUP_ID` (por defecto
`events-processor-geofences`, el nombre que trae la ACL del contrato), para no
compartirlo con el consumidor de mensajes GPS.
