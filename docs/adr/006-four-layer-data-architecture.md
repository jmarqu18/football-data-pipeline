# ADR-006: Arquitectura de 4 Capas (RAW → CLEAN → FEATURES → ENRICHED)

**Status:** Accepted
**Date:** 2026-03-20
**Decision makers:** Juanje Márquez

## Context

El pipeline ingiere datos de 2 fuentes heterogéneas y necesita transformarlos hasta un formato apto para consumo en Datasette. Cada etapa tiene requisitos distintos: persistencia inmutable, normalización relacional, cálculo de métricas derivadas, y aplanamiento para consumo.

### Requisitos

- **Inmutabilidad:** Los datos originales de cada fuente deben preservarse sin modificación para permitir re-procesamiento.
- **Reproducibilidad:** Dado el límite de 100 calls/día de API-Football, debe poder re-ejecutarse el pipeline sin llamadas adicionales.
- **Separación de concerns:** Cada capa debe tener una responsabilidad única y un formato de almacenamiento apropiado.
- **Trazabilidad:** Debe poder rastrearse un valor en ENRICHED hasta su origen en RAW.
- **Escalabilidad:** El pipeline debe poder expandirse a más ligas sin cambios arquitectónicos.

## Decision

Estructurar el pipeline en 4 capas con formatos de almacenamiento específicos:

```
RAW (Parquet) → CLEAN (PostgreSQL) → FEATURES (Parquet) → ENRICHED (SQLite / Datasette)
```

### Capa 1 — RAW (Parquet + JSON cache)

**Propósito:** Preservar los datos tal cual llegan de cada fuente, validados pero sin transformar.

**Formato:** Parquet columnar (un archivo por endpoint/fuente) + JSON cache para API-Football.

**Justificación de Parquet sobre JSON/CSV:**

- Tipado fuerte: esquemas definidos por modelos Pydantic v2.
- Compresión columnar: ratios 5-10x sobre JSON.
- Lectura parcial: puede cargar columnas específicas sin leer el archivo completo.
- Integración nativa con pandas y pyarrow para la capa FEATURES.

**JSON cache (API-Football):** Se preserva el JSON crudo por endpoint antes de la validación Pydantic. Esto permite:

- Debuggear problemas de parsing sin re-llamar a la API.
- Re-procesar desde cache cuando expira el TTL de Parquet.
- Auditar cambios en el schema de la API.

### Capa 2 — CLEAN (PostgreSQL)

**Propósito:** Datos normalizados, relacionales, con entity resolution aplicada.

**Formato:** PostgreSQL 18 con 8 tablas. Ver DDL en `config/sql/init.sql`.

**Justificación de PostgreSQL:**

- Integridad referencial: claves foráneas entre `teams → players → stats`.
- Joins eficientes: las consultas ENRICHED cruzan 5+ tablas.
- Transacciones ACID: la entity resolution requiere inserts atómicos.
- Tipos nativos: `DATE`, `NUMERIC`, arrays para stats.
- Madurez: herramienta conocida, documentación extensa, hosting en cualquier proveedor.

**Schema:**

| Tabla                    | Propósito                                                   |
| ------------------------ | ----------------------------------------------------------- |
| `teams`                  | Identidad de equipos resueltos con IDs cruzados             |
| `players`                | Identidad de jugadores resueltos con metadata de resolución |
| `player_season_stats`    | Stats observables de API-Football (goals, assists, minutes) |
| `player_season_advanced` | Métricas avanzadas Understat (xG, xA, npxG, xGChain)        |
| `player_shots`           | Datos shot-level de Understat (coordenadas, xG)             |
| `player_profile`         | Datos de scouting (height, weight, position)                |
| `player_injuries`        | Historial de lesiones                                       |
| `player_transfers`       | Historial de traspasos                                      |

### Capa 3 — FEATURES (Parquet)

**Propósito:** Métricas derivadas calculadas sobre CLEAN, en formato columnar para análisis.

**Formato:** Parquet columnar en `data/features/`.

**Justificación de Parquet sobre PostgreSQL:**

- Las features se calculan batch, no requieren consultas ad-hoc.
- El output se consume principalmente desde Python (análisis, ML) y desde la capa ENRICHED.
- Parquet permite schema evolution: añadir nuevas features sin migrar DB.
- Cero overhead operativo: no hay que mantener un schema DB para features.

**Features calculadas:**

- Per-90 normalizados: goals, assists, shots, cards.
- xG overperformance: goals - xG, ratio real/esperado.
- Percentiles de liga por posición.
- Features de disponibilidad: días de lesión, número de transferencias.

### Capa 4 — ENRICHED (SQLite / Datasette)

**Propósito:** Vista desnormalizada para consumo rápido desde Datasette.

**Formato:** SQLite en `data/enriched/enriched.db`, servido por Datasette en puerto `:8001`.

**Justificación de SQLite + Datasette:**

- Zero configuración: SQLite es un solo archivo, no requiere servidor.
- Datasette proporciona UI, API JSON, exportación CSV, y plugins (`datasette-vega`, `datasette-render-image-tags`).
- La vista desnormalizada evita joins complejos en el frontend.
- SQLite soporta queries SQL completas para exploración ad-hoc.

## Alternatives Considered

### 2 capas: RAW → SQLite (sin normalización)

- Pros: Simple, menos código
- Cons: Sin integridad referencial, sin entity resolution, sin métricas derivadas. Los datos de ambas fuentes no se podrían cruzar.
- Rejected: El problema central (entity resolution) requiere una capa CLEAN.

### Todas las capas en PostgreSQL

- Pros: Un solo sistema de almacenamiento, joins entre features y raw data posible
- Cons: RAW inmutable difícil de garantizar (PostgreSQL permite UPDATE), features como tablas requieren migraciones, overhead de DB para datos que solo se leen batch
- Rejected: Parquet es más apropiado para datos inmutables y procesamiento batch.

### Todas las capas en Parquet + DuckDB

- Pros: Sin servidor DB, DuckDB permite consultas SQL sobre Parquet
- Cons: Sin integridad referencial en CLEAN, sin UI tipo Datasette, menos ecosistema de herramientas
- Rejected: PostgreSQL aporta integridad referencial que es crítica para entity resolution.

### MongoDB / Document store

- Pros: Schema flexible, facilidad inicial
- Cons: Los datos son inherentemente relacionales (teams → players → stats). Entity resolution requiere joins y actualizaciones atómicas.
- Rejected: Modelo relacional es el correcto para este dominio.

## Consequences

**Positivas:**

- Cada capa puede evolucionar independientemente (schema evolution en RAW/FEATURES).
- Reprocesamiento completo del pipeline sin calls API (cache RAW + cache JSON).
- Trazabilidad: cualquier valor en Datasette se puede rastrear hasta su Parquet RAW y su JSON cache.
- Escalabilidad: añadir nueva fuente solo requiere un nuevo loader RAW y posiblemente una pasada extra en entity resolution.
- El formato Parquet permite integrar herramientas de ML/analytics directamente.

**Negativas:**

- 4 formatos de almacenamiento distintos (JSON, Parquet, PostgreSQL, SQLite) aumentan la complejidad operativa.
- La capa CLEAN es un cuello de botella: todo pasa por entity resolution en PostgreSQL.
- Datos duplicados entre capas (RAW + CLEAN + FEATURES + ENRICHED contienen esencialmente los mismos datos en diferentes niveles de procesamiento).
- La consistencia entre capas debe gestionarse manualmente (no hay un mecanismo automático que garantice que FEATURES está sincronizado con CLEAN).

## Referencias

- Config: `config/ingestion.yaml`
- DDL: `config/sql/init.sql`
- RAW models: `src/pipeline/models/raw.py`
- CLEAN models: `src/pipeline/models/clean.py`
- FEATURES models: `src/pipeline/models/features.py`
- Entity resolution: `src/pipeline/entity_resolution.py`
- Feature engineering: `src/pipeline/feature_engineering.py`
- Export enrich: `src/pipeline/export_enriched.py`
