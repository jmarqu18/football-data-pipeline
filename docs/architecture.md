# Arquitectura del Pipeline — Football Data Pipeline

## Visión General

Pipeline de datos multi-fuente que ingesta, limpia, fusiona y sirve datos de fútbol provenientes de **API-Football** y **Understat**. Resuelve entity resolution entre fuentes con sistemas de IDs incompatibles. Airflow 3.x orquesta todo el flujo; Pydantic v2 valida los datos en cada transición entre capas.

**Scope actual:** La Liga 2024/25 (configurable vía `config/ingestion.yaml`).

Para el contexto de por qué estas fuentes y no otras, ver [ADR-002](adr/002-data-source-selection.md). Para por qué no se incluye event data, ver [ADR-003](adr/003-event-data-out-of-scope.md). La estrategia de entity resolution se detalla en [ADR-004](adr/004-entity-resolution-strategy.md).

```
┌─────────────┐   ┌─────────────┐
│ API-Football │   │  Understat   │
│  (REST API)  │   │  (scraping)  │
└──────┬───────┘   └──────┬───────┘
       │                  │
       ▼                  ▼
┌──────────────┐   ┌─────────────────────────────────┐
│ CACHE        │   │         CAPA 1 — RAW            │
│ data/cache/  │──▶│         data/raw/ (Parquet)      │
│ (JSON crudo) │   │         Pydantic validation      │
└──────────────┘   └──────────────┬──────────────────┘
                                  │ Entity Resolution
                                  ▼
                   ┌─────────────────────────────────┐
                   │         CAPA 2 — CLEAN          │
                   │         PostgreSQL (8 tablas)    │
                   │         teams → players → stats  │
                   └──────────────┬──────────────────┘
                                  │ Feature Engineering
                                  ▼
                   ┌─────────────────────────────────┐
                   │         CAPA 3 — FEATURES       │
                   │         data/features/ (Parquet) │
                   │         Per-90, xG, percentiles  │
                   └──────────────┬──────────────────┘
                                  │ Aplanamiento
                                  ▼
                   ┌─────────────────────────────────┐
                   │         CAPA 4 — ENRICHED       │
                   │         SQLite → Datasette       │
                   │         Vista plana + imágenes   │
                   └─────────────────────────────────┘
```

## Fuentes de Datos

| Fuente | Qué aporta | Granularidad | Acceso |
|--------|-----------|-------------|--------|
| **API-Football** | Stats jugador/equipo, lesiones, transferencias, imágenes | Season + Player | REST API, free tier (100 calls/día) |
| **Understat** | Métricas avanzadas (xG, xA, npxG, xGChain, xGBuildup) + shot data | Temporada/jugador + Tiro individual | `soccerdata` (scraping) |

Cada fuente usa su propio sistema de IDs. El pipeline los reconcilia en la capa CLEAN mediante entity resolution.

## Detalle de las Capas

### Cache — API-Football (`data/cache/`)

API-Football tiene un límite de 100 calls/día en el free tier. Para no agotar la cuota en desarrollo y hacer el pipeline reproducible sin calls adicionales, toda respuesta de la API se persiste como JSON crudo antes de transformar.

```
data/cache/api_football/
├── players/
│   ├── league_140_season_2024_team_529_page_1.json
│   └── league_140_season_2024_team_541_page_1.json
├── injuries/
│   └── league_140_season_2024.json
└── transfers/
    ├── team_529.json
    └── team_541.json
```

El loader consulta cache antes de cada call. Si el cache existe y no ha expirado (`cache_ttl_hours` configurable), usa cache. Expone `force_refresh: bool` para invalidar manualmente.

### Capa 1 — RAW (`data/raw/`)

| Aspecto | Detalle |
|---------|---------|
| **Formato** | Parquet, un directorio por fuente |
| **Contenido** | Datos tal cual llegan, sin transformación |
| **Validación** | Modelos Pydantic v2 que verifican esquema y tipos |

```
data/raw/
├── api_football/
│   ├── players.parquet
│   ├── injuries.parquet
│   └── transfers.parquet
└── understat/
    ├── shots.parquet
    └── player_season.parquet    # xG, xA, npxG, xGChain, xGBuildup
```

Understat produce 2 outputs: shot-level (cada disparo con coordenadas y xG) y season-level (métricas avanzadas agregadas por jugador/temporada). Son datos distintos que no se derivan uno del otro.

### Capa 2 — CLEAN (PostgreSQL)

| Aspecto | Detalle |
|---------|---------|
| **Formato** | 8 tablas relacionales en PostgreSQL |
| **Contenido** | Datos limpiados + entity resolution (IDs unificados) |
| **Validación** | Modelos Pydantic v2 para integridad referencial y rangos |

**Schema centrado en jugador (8 tablas):**

| Tabla | Fuente principal | Qué contiene |
|-------|-----------------|-------------|
| `teams` | API-Football + Understat | Identidad de equipos, IDs cruzados, logo |
| `players` | Ambas fuentes | Identidad unificada, IDs cruzados, metadata de resolución |
| `player_season_stats` | API-Football | Stats base: appearances, minutes, goals, assists, shots, cards |
| `player_season_advanced` | Understat (season) | Métricas avanzadas: xG, xA, npxG, xGChain, xGBuildup |
| `player_shots` | Understat (shots) | Cada tiro: coordenadas x/y, xG, resultado, situación |
| `player_profile` | API-Football | Scouting: height, weight, foot, position, contract |
| `player_injuries` | API-Football | Historial de lesiones: tipo, fechas |
| `player_transfers` | API-Football | Historial de traspasos: equipos, fecha, tipo, fee |

Las tablas `players` y `teams` incluyen campos de entity resolution: `resolution_confidence`, `resolution_method` y `resolved_at`.

### Capa 3 — FEATURES (`data/features/`)

| Aspecto | Detalle |
|---------|---------|
| **Formato** | Parquet columnar |
| **Contenido** | Métricas derivadas por jugador (min. 450 minutos jugados) |
| **Validación** | Modelos Pydantic v2 para rangos y completitud |

**Features calculadas:**

- **Per-90** (desde `player_season_stats`): goals_per_90, assists_per_90, shots_per_90.
- **xG avanzadas** (desde `player_season_advanced`): xg_overperformance, npxg_per_90, xg_chain_share, xg_buildup_per_90.
- **Shot quality** (desde `player_shots`): xg_per_shot, avg_shot_distance, shot_conversion_rate.
- **Scouting** (desde `player_injuries` + `player_transfers`): total_injury_days, injury_count, transfer_count, days_since_last_injury.
- **Percentiles**: percentile_rank por posición y liga para métricas clave.

### Capa 4 — ENRICHED (SQLite / Datasette)

| Aspecto | Detalle |
|---------|---------|
| **Formato** | SQLite servido con Datasette |
| **Contenido** | Vista desnormalizada `player_season_stats_flat` con todas las métricas, IDs cruzados, foto de jugador y logo de equipo |
| **Acceso** | Datasette en `:8001` con UI, API JSON, exportación CSV y queries predefinidas |

Queries predefinidas en `config/datasette/metadata.yml`: Top 10 por xG overperformance, jugadores con más lesiones, jugadores con confidence < 0.8, distribución de tiros por zona.

## Orquestación

Airflow 3.x con TaskFlow API. 5 DAGs independientes ejecutables por separado:

```
ingest_api_football ──┐
ingest_understat ─────┼──▶ transform_clean ──▶ build_features ──▶ export_enriched
```

- **ingest_api_football** (5 tasks): carga config YAML, teams, players per-team, injuries, transfers, standings.
- **ingest_understat** (2 tasks): scraping de shots + season stats (2 outputs Parquet).
- **transform_clean** (1 task): team resolution → player resolution → inserta en PostgreSQL.
- **build_features** (1 task): lee CLEAN, calcula métricas, escribe Parquet en `data/features/`.
- **export_enriched** (1 task): exporta a SQLite, construye flat view + shots table.

## Config de Ingesta

`config/ingestion.yaml` controla el scope completo. Cambiar de liga o temporada es editar el YAML:

```yaml
sources:
  api_football:
    league_id: 140        # La Liga
    season: 2024
    endpoints: [players_stats, injuries, transfers]
    cache_dir: data/cache/api_football
    cache_ttl_hours: 168  # 7 días
    rate_limit:
      max_calls_per_day: 100
      delay_between_calls: 7.0
  understat:
    league: "ESP-La Liga"   # soccerdata format
    season: "2024/2025"
```

Cargado por un modelo Pydantic Settings en `src/pipeline/config.py`.

## Stack Tecnológico

| Componente | Tecnología |
|------------|-----------|
| Contenerización | Podman ([ADR-001](adr/001-podman-over-docker.md)) |
| Orquestación | Apache Airflow 3.1.8 (TaskFlow API) |
| Base de datos | PostgreSQL 18 (capa CLEAN) |
| Exploración | Datasette + datasette-vega + datasette-render-image-tags (capa ENRICHED) |
| Validación | Pydantic v2 |
| Formato intermedio | Apache Parquet |
| HTTP client | httpx (API-Football, con cache + rate limit) |
| Scraping | soccerdata (Understat) |
| Fuzzy matching | rapidfuzz (entity resolution) |
| Lenguaje | Python 3.13+ |
| Gestión de paquetes | uv + pyproject.toml |

## Estructura de Directorios

```
football-data-pipeline/
├── .agents/
│   └── skills/                    # Agent skills (TDD, docs, review, etc.)
├── config/
│   ├── ingestion.yaml             # Scope de ingesta (liga, temporada, endpoints)
│   ├── datasette/                 # metadata.yml y assets estáticos
│   └── sql/
│       ├── init.sql               # DDL PostgreSQL (8 tablas)
│       └── postgres-init.sh       # Script de inicialización del contenedor
├── dags/
│   ├── dag_ingest_api_football.py
│   ├── dag_ingest_understat.py
│   ├── dag_transform_clean.py
│   ├── dag_build_features.py
│   └── dag_export_enriched.py
├── docs/
│   ├── adr/                       # ADR-001 a ADR-006
│   └── entity-resolution-spec.md  # Spec detallado de entity resolution
├── src/pipeline/
│   ├── config.py                  # Pydantic Settings + singleton
│   ├── db.py                      # Conexión PostgreSQL
│   ├── models/                    # raw.py, clean.py, features.py (Pydantic v2)
│   ├── loaders/                   # api_football_loader + understat_loader
│   ├── entity_resolution.py       # 4 pasadas + informe CSV
│   ├── transform_clean.py         # RAW → CLEAN orchestration
│   ├── feature_engineering.py     # Métricas derivadas
│   └── observability.py           # Skeleton — logging estructurado
├── tests/
│   ├── fixtures/                  # Payloads reales para tests
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_api_football_loader.py
│   ├── test_understat_loader.py
│   ├── test_models_raw.py
│   ├── test_entity_resolution.py
│   ├── test_transform_clean.py
│   ├── test_feature_engineering.py
│   └── test_export_enriched.py
├── Containerfile                  # Imagen OCI (Podman + Docker)
├── compose.yml                    # 5 servicios: postgres, webserver, scheduler, dag-processor, datasette
├── pyproject.toml
├── .env.example
├── AGENTS.md
├── CLAUDE.md
└── .gitignore                     # data/ y cache/ excluidos
```
