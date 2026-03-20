# Arquitectura del Pipeline — Football Data Pipeline

## Visión General

Pipeline de datos multi-fuente que ingesta, limpia, fusiona y sirve datos de fútbol provenientes de 3 fuentes heterogéneas: **API-Football**, **Understat** y **FBref**. Resuelve entity resolution entre fuentes con sistemas de IDs incompatibles. Airflow orquesta todo el flujo; Pydantic v2 valida los datos en cada transición entre capas.

**Scope actual:** La Liga 2024/25 (configurable vía `config/ingestion.yaml`).

Para el contexto de por qué estas fuentes y no otras, ver [ADR-002](adr/002-data-source-selection.md). Para por qué no se incluye event data, ver [ADR-003](adr/003-event-data-out-of-scope.md).

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ API-Football │   │  Understat   │   │    FBref     │
│  (REST API)  │   │  (scraping)  │   │  (scraping)  │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
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
| **FBref** | Stats base agregadas: goles, minutos, tarjetas, tiros, tackles | Temporada/jugador | `soccerdata` (scraping) |

Cada fuente usa su propio sistema de IDs. El pipeline los reconcilia en la capa CLEAN mediante entity resolution.

## Detalle de las Capas

### Cache — API-Football (`data/cache/`)

API-Football tiene un límite de 100 calls/día en el free tier. Para no agotar la cuota en desarrollo y hacer el pipeline reproducible sin calls adicionales, toda respuesta de la API se persiste como JSON crudo antes de transformar.

```
data/cache/api_football/
├── players/
│   ├── league_140_season_2024_page_1.json
│   └── league_140_season_2024_page_2.json
├── injuries/
│   └── league_140_season_2024_page_1.json
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
├── understat/
│   ├── shots.parquet
│   └── player_season.parquet    # xG, xA, npxG, xGChain, xGBuildup
└── fbref/
    └── player_season_stats.parquet
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
| `teams` | API-Football + FBref | Identidad de equipos, IDs cruzados, logo |
| `players` | Las 3 fuentes | Identidad unificada, IDs cruzados, metadata de resolución |
| `player_season_stats` | FBref + API-Football | Stats base: appearances, minutes, goals, assists, shots, cards |
| `player_season_advanced` | Understat (season) | Métricas avanzadas: xG, xA, npxG, xGChain, xGBuildup |
| `player_shots` | Understat (shots) | Cada tiro: coordenadas x/y, xG, resultado, situación |
| `player_profile` | API-Football | Scouting: height, weight, foot, position, contract |
| `player_injuries` | API-Football | Historial de lesiones: tipo, fechas |
| `player_transfers` | API-Football | Historial de traspasos: equipos, fecha, tipo, fee |

Las tablas `players` y `teams` incluyen campos de entity resolution: `resolution_confidence`, `resolution_method` y `resolved_at`.

Tablas pospuestas a sprints futuros: `competitions`, `seasons`, `matches`, `lineups`, `player_match_stats`.

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

Queries predefinidas en `metadata.yml`: Top 10 por xG overperformance, jugadores con más lesiones, jugadores con confidence < 0.8, distribución de tiros por zona.

## Orquestación

Airflow con TaskFlow API. 6 DAGs independientes ejecutables por separado:

```
ingest_api_football ──┐
ingest_understat ─────┼──▶ transform_clean ──▶ build_features ──▶ enrich
ingest_fbref ─────────┘
```

- **ingest_api_football**: carga config YAML, llama al loader con rate limiting y cache.
- **ingest_understat**: scraping de shots + season stats (2 outputs Parquet).
- **ingest_fbref**: scraping de stats básicas.
- **transform_clean**: team resolution → player resolution → inserta en PostgreSQL.
- **build_features**: lee CLEAN, calcula métricas, escribe Parquet en `data/features/`.
- **enrich**: exporta a SQLite, refresca Datasette.

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
      delay_between_calls: 1.0
  understat:
    league: "La Liga"
    season: "2024/2025"
  fbref:
    league: "La Liga"
    season: "2024-2025"
```

Cargado por un modelo Pydantic Settings en `src/pipeline/config.py`.

## Stack Tecnológico

| Componente | Tecnología |
|------------|-----------|
| Contenerización | Podman ([ADR-001](adr/001-podman-over-docker.md)) |
| Orquestación | Apache Airflow 2.10 (TaskFlow API) |
| Base de datos | PostgreSQL 16 (capa CLEAN) |
| Exploración | Datasette + datasette-vega (capa ENRICHED) |
| Validación | Pydantic v2 |
| Formato intermedio | Apache Parquet |
| HTTP client | httpx (API-Football, con cache + rate limit) |
| Scraping | soccerdata (Understat + FBref) |
| Fuzzy matching | rapidfuzz (entity resolution) |
| Lenguaje | Python 3.11+ |
| Gestión de paquetes | uv + pyproject.toml |

## Estructura de Directorios

```
football-data-pipeline/
├── config/
│   └── ingestion.yaml                 # Scope de ingesta (liga, temporada, endpoints)
├── dags/
│   ├── ingest_api_football.py
│   ├── ingest_understat.py
│   ├── ingest_fbref.py
│   ├── transform_clean.py
│   ├── build_features.py
│   └── enrich.py
├── src/
│   └── pipeline/
│       ├── config.py                  # Pydantic Settings
│       ├── models/                    # Modelos Pydantic (raw, clean, features)
│       ├── loaders/                   # Módulos de ingesta por fuente
│       ├── entity_resolution.py       # Fuzzy matching entre fuentes
│       ├── feature_engineering.py     # Construcción de métricas derivadas
│       └── observability.py           # Logging estructurado
├── tests/
│   ├── fixtures/                      # Payloads reales para tests
│   ├── test_models.py
│   ├── test_entity_resolution.py
│   └── test_loaders.py
├── data/
│   ├── cache/                         # Cache JSON de API-Football
│   ├── raw/                           # Capa RAW (Parquet)
│   │   ├── api_football/
│   │   ├── understat/
│   │   └── fbref/
│   └── features/                      # Capa FEATURES (Parquet)
├── sql/
│   └── init.sql                       # DDL PostgreSQL (8 tablas)
├── docs/
│   ├── adr/
│   │   ├── 001-podman-over-docker.md
│   │   ├── 002-data-source-selection.md
│   │   └── 003-event-data-out-of-scope.md
│   └── architecture.md                # Este documento
├── docker-compose.yml                 # Stack: Airflow + PostgreSQL + Datasette
├── Containerfile                      # Imagen base (OCI-compatible)
├── pyproject.toml
├── .env.example
├── .gitignore                         # data/ y cache/ excluidos
└── README.md
```
