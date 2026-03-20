# CLAUDE.md — Football Data Pipeline

## Qué es este proyecto

Pipeline de datos multi-fuente para football analytics. Integra 3 fuentes con IDs incompatibles (API-Football, Understat, FBref), resuelve entity resolution entre ellas, y sirve datos enriquecidos via Datasette. Orquestado con Airflow, containerizado con Podman.

**Scope actual:** La Liga 2024/25, una sola temporada. El config permite expandir.

## Stack técnico

- **Python 3.11+** con `uv` como gestor de paquetes.
- **Pydantic v2** para validación en cada transición de capa.
- **Airflow 2.10** con TaskFlow API para orquestación.
- **PostgreSQL 16** para capa CLEAN (modelo relacional).
- **SQLite + Datasette** para capa ENRICHED (exploración).
- **httpx** como cliente HTTP para API-Football.
- **soccerdata** para scraping de Understat y FBref.
- **rapidfuzz** para fuzzy string matching en entity resolution.
- **Podman + podman-compose** para contenedores (Containerfiles OCI-compatible).
- **Parquet** como formato de almacenamiento para capas RAW y FEATURES.

## Arquitectura de 4 capas

```
API-Football ──┐
Understat ─────┼──→ RAW (Parquet) → CLEAN (PostgreSQL) → FEATURES (Parquet) → ENRICHED (SQLite/Datasette)
FBref ─────────┘         │                  │                    │                     │
                    Pydantic v2         Entity Res.          Per-90, xG,          Vista plana
                    validation          + normalize          percentiles          + imágenes
```

- **RAW** (`data/raw/`): datos tal cual llegan de cada fuente, en Parquet. API-Football también cachea JSON crudo en `data/cache/api_football/`.
- **CLEAN** (PostgreSQL): 8 tablas centradas en jugador. Entity resolution ocurre aquí. Tablas `players` y `teams` con IDs de las 3 fuentes + `resolution_confidence`. Tablas separadas para stats básicas (FBref), avanzadas (Understat season), shots, profile, injuries, transfers.
- **FEATURES** (`data/features/`): métricas derivadas en Parquet (per-90, xG overperformance, xGChain/xGBuildup per-90, percentiles, injury/transfer features).
- **ENRICHED** (SQLite): vista desnormalizada `player_season_stats_flat` servida por Datasette con imágenes.

## Fuentes de datos

### API-Football
- **API REST** con free tier: 100 calls/día.
- Endpoints: `/players`, `/injuries`, `/transfers`, `/teams`.
- **Cache obligatorio:** cada respuesta se guarda como JSON en `data/cache/api_football/{endpoint}/{params_hash}.json` antes de transformar.
- Config en `config/ingestion.yaml` controla league_id, season, endpoints activos.
- Estimación La Liga: ~60 calls (cabe en 1 día).

### Understat
- Scraping via `soccerdata`. **Dos ingestas separadas:**
  - **Shots** (shot-level): coordenadas, xG, resultado, situación de juego.
  - **Season stats** (player-season): xG, xA, npxG, xGChain, xGBuildup, shots, key_passes. Estas métricas avanzadas NO se derivan de los shots — son cálculos propios de Understat a nivel temporada.
- Sin límite explícito pero respetar delays.

### FBref
- Scraping via `soccerdata`. Stats agregadas básicas (goles, minutos, tarjetas).
- Perdió métricas avanzadas Opta en enero 2026.

### Cumplimiento TOS (para README)
- FBref/Understat: uso personal/educativo, sin redistribución masiva.
- API-Football: consumo vía API key del usuario, dentro de las cuotas de su plan.

## Estructura del repo

```
football-data-pipeline/
├── config/
│   └── ingestion.yaml           # Scope de ingesta (liga, temporada, endpoints)
├── docs/
│   ├── adr/
│   │   ├── 001-podman-over-docker.md
│   │   ├── 002-data-source-selection.md
│   │   └── 003-event-data-out-of-scope.md
│   └── architecture.md
├── dags/
│   ├── ingest_api_football.py
│   ├── ingest_understat.py
│   ├── ingest_fbref.py
│   ├── transform_clean.py
│   ├── build_features.py
│   └── enrich.py
├── sql/
│   └── init.sql                 # DDL PostgreSQL
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── config.py            # Pydantic Settings model
│       ├── models/
│       │   ├── raw.py           # RawAPIFootballPlayer, RawUnderstatShot, RawUnderstatPlayerSeason, RawFBrefPlayerSeason
│       │   ├── clean.py
│       │   └── features.py
│       ├── loaders/
│       │   ├── api_football_loader.py  # httpx + cache + rate limit
│       │   ├── understat_loader.py
│       │   └── fbref_loader.py
│       ├── entity_resolution.py
│       ├── feature_engineering.py
│       └── observability.py
├── tests/
│   ├── fixtures/                # JSON payloads reales para tests
│   ├── test_models.py
│   ├── test_entity_resolution.py
│   └── test_loaders.py
├── data/
│   ├── cache/                   # Cache JSON de API-Football (git-ignored)
│   ├── raw/                     # Parquet crudo (git-ignored)
│   │   ├── api_football/        # players, injuries, transfers
│   │   ├── understat/           # shots.parquet + player_season.parquet
│   │   └── fbref/               # player_season_stats.parquet
│   └── features/                # Parquet features (git-ignored)
├── Containerfile
├── docker-compose.yml
├── pyproject.toml
├── README.md
├── .env.example
└── .gitignore
```

## Convenciones de código

### Idioma
- **Código en inglés:** variables, funciones, clases, docstrings, comentarios, commits.
- **Documentación:** README y docs en español.

### Estilo Python
- Tipado estricto. Usar Pydantic v2 para modelos de datos, no dataclasses.
- `from __future__ import annotations` en todos los módulos.
- Docstrings en inglés, formato Google style.
- Imports ordenados: stdlib → third-party → local (`src.pipeline.*`).
- Sin `print()`. Usar `logging` con el patrón de `observability.py`.

### Logging y observabilidad
- Cada loader loguea: registros totales, válidos, rechazados, tiempo de ejecución.
- Entity resolution loguea: resueltos, fallidos, confidence media, método usado.
- API-Football loader loguea: calls realizadas, calls desde cache, calls restantes.
- Nivel INFO para resúmenes, DEBUG para detalle, WARNING para datos rechazados.

### Gestión de errores
- Los loaders nunca crashean el DAG por un registro inválido. Loguean WARNING y continúan.
- Fallos de red en API-Football: retry con backoff exponencial (3 intentos). Si falla, loguea ERROR con endpoint y params.
- Pydantic `ValidationError` se captura, se loguea el registro problemático, y se cuenta como "rechazado".

### Tests
- Tests unitarios con pytest.
- Fixtures JSON reales en `tests/fixtures/` (payloads reales de cada API).
- Entity resolution: test con 20 jugadores conocidos, assert ≥ 18 correctos.
- Loaders: test con fixtures locales, sin llamadas reales a APIs.

### Commits
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`.
- Un commit por bloque de trabajo como mínimo.

## Decisiones técnicas clave (ADRs)

### ADR-001: Podman sobre Docker
Rootless by default, daemonless, OCI-compliant. Containerfiles estándar. README documenta ambos comandos. El fichero se llama `docker-compose.yml` (compatibilidad).

### ADR-002: Selección de fuentes de datos
FBref + Understat + API-Football como las 3 fuentes del pipeline. Seleccionadas por ser gratuitas, éticamente utilizables y con TOS compatibles con un proyecto público. Descartadas Transfermarkt, FotMob y WhoScored por prohibir scraping o tener datos bajo licencia.

### ADR-003: Event data fuera de scope
Event data (pases, presiones, carries) es privativo en todos los proveedores. StatsBomb Open Data tiene cobertura limitada a temporadas antiguas (2015/16). El pipeline se centra en stats agregadas y shot-level con datos actuales. StatsBomb queda como candidato para sprints futuros (ML con event data histórico).

## Entity resolution: Enfoque

### Problema
- API-Football: `player_id: 1100`, nombre "Pedro González López", birth_date "2002-11-25".
- Understat: `player_id: 8872`, nombre "Pedri".
- FBref: slug `pedri/fbcef978`, nombre "Pedri".

### Solución: 3 pasadas
1. **Exact match:** nombre normalizado (unidecode + lowercase + strip diacríticos) + birth_date exacta → confidence 1.0.
2. **Fuzzy match:** rapidfuzz `token_sort_ratio` ≥ 85 + mismo año nacimiento → confidence 0.85.
3. **Contextual:** mismo equipo (resuelto previamente) + temporada + fuzzy ≥ 75 → confidence 0.70.

**Team resolution se ejecuta primero** (nombres de equipos más estables) y alimenta la pasada 3.

## PostgreSQL CLEAN schema — 8 tablas

Schema centrado en jugador, sin sobre-normalización. Sin tablas de partidos, alineaciones ni stats match-level (Sprint 3+).

| # | Tabla | Fuente principal | Descripción |
|---|-------|-----------------|-------------|
| 1 | `teams` | API-Football + FBref | Identidad de equipos con IDs de las 3 fuentes + metadata de resolución |
| 2 | `players` | Las 3 fuentes | Identidad base: IDs cruzados, canonical_name, birth_date, resolution_confidence/method/timestamp |
| 3 | `player_season_stats` | FBref + API-Football | Stats agregadas por temporada: appearances, minutes, goals, assists, shots, tackles, cards |
| 4 | `player_season_advanced` | Understat (season-level) | Métricas avanzadas: xG, xA, npxG, xGChain, xGBuildup, shots, key_passes |
| 5 | `player_shots` | Understat (shot-level) | Cada tiro: coordenadas x/y, xG, resultado, body_part, situation |
| 6 | `player_profile` | API-Football | Scouting estático: height, weight, foot, position, current_team, contract |
| 7 | `player_injuries` | API-Football | Historial: tipo lesión, fechas, equipo |
| 8 | `player_transfers` | API-Football | Historial: from/to team, date, type (loan/permanent/free), fee |

**Tablas pospuestas a Sprint 3+:** `competitions`, `seasons`, `matches`, `lineups`, `player_match_stats`.

## API-Football: Rate limiting y cache

### Estrategia de cache
```
data/cache/api_football/
├── players/
│   ├── league_140_season_2024_page_1.json
│   └── league_140_season_2024_page_2.json
├── injuries/
│   └── league_140_season_2024_page_1.json
└── transfers/
    ├── team_529.json    # Barcelona
    └── team_541.json    # Real Madrid
```

- Cada call a la API primero busca en cache por path + params hash.
- Si el cache existe y `cache_ttl` no ha expirado (default 24h para desarrollo, 7 días para producción), usa cache.
- El loader expone `force_refresh: bool` para ignorar cache cuando sea necesario.

### Estimación de calls La Liga 2024/25
| Endpoint | Calls estimadas | Notas |
|----------|----------------|-------|
| `/players?league=140&season=2024` | ~30 | 20 equipos × ~1.5 páginas |
| `/injuries?league=140&season=2024` | ~10 | Paginado |
| `/transfers?team={id}` | ~20 | 1 por equipo |
| **Total** | **~60** | Margen de 40 calls para debugging |

## Config de ingesta

`config/ingestion.yaml` controla el scope completo. El objetivo es que cambiar de La Liga a Premier League sea editar 3 líneas en el YAML:

```yaml
sources:
  api_football:
    league_id: 140
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

## Qué NO hacer

- No añadir más fuentes en este sprint. Event data (StatsBomb) queda para sprints futuros.
- No entrenar modelos ML. Solo calcular features.
- No montar CI/CD todavía.
- No sobreingenierar Airflow (sin Kubernetes executor, sin custom operators).
- No cubrir múltiples ligas. Una liga, una temporada.
- No hacer calls a API-Football sin verificar cache primero.
- No commitear datos ni cache al repo (`data/` en .gitignore).
