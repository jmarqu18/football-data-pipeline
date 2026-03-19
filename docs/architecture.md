# Arquitectura del Pipeline — Football Data Pipeline

## Visión General

Pipeline de datos multi-fuente que ingesta, limpia, fusiona y sirve datos de fútbol provenientes de 3 fuentes heterogéneas: **StatsBomb**, **Understat** y **FBref**. Airflow orquesta todo el flujo; Pydantic valida los datos en cada transición entre capas.

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  StatsBomb   │   │  Understat   │   │  FBref       │
│  (API)       │   │  (scraping)  │   │  (scraping)  │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│              CAPA 1 — RAW                           │
│              data/raw/ (Parquet crudo)               │
│              Pydantic: RawEventModel, RawShotModel…  │
└──────────────────────┬──────────────────────────────┘
                       │ Validación + Entity Resolution
                       ▼
┌─────────────────────────────────────────────────────┐
│              CAPA 2 — CLEAN                         │
│              PostgreSQL                              │
│              Pydantic: CleanPlayerModel, CleanMatch…  │
│              Entity Resolution (fuzzy matching IDs)  │
└──────────────────────┬──────────────────────────────┘
                       │ Feature engineering
                       ▼
┌─────────────────────────────────────────────────────┐
│              CAPA 3 — FEATURES                      │
│              data/features/ (Parquet columnar)        │
│              Pydantic: PlayerFeatureModel…            │
└──────────────────────┬──────────────────────────────┘
                       │ Aplanamiento + exportación
                       ▼
┌─────────────────────────────────────────────────────┐
│              CAPA 4 — ENRICHED                      │
│              SQLite → Datasette (vista plana)        │
│              Pydantic: EnrichedPlayerModel…           │
└─────────────────────────────────────────────────────┘
```

## Detalle de las Capas

### Capa 1 — RAW (`data/raw/`)

| Aspecto       | Detalle                                                             |
|---------------|---------------------------------------------------------------------|
| **Formato**   | Parquet crudo, un directorio por fuente (`statsbomb/`, `understat/`, `fbref/`) |
| **Contenido** | Datos tal cual llegan de la fuente, sin transformación               |
| **Validación**| Modelos Pydantic que verifican esquema mínimo y tipos básicos        |
| **Partición** | Por competición y temporada (`euro2020/`, `laliga_2023/`, …)         |

### Capa 2 — CLEAN (PostgreSQL)

| Aspecto       | Detalle                                                             |
|---------------|---------------------------------------------------------------------|
| **Formato**   | Tablas relacionales en PostgreSQL                                    |
| **Contenido** | Datos limpiados + entity resolution (IDs unificados entre fuentes)   |
| **Validación**| Modelos Pydantic que verifican integridad referencial y rangos lógicos|
| **Entity Res.**| Fuzzy matching por nombre + metadatos (equipo, posición, fecha nacimiento) para resolver IDs cruzados entre StatsBomb, Understat y FBref |

### Capa 3 — FEATURES (`data/features/`)

| Aspecto       | Detalle                                                             |
|---------------|---------------------------------------------------------------------|
| **Formato**   | Parquet columnar optimizado para análisis                            |
| **Contenido** | Métricas derivadas y features por jugador/partido                    |
| **Validación**| Modelos Pydantic que verifican rangos y completitud de features      |
| **Ejemplos**  | xG acumulado (Understat), acciones progresivas (StatsBomb), goles/asistencias (FBref) |

### Capa 4 — ENRICHED (SQLite / Datasette)

| Aspecto       | Detalle                                                             |
|---------------|---------------------------------------------------------------------|
| **Formato**   | SQLite exportado desde features → servido con Datasette              |
| **Contenido** | Vista plana desnormalizada, lista para exploración interactiva       |
| **Validación**| Modelos Pydantic finales antes de inserción en SQLite                |
| **Acceso**    | Datasette expone la BD en el navegador con UI, API JSON y exportación CSV |

## Orquestación

**Airflow** orquesta el pipeline completo mediante un DAG que ejecuta las tareas en orden:

```
ingest_statsbomb → ingest_understat → ingest_fbref
        │                │                │
        └───────┬────────┘                │
                ▼                         │
        validate_raw ◄────────────────────┘
                │
                ▼
        entity_resolution (CLEAN)
                │
                ▼
        build_features (FEATURES)
                │
                ▼
        export_enriched (ENRICHED → SQLite)
                │
                ▼
        refresh_datasette
```

## Stack Tecnológico

| Componente          | Tecnología                          |
|---------------------|-------------------------------------|
| Contenerización     | Podman (ver [ADR-001](adr/001-podman-over-docker.md)) |
| Orquestación        | Apache Airflow                      |
| Base de datos       | PostgreSQL (capa CLEAN)             |
| Exploración         | Datasette (capa ENRICHED)           |
| Validación          | Pydantic v2                         |
| Formato intermedio  | Apache Parquet                      |
| Ingesta StatsBomb   | `statsbombpy`                       |
| Ingesta Understat   | `soccerdata`                        |
| Ingesta FBref       | `soccerdata`                        |
| Lenguaje            | Python 3.13+                        |

## Estructura de Directorios

```
football-data-pipeline/
├── dags/                              # DAGs de Airflow (6)
│   ├── dag_ingest_statsbomb.py        #   Ingesta StatsBomb
│   ├── dag_ingest_understat.py        #   Ingesta Understat
│   ├── dag_ingest_fbref.py            #   Ingesta FBref
│   ├── dag_entity_resolution.py       #   RAW → CLEAN
│   ├── dag_build_features.py          #   CLEAN → FEATURES
│   └── dag_export_enriched.py         #   FEATURES → ENRICHED
├── src/
│   └── pipeline/
│       ├── models/                    # Modelos Pydantic (por capa)
│       ├── loaders/                   # Módulos de ingesta por fuente
│       ├── entity_resolution.py       # Fuzzy matching entre fuentes
│       ├── feature_engineering.py     # Construcción de métricas derivadas
│       └── observability.py           # Logging estructurado y métricas
├── tests/                             # Tests (pytest)
├── data/
│   ├── raw/                           # Capa RAW (Parquet crudo)
│   └── features/                      # Capa FEATURES (Parquet columnar)
├── sql/
│   └── init.sql                       # Inicialización PostgreSQL
├── docs/
│   ├── adr/                           # Architecture Decision Records
│   └── architecture.md                # Este documento
├── compose.yml                        # Stack: Airflow + PostgreSQL + Datasette
├── Containerfile                      # Imagen base del pipeline (OCI)
├── pyproject.toml                     # Dependencias y configuración del proyecto
└── README.md
```
