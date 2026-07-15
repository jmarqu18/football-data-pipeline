# Changelog

## [0.1.0] — 2026-07-07

### Added
- Pipeline completo de 4 capas: RAW (Parquet) → CLEAN (PostgreSQL) → FEATURES (Parquet) → ENRICHED (SQLite/Datasette).
- Ingesta de API-Football con estrategia per-team para bypass de limitación de paginación del free tier (página 3).
- Ingesta de Understat vía soccerdata: shot-level data + player-season advanced metrics (xG, xA, npxG, xGChain, xGBuildup).
- Entity resolution de 4 pasadas (exact → fuzzy → contextual → statistical) con position-aware conflict resolution.
- 5 DAGs de Airflow 3.x con TaskFlow API.
- Stack containerizado con Podman/Docker: PostgreSQL 18, Airflow 3.1.8, Datasette.
- Feature engineering: per-90 metrics, xG overperformance, percentiles, scouting features.
- Export enriched: flat view desnormalizada + shots table + Datasette con `datasette-render-image-tags`.
- Sistema de cache JSON para API-Football con TTL configurable (7 días).
- 6 ADRs documentando todas las decisiones arquitectónicas.

### Known issues
- _(sin issues conocidos)_ — `observability.py` completado (logging centralizado) y las
  referencias obsoletas a FBref ya fueron eliminadas de la documentación.
