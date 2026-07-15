# Changelog

## [Unreleased]

### Security
- **Fail-closed de secretos en `compose.yml`**: `FERNET_KEY`, `AIRFLOW__API__SECRET_KEY` y
  `AIRFLOW__API_AUTH__JWT_SECRET` usan `${VAR:?...}` en lugar de los defaults adivinables
  `dev-jwt-secret-change-in-prod` / `dev-secret-key-change-in-prod`. `make up` ahora aborta con
  un mensaje claro si `.env` no define los secretos, en vez de correr inseguro en silencio.
- **`make rotate-secrets` (nuevo)**: genera y hace upsert de los 3 secretos en `.env` de forma
  idempotente (preserva `API_FOOTBALL_KEY`) vía `scripts/generate_airflow_secrets.py`.
  `make init` también usa el script. Ambos aceptan `PYTHON='uv run python'` para entornos con uv.
- Resuelve el `InsecureKeyLengthWarning` de Airflow 3.x (JWT secret 29 → 86 bytes). Al rotar el
  JWT secret, las sesiones de la UI se invalidan y requieren re-login (admin/admin).

### Fixed
- `make init` / `make rotate-secrets` ya no fallan en git-bash (Windows): la generación de
  secretos se movió de `python -c "..."` (que git-bash malinterpreta por el `;` y los
  paréntesis dentro de `$(...)`) a `scripts/generate_airflow_secrets.py`.

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
