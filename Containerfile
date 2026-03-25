# Containerfile — Imagen base del pipeline (OCI-compatible)
# Compatible con: podman build / docker build

# ── Base ─────────────────────────────────────────────────────────────────────
FROM apache/airflow:3.1.8-python3.13

LABEL maintainer="Juanje Marquez <juanje.mn@gmail.com>" \
      description="Football data pipeline: API-Football + Understat, orchestrated by Airflow"

# ── Python dependencies ───────────────────────────────────────────────────────
# apache-airflow is already in the base image — all other project deps go here.
# No apt-get layer needed: psycopg2-binary, pyarrow, and lxml (via soccerdata)
# all ship pre-built binary wheels.
RUN pip install --no-cache-dir \
    "pydantic>=2.12.5" \
    "pyyaml>=6.0.3" \
    "httpx>=0.27.0" \
    "soccerdata>=1.8.8" \
    "rapidfuzz>=3.0.0" \
    "unidecode>=1.3.0" \
    "pandas>=2.1.0,<3.0.0" \
    "pyarrow>=23.0.1" \
    "sqlalchemy>=2.0.48" \
    "psycopg2-binary>=2.9.11" \
    "datasette>=0.65.2,<1.0" \
    "datasette-vega>=0.6"

# ── Pre-warm tls_requests native library ─────────────────────────────────────
# soccerdata depends on tls_requests, which lazily downloads a platform-specific
# .so from GitHub on first HTTP request (not on import).  Making a real request
# here bakes the binary into the image layer so no download occurs at DAG runtime.
# || true: build succeeds even if the test URL is unreachable on the build host.
RUN python -c 'import tls_requests; tls_requests.get("https://cloudflare.com", timeout=15)' 2>/dev/null || true

# ── Application code ──────────────────────────────────────────────────────────
COPY --chown=airflow:root src/       /opt/airflow/src/
COPY --chown=airflow:root dags/      /opt/airflow/dags/
COPY --chown=airflow:root config/    /opt/airflow/config/
COPY --chown=airflow:root datasette/ /opt/airflow/datasette/
COPY --chown=airflow:root sql/       /opt/airflow/sql/

# ── Data directories ──────────────────────────────────────────────────────────
# DAGs resolve data paths via Path(__file__).parents[1] / "data" / ...
# (dags at /opt/airflow/dags/ → parents[1] = /opt/airflow)
RUN mkdir -p \
    /opt/airflow/data/raw/api_football \
    /opt/airflow/data/raw/understat \
    /opt/airflow/data/cache/api_football \
    /opt/airflow/data/features \
    /opt/airflow/data/enriched

# ── Environment ───────────────────────────────────────────────────────────────
# PYTHONPATH makes `pipeline` importable (src/pipeline/ package).
# PIPELINE_CONFIG_PATH is required by config.py (not optional fallback).
# API_FOOTBALL_KEY and DATABASE_URL must be injected at runtime via compose.yml.
ENV PYTHONPATH="/opt/airflow/src" \
    PIPELINE_CONFIG_PATH="/opt/airflow/config/ingestion.yaml"
