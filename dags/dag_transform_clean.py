"""Airflow DAG for transforming RAW Parquet data into CLEAN PostgreSQL tables.

Reads all RAW Parquet files (API-Football + Understat), runs entity resolution
(teams first, then players), and inserts the resolved data into the 8 CLEAN
tables.  Idempotent: truncates all tables before re-inserting.

Depends on: ingest_api_football, ingest_understat (both must have run at least once).
"""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

_RAW_DIR = Path(__file__).parents[1] / "data" / "raw"


@dag(
    dag_id="transform_clean",
    schedule=None,
    catchup=False,
    tags=["transform", "clean", "entity_resolution"],
    doc_md=__doc__,
)
def transform_clean() -> None:
    """Transform RAW layer into CLEAN PostgreSQL tables with entity resolution."""

    @task
    def transform_raw_to_clean() -> dict:
        """Run the full RAW → CLEAN pipeline: read Parquet, resolve, insert."""
        from pipeline.transform_clean import run_transform_clean

        counts = run_transform_clean(raw_dir=_RAW_DIR)
        logger.info("Transform complete. Row counts: %s", counts)
        return counts

    transform_raw_to_clean()


transform_clean()
