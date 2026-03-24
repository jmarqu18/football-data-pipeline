"""DAG: Exportación a SQLite y refresco de Datasette (FEATURES → ENRICHED).

Reads player_season_features.parquet and CLEAN PostgreSQL tables, assembles a
fully denormalized SQLite database at data/enriched/enriched.db for Datasette.

Depends on: dag_build_features must have run first (FEATURES Parquet must exist).
"""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.sdk import Asset, dag, task

from pipeline.config import get_config
from pipeline.export_enriched import run_export_enriched

logger = logging.getLogger(__name__)

_FEATURES_DIR = Path(__file__).parents[1] / "data" / "features"
_ENRICHED_DIR = Path(__file__).parents[1] / "data" / "enriched"


@dag(
    dag_id="export_enriched",
    schedule=[Asset("player_season_features")],
    catchup=False,
    tags=["enriched"],
    doc_md=__doc__,
)
def export_enriched_dag() -> None:
    """Export FEATURES + CLEAN data to SQLite for Datasette exploration."""

    @task
    def export_enriched_task() -> dict:
        """Assemble flat view and shots table, write to enriched.db."""
        config = get_config()
        season_year = config.sources.api_football.season
        season = f"{season_year}/{season_year + 1}"

        return run_export_enriched(
            output_path=_ENRICHED_DIR / "enriched.db",
            features_path=_FEATURES_DIR / "player_season_features.parquet",
            season=season,
        )

    export_enriched_task()


export_enriched_dag()
