"""DAG: Feature engineering (CLEAN → FEATURES).

Reads from CLEAN PostgreSQL tables and computes derived metrics (per-90, xG,
shot quality, scouting, percentiles). Writes a single Parquet file to
data/features/player_season_features.parquet.

Depends on: dag_transform_clean must have run first (CLEAN tables must be populated).
"""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.sdk import dag, task

from pipeline.config import get_config
from pipeline.feature_engineering import run_feature_engineering

logger = logging.getLogger(__name__)

_FEATURES_DIR = Path(__file__).parents[1] / "data" / "features"
_RAW_DIR = Path(__file__).parents[1] / "data" / "raw"


@dag(
    dag_id="build_features",
    schedule=None,
    catchup=False,
    tags=["features"],
    doc_md=__doc__,
)
def build_features_dag() -> None:
    """Compute derived football metrics and write to data/features/ Parquet."""

    @task
    def build_features_task() -> dict:
        """Compute all features from CLEAN PostgreSQL tables."""
        config = get_config()
        season_year = config.sources.api_football.season
        season = f"{season_year - 1}/{season_year}"
        output_path = _FEATURES_DIR / "player_season_features.parquet"

        return run_feature_engineering(
            output_path=output_path,
            season=season,
            raw_dir=_RAW_DIR,
        )

    build_features_task()


build_features_dag()
