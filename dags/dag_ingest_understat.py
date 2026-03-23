"""Airflow DAG for ingesting Understat data into the RAW layer.

Tasks (independent — no shared state between shots and season stats):
    1. ingest_shots        — shot-level events → shots.parquet.
    2. ingest_player_season — player season stats → player_season.parquet.
"""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.sdk import dag, task

from pipeline.config import get_config
from pipeline.loaders.understat_loader import UnderstatLoader

logger = logging.getLogger(__name__)

_RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "understat"


@dag(
    dag_id="ingest_understat",
    schedule=None,
    catchup=False,
    tags=["ingestion", "understat"],
    doc_md=__doc__,
)
def ingest_understat() -> None:
    """Ingest Understat shot events and player season stats into RAW Parquet files."""

    @task
    def ingest_shots_task() -> None:
        """Scrape shot-level events for the configured league and season."""
        cfg = get_config().sources.understat
        loader = UnderstatLoader(config=cfg)
        shots = loader.ingest_shots()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        UnderstatLoader.save_parquet(shots, _RAW_DIR / "shots.parquet")
        logger.info("Shots ingested: %d records", len(shots))

    @task
    def ingest_player_season_task() -> None:
        """Scrape player season stats (xG, xA, npxG, xGChain, xGBuildup)."""
        cfg = get_config().sources.understat
        loader = UnderstatLoader(config=cfg)
        player_season = loader.ingest_player_season_stats()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        UnderstatLoader.save_parquet(player_season, _RAW_DIR / "player_season.parquet")
        logger.info("Player season stats ingested: %d records", len(player_season))

    # Both tasks are independent — no data flows between them.
    ingest_shots_task()
    ingest_player_season_task()


ingest_understat()
