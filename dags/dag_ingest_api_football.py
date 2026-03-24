"""Airflow DAG for ingesting API-Football data into the RAW layer.

Tasks (sequential to respect the 100 calls/day free tier limit):
    1. ingest_players   — players + season stats → Parquet; returns team_ids via XCom.
    2. ingest_injuries  — injury records → Parquet.
    3. ingest_transfers — transfer records for each team → Parquet.
    4. ingest_standings — league standings (1 API call) → Parquet (independent).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from airflow.sdk import dag, task

from pipeline.config import get_config
from pipeline.loaders.api_football_loader import APIFootballLoader

logger = logging.getLogger(__name__)

_RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "api_football"


@dag(
    dag_id="ingest_api_football",
    schedule=None,
    catchup=False,
    tags=["ingestion", "api_football"],
    doc_md=__doc__,
)
def ingest_api_football() -> None:
    """Ingest API-Football players, injuries, and transfers into RAW Parquet files."""

    @task
    def ingest_players_task() -> list[int]:
        """Fetch players and season stats; return unique team_ids for the transfers task."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            players, stats = loader.ingest_players()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(players, _RAW_DIR / "players.parquet")
        APIFootballLoader.save_parquet(stats, _RAW_DIR / "player_stats.parquet")

        team_ids = sorted({s.team_id for s in stats})
        logger.info("Players ingested. Unique teams: %d", len(team_ids))
        return team_ids

    @task
    def ingest_injuries_task() -> None:
        """Fetch injury records for the configured league and season."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            injuries = loader.ingest_injuries()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(injuries, _RAW_DIR / "injuries.parquet")
        logger.info("Injuries ingested: %d records", len(injuries))

    @task
    def ingest_transfers_task(team_ids: list[int]) -> None:
        """Fetch transfer records for each team extracted in the players task."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            transfers = loader.ingest_transfers(team_ids)

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(transfers, _RAW_DIR / "transfers.parquet")
        logger.info("Transfers ingested: %d records", len(transfers))

    @task
    def ingest_standings_task() -> int:
        """Fetch league standings from API-Football (1 API call)."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            standings = loader.ingest_standings()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(standings, _RAW_DIR / "standings.parquet")
        logger.info("Standings ingested: %d records", len(standings))
        return len(standings)

    # Sequential chain: players → injuries → transfers.
    # Injuries has no data dependency on players, but the explicit ordering avoids
    # simultaneous API calls on the 100 calls/day free tier.
    # Standings runs independently (separate 1-call endpoint).
    team_ids = ingest_players_task()
    injuries = ingest_injuries_task()
    team_ids >> injuries >> ingest_transfers_task(team_ids)
    ingest_standings_task()


ingest_api_football()
