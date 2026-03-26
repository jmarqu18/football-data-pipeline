"""Airflow DAG for ingesting API-Football data into the RAW layer.

Tasks:
    1. fetch_teams       — 1 API call → returns team_ids via XCom.
    2. ingest_players    — per-team pagination (~40 calls) → Parquet.
    3. ingest_injuries   — injury records → Parquet (independent of teams).
    4. ingest_transfers  — transfer records per team → Parquet.
    5. ingest_standings  — league standings (1 API call) → Parquet (independent).

Free-tier workaround:
    The ``/players`` endpoint limits free plans to page ≤ 3 per query.
    By querying per team (``?team={id}&season=2024``), each team fits in
    1-2 pages (~25-35 players), recovering all ~500-700 players.
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
    def fetch_teams_task() -> list[int]:
        """Discover all teams for the configured league; save metadata to Parquet."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            teams = loader.fetch_teams()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(teams, _RAW_DIR / "teams.parquet")
        logger.info("Teams fetched: %d teams", len(teams))
        return [t.team_id for t in teams]

    @task
    def ingest_players_task(team_ids: list[int]) -> None:
        """Fetch players and season stats per team; save to Parquet."""
        cfg = get_config().sources.api_football
        api_key = os.environ["API_FOOTBALL_KEY"]

        with APIFootballLoader(config=cfg, api_key=api_key) as loader:
            players, stats = loader.ingest_players(team_ids=team_ids)

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(players, _RAW_DIR / "players.parquet")
        APIFootballLoader.save_parquet(stats, _RAW_DIR / "player_stats.parquet")
        logger.info("Players ingested: %d players, %d stats", len(players), len(stats))

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
        """Fetch transfer records for each team."""
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

    # Task graph:
    #   fetch_teams ──→ ingest_players ──→ ingest_injuries ──→ ingest_transfers
    #                                                          (uses team_ids from fetch_teams)
    # Injuries has no data dependency on teams but is sequenced between
    # players and transfers to spread API calls and respect rate limits.
    # Standings runs independently (1 call).
    team_ids = fetch_teams_task()
    players = ingest_players_task(team_ids)
    injuries = ingest_injuries_task()
    players >> injuries >> ingest_transfers_task(team_ids)
    ingest_standings_task()


ingest_api_football()
