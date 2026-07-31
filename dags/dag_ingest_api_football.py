"""Airflow DAG for ingesting API-Football data into the RAW layer.

Tasks:
    1. fetch_teams       — 1 API call → returns team_ids via XCom.
    2. ingest_players    — per-team pagination (~40 calls) → Parquet.
    3. ingest_injuries   — injury records → Parquet (independent of teams).
    4. ingest_transfers  — transfer records per team → Parquet.
    5. ingest_standings  — league standings (1 API call) → Parquet (independent).

Configured scope:
    Tasks 2-5 each map to an endpoint key in ``config/ingestion.yaml`` and are
    skipped when their key is absent, so the YAML is the single source of truth
    for what gets ingested — same contract ``ingest_all`` honours.
    ``fetch_teams`` is not gated: ``teams`` is not an endpoint key, it costs one
    call, and ``teams.parquet`` feeds team entity resolution downstream
    regardless of which player endpoints are enabled.

Free-tier workaround:
    The ``/players`` endpoint limits free plans to page ≤ 3 per query.
    By querying per team (``?team={id}&season=2024``), each team fits in
    1-2 pages (~25-35 players), recovering all ~500-700 players.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from pipeline.config import get_config
from pipeline.loaders.api_football_loader import APIFootballLoader

logger = logging.getLogger(__name__)

_RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "api_football"


def _require_endpoint(name: str) -> None:
    """Skip the calling task unless *name* is in the configured endpoints.

    Raises AirflowSkipException rather than returning early so the run shows
    the task as skipped instead of green — a task that ingested nothing must
    not look like a task that ingested successfully.
    """
    endpoints = get_config().sources.api_football.endpoints
    if name not in endpoints:
        raise AirflowSkipException(f"Endpoint {name!r} not enabled in ingestion.yaml (endpoints={list(endpoints)})")


def _loader() -> APIFootballLoader:
    """Build a loader from the ingestion config and the deployment environment.

    Reading the API key from the environment belongs here, at the edge, rather
    than inside the loader — the loader takes its credentials explicitly so it
    stays constructible in tests without touching os.environ.
    """
    return APIFootballLoader(
        config=get_config().sources.api_football,
        api_key=os.environ["API_FOOTBALL_KEY"],
    )


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
        with _loader() as loader:
            teams = loader.fetch_teams()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(teams, _RAW_DIR / "teams.parquet")
        logger.info("Teams fetched: %d teams", len(teams))
        return [t.team_id for t in teams]

    @task
    def ingest_players_task(team_ids: list[int]) -> None:
        """Fetch players and season stats per team; save to Parquet."""
        _require_endpoint("players_stats")
        with _loader() as loader:
            players, stats = loader.ingest_players_with_recovery(team_ids=team_ids)

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(players, _RAW_DIR / "players.parquet")
        APIFootballLoader.save_parquet(stats, _RAW_DIR / "player_stats.parquet")
        logger.info("Players ingested: %d players, %d stats", len(players), len(stats))

    # none_failed: the edge from ingest_players is rate-limit sequencing, not a
    # data dependency, so a players task skipped by config must not cascade here.
    @task(trigger_rule="none_failed")
    def ingest_injuries_task() -> None:
        """Fetch injury records for the configured league and season."""
        _require_endpoint("injuries")
        with _loader() as loader:
            injuries = loader.ingest_injuries()

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(injuries, _RAW_DIR / "injuries.parquet")
        logger.info("Injuries ingested: %d records", len(injuries))

    # none_failed: same reason as injuries. fetch_teams is never skipped, so
    # team_ids is always available when this task actually runs.
    @task(trigger_rule="none_failed")
    def ingest_transfers_task(team_ids: list[int]) -> None:
        """Fetch transfer records for each team."""
        _require_endpoint("transfers")
        with _loader() as loader:
            transfers = loader.ingest_transfers(team_ids)

        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        APIFootballLoader.save_parquet(transfers, _RAW_DIR / "transfers.parquet")
        logger.info("Transfers ingested: %d records", len(transfers))

    @task
    def ingest_standings_task() -> int:
        """Fetch league standings from API-Football (1 API call)."""
        _require_endpoint("standings")
        with _loader() as loader:
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
    #
    # The graph is fixed; endpoints disabled in ingestion.yaml surface as
    # skipped tasks rather than a reshaped DAG, so task history stays
    # comparable across runs when the configured scope changes.
    team_ids = fetch_teams_task()
    players = ingest_players_task(team_ids)
    injuries = ingest_injuries_task()
    players >> injuries >> ingest_transfers_task(team_ids)
    ingest_standings_task()


ingest_api_football()
