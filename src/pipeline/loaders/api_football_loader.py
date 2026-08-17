"""Loader for API-Football: endpoint semantics on top of a shared transport.

Implements the RAW layer ingestion for API-Football endpoints:
``/players``, ``/injuries``, ``/transfers``, ``/teams`` and ``/standings``.
Each endpoint's JSON shape is flattened here and validated with Pydantic
models before being persisted as Parquet files.

Cache-first HTTP, rate limiting and retry live in
``pipeline.loaders.api_football_transport`` (``ApiFootballTransport``).
Recovering players truncated by the free-tier ``/players`` page cap lives in
``pipeline.loaders.api_football_recovery`` (``FixtureRecovery``). This module
composes both behind ``APIFootballLoader`` and adds nothing but endpoint
knowledge: which params an endpoint takes, how to flatten its response, and
which teams need fixture-based recovery.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from pipeline.config import ApiFootballConfig
from pipeline.loaders.api_football_recovery import FixtureRecovery
from pipeline.loaders.api_football_transport import (
    APIFootballError,
    APIFootballPlanRestricted,
    ApiFootballTransport,
)
from pipeline.models.raw import (
    RawAPIFootballInjury,
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballStandings,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
)

__all__ = [
    "APIFootballError",
    "APIFootballLoader",
    "APIFootballPlanRestricted",
]

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────


class APIFootballLoader:
    """Loader for API-Football with cache-first HTTP and rate limiting.

    Args:
        config: API-Football configuration from ``ingestion.yaml``.
        api_key: API key for the ``x-apisports-key`` header.
        client: Optional injectable ``httpx.Client`` (for testing).
    """

    def __init__(
        self,
        config: ApiFootballConfig,
        api_key: str,
        client: httpx.Client | None = None,
    ) -> None:
        self._config = config
        self._transport = ApiFootballTransport(config, api_key, client)
        self._recovery = FixtureRecovery(self._transport, config)
        # Teams whose /players pagination was truncated by the free-tier page
        # cap (paging.total > 3).  Used to trigger fixture-based recovery as a
        # fallback.  Empty on paid plans (no truncation), so recovery is a
        # no-op there.
        self._truncated_team_ids: set[int] = set()

    def close(self) -> None:
        """Close the HTTP client if it was created internally."""
        self._transport.close()

    def __enter__(self) -> APIFootballLoader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ─────────────────────────────────────────────────────────
    # Extraction: API JSON → Pydantic model dicts
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _extract_player(raw_item: dict[str, Any]) -> dict[str, Any]:
        """Flatten a ``/players`` response item into a ``RawAPIFootballPlayer`` dict."""
        p = raw_item["player"]
        birth = p.get("birth") or {}
        return {
            "player_id": p["id"],
            "name": p["name"],
            "firstname": p.get("firstname"),
            "lastname": p.get("lastname"),
            "age": p.get("age"),
            "birth_date": birth.get("date"),
            "nationality": p.get("nationality"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "photo_url": p.get("photo"),
        }

    @staticmethod
    def _extract_player_stats(player_id: int, stat: dict[str, Any]) -> dict[str, Any]:
        """Flatten one ``statistics[]`` entry into a ``RawAPIFootballPlayerStats`` dict.

        Fixes known API typos:
        - ``games.appearences`` → ``games.appearances``
        - ``penalty.commited`` → ``penalty.committed``
        """
        games_raw = stat.get("games") or {}
        games = {
            "appearances": games_raw.get("appearences"),
            "lineups": games_raw.get("lineups"),
            "minutes": games_raw.get("minutes"),
            "number": games_raw.get("number"),
            "position": games_raw.get("position"),
            "rating": games_raw.get("rating"),
            "captain": games_raw.get("captain", False),
        }

        penalty_raw = stat.get("penalty") or {}
        penalty = {
            "won": penalty_raw.get("won"),
            "committed": penalty_raw.get("commited"),
            "scored": penalty_raw.get("scored"),
            "missed": penalty_raw.get("missed"),
            "saved": penalty_raw.get("saved"),
        }

        return {
            "player_id": player_id,
            "team_id": stat["team"]["id"],
            "team_name": stat["team"]["name"],
            "league_id": stat["league"]["id"],
            "season": stat["league"]["season"],
            "games": games,
            "shots": stat.get("shots") or {},
            "goals": stat.get("goals") or {},
            "passes": stat.get("passes") or {},
            "tackles": stat.get("tackles") or {},
            "duels": stat.get("duels") or {},
            "dribbles": stat.get("dribbles") or {},
            "fouls": stat.get("fouls") or {},
            "cards": stat.get("cards") or {},
            "penalty": penalty,
        }

    @staticmethod
    def _extract_injury(raw_item: dict[str, Any]) -> dict[str, Any]:
        """Flatten an ``/injuries`` response item into a ``RawAPIFootballInjury`` dict."""
        fixture = raw_item.get("fixture") or {}
        fixture_id = fixture.get("id")

        # Date comes from fixture.date (ISO with timezone) — extract date part
        fixture_date = fixture.get("date")
        date_str = fixture_date[:10] if fixture_date else None

        return {
            "player_id": raw_item["player"]["id"],
            "player_name": raw_item["player"]["name"],
            "team_id": raw_item["team"]["id"],
            "team_name": raw_item["team"]["name"],
            "fixture_id": fixture_id,
            "league_id": raw_item["league"]["id"],
            "reason": raw_item["player"]["reason"],
            "type": raw_item["player"]["type"],
            "date": date_str or "unknown",
        }

    @staticmethod
    def _extract_transfer(player_id: int, player_name: str, transfer: dict[str, Any]) -> dict[str, Any]:
        """Flatten one transfer entry into a ``RawAPIFootballTransfer`` dict."""
        teams = transfer.get("teams") or {}
        team_in = teams.get("in") or {}
        team_out = teams.get("out") or {}
        return {
            "player_id": player_id,
            "player_name": player_name,
            "date": transfer.get("date"),
            "team_in_id": team_in.get("id"),
            "team_in_name": team_in.get("name"),
            "team_out_id": team_out.get("id"),
            "team_out_name": team_out.get("name"),
            "type": transfer.get("type"),
        }

    @staticmethod
    def _extract_team(item: dict[str, Any]) -> RawAPIFootballTeam:
        """Map one /teams response entry to RawAPIFootballTeam."""
        t = item["team"]
        v = item.get("venue") or {}
        return RawAPIFootballTeam(
            team_id=t["id"],
            name=t["name"],
            code=t.get("code") or None,
            country=t.get("country") or None,
            founded=t.get("founded") or None,
            national=bool(t.get("national", False)),
            logo_url=t.get("logo") or None,
            venue_name=v.get("name") or None,
            venue_address=v.get("address") or None,
            venue_city=v.get("city") or None,
            venue_capacity=v.get("capacity") or None,
            venue_surface=v.get("surface") or None,
            venue_image_url=v.get("image") or None,
        )

    # ─────────────────────────────────────────────────────────
    # Public ingestion methods
    # ─────────────────────────────────────────────────────────

    def fetch_teams(self, *, force_refresh: bool = False) -> list[RawAPIFootballTeam]:
        """Fetch all teams for the configured league/season (1 API call).

        Captures team identity, founding info, logo and home venue.

        Returns:
            List of validated team models, sorted by team_id.

        Raises:
            APIFootballError: If the response contains zero teams.
        """
        params: dict[str, str | int] = {
            "league": self._config.league_id,
            "season": self._config.season,
        }
        data = self._transport.request("teams", params, force_refresh=force_refresh)
        raw_teams = data.get("response", [])

        teams: list[RawAPIFootballTeam] = []
        for item in raw_teams:
            try:
                teams.append(self._extract_team(item))
            except (KeyError, ValidationError) as exc:
                logger.warning("Skipping malformed team entry: %s — %s", item, exc)

        if not teams:
            msg = f"No teams found for league={self._config.league_id} season={self._config.season}"
            logger.error(msg)
            raise APIFootballError(msg)

        teams.sort(key=lambda t: t.team_id)
        logger.info(
            "Fetched %d teams for league %d season %d",
            len(teams),
            self._config.league_id,
            self._config.season,
        )
        return teams

    def fetch_team_ids(self, *, force_refresh: bool = False) -> list[int]:
        """Fetch all team IDs for the configured league and season (1 API call).

        Delegates to ``fetch_teams()`` and returns only the IDs.  Used to
        drive per-team player pagination on the free tier (3-page limit).

        Returns:
            Sorted list of team IDs.

        Raises:
            APIFootballError: If the response contains zero teams.
        """
        return [t.team_id for t in self.fetch_teams(force_refresh=force_refresh)]

    def ingest_players(
        self,
        *,
        team_ids: list[int] | None = None,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Ingest players and their season statistics from ``/players``.

        Supports two modes:
        - **Per-team** (recommended for free tier): pass ``team_ids`` to
          paginate each team independently, bypassing the 3-page global limit.
        - **Per-league** (fallback): omit ``team_ids`` to paginate the full
          league in a single query (limited to 3 pages / 60 players on free tier).

        Args:
            team_ids: If provided, fetch players for each team separately.
            force_refresh: If ``True``, skip cache for all requests.

        Returns:
            Tuple of (validated players, validated player stats).
        """
        # Reset first: the attribute must describe this call only, otherwise a
        # later recovery would revisit teams truncated by an earlier one.
        self._truncated_team_ids = set()

        if team_ids:
            raw_items, truncated = self._fetch_players_per_team(team_ids, force_refresh=force_refresh)
            self._truncated_team_ids = truncated
        else:
            params: dict[str, str | int] = {
                "league": self._config.league_id,
                "season": self._config.season,
            }
            raw_items = self._transport.paginate("players", params, force_refresh=force_refresh)[0]

        return self._parse_player_items(raw_items)

    def _fetch_players_per_team(
        self,
        team_ids: list[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[dict[str, Any]], set[int]]:
        """Paginate ``/players`` per team to bypass the free-tier 3-page limit.

        Each team is queried independently (``?team={id}&season={}``).
        The returned list contains ALL items across all teams — one item per
        player per team.  A player who transferred mid-season appears once per
        team with that team's ``statistics[]`` entry, preserving full
        player×team granularity.  Profile deduplication is handled downstream
        in ``_parse_player_items``.

        Teams whose ``paging.total`` exceeds 3 are recorded as truncated: the
        free-tier page cap prevents fetching their remaining players, so they
        become candidates for fixture-based recovery (see
        ``FixtureRecovery.recover``).

        Args:
            team_ids: Team IDs to iterate over.
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            A tuple of ``(items, truncated_team_ids)`` where ``items`` is the
            flat list of raw response items (one per player×team) and
            ``truncated_team_ids`` is the set of teams whose pagination was
            cut short by the free-tier page cap.
        """
        all_items: list[dict[str, Any]] = []
        truncated_team_ids: set[int] = set()

        for team_id in team_ids:
            params: dict[str, str | int] = {
                "league": self._config.league_id,
                "season": self._config.season,
                "team": team_id,
            }
            team_items, total_pages = self._transport.paginate("players", params, force_refresh=force_refresh)
            if total_pages > 3:
                truncated_team_ids.add(team_id)
            all_items.extend(team_items)

        logger.info(
            "Per-team fetch: %d teams queried, %d player×team items collected, %d truncated by page cap",
            len(team_ids),
            len(all_items),
            len(truncated_team_ids),
        )
        return all_items, truncated_team_ids

    def _parse_player_items(
        self, raw_items: list[dict[str, Any]]
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Validate and extract player profiles and stats from raw API items.

        - ``players``: one ``RawAPIFootballPlayer`` per unique ``player_id``
          (profile data: bio, photo, physical attributes).
        - ``stats``: one ``RawAPIFootballPlayerStats`` per ``statistics[]``
          entry, i.e. one row per (player_id, team_id, season).  A player
          who transferred mid-season produces two stat rows — one per team.

        Factored out of ``ingest_players`` so both per-team and per-league
        paths share identical parsing logic.
        """
        players: list[RawAPIFootballPlayer] = []
        stats: list[RawAPIFootballPlayerStats] = []
        seen_player_ids: set[int] = set()
        rejected = 0

        for item in raw_items:
            try:
                player_dict = self._extract_player(item)
                pid = player_dict["player_id"]
                # One profile per player — deduplicate across per-team items
                if pid not in seen_player_ids:
                    players.append(RawAPIFootballPlayer.model_validate(player_dict))
                    seen_player_ids.add(pid)
            except (ValidationError, KeyError) as exc:
                pid = item.get("player", {}).get("id", "unknown")
                logger.warning("Rejected player %s: %s", pid, exc)
                rejected += 1
                continue

            # One stat row per (player_id, team_id) — keep all entries
            for stat_entry in item.get("statistics", []):
                try:
                    stat_dict = self._extract_player_stats(player_dict["player_id"], stat_entry)
                    stats.append(RawAPIFootballPlayerStats.model_validate(stat_dict))
                except (ValidationError, KeyError) as exc:
                    logger.warning(
                        "Rejected stats for player %s: %s",
                        player_dict["player_id"],
                        exc,
                    )
                    rejected += 1

        logger.info(
            "Players ingested: %d profiles, %d stat rows, %d rejected, %d API calls, %d from cache",
            len(players),
            len(stats),
            rejected,
            self._transport.calls_made,
            self._transport.cache_hits,
        )
        return players, stats

    # ─────────────────────────────────────────────────────────
    # Fixture-based recovery (free-tier fallback)
    # ─────────────────────────────────────────────────────────

    def recover_truncated_players(
        self,
        known_player_ids: set[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Recover players for every team truncated by the free-tier page cap.

        Delegates to ``FixtureRecovery`` for the teams recorded as truncated
        by the most recent ``ingest_players`` call.  Stops gracefully if the
        daily API limit is hit mid-recovery so the pipeline still completes
        with whatever was recovered.

        Args:
            known_player_ids: Player IDs already obtained from ``/players``
                (mutated in place as players are recovered).
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            Tuple of (recovered player profiles, recovered season stats).
        """
        return self._recovery.recover(self._truncated_team_ids, known_player_ids, force_refresh=force_refresh)

    def ingest_players_with_recovery(
        self,
        *,
        team_ids: list[int] | None = None,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Ingest players, transparently recovering any truncated by the page cap.

        Composes ``ingest_players`` with ``recover_truncated_players`` so callers
        get the complete roster without knowing about the free-tier page cap.
        On a paid plan no team is truncated and the recovery step is a no-op.

        Prefer this over calling the two steps yourself; ``ingest_players``
        remains available when you specifically want the un-recovered result.

        Args:
            team_ids: Team IDs for per-team pagination. If omitted, falls back
                to league-level pagination (which the page cap can truncate).
            force_refresh: If ``True``, skip cache for all requests.

        Returns:
            Tuple of (validated players, validated player stats), recovery included.
        """
        players, stats = self.ingest_players(team_ids=team_ids, force_refresh=force_refresh)
        # No guard needed: recover_truncated_players returns empty lists when
        # nothing was truncated.
        recovered_players, recovered_stats = self.recover_truncated_players(
            {p.player_id for p in players}, force_refresh=force_refresh
        )
        return players + recovered_players, stats + recovered_stats

    def ingest_injuries(self, *, force_refresh: bool = False) -> list[RawAPIFootballInjury]:
        """Ingest injury records from ``/injuries``.

        Returns:
            List of validated injury models.
        """
        params: dict[str, str | int] = {
            "league": self._config.league_id,
            "season": self._config.season,
        }
        # /injuries does not support the `page` parameter — use request() directly.
        data = self._transport.request("injuries", params, force_refresh=force_refresh)
        raw_items = data.get("response", [])

        injuries: list[RawAPIFootballInjury] = []
        rejected = 0

        for item in raw_items:
            try:
                injury_dict = self._extract_injury(item)
                injuries.append(RawAPIFootballInjury.model_validate(injury_dict))
            except (ValidationError, KeyError) as exc:
                player_id = item.get("player", {}).get("id", "unknown")
                logger.warning("Rejected injury for player %s: %s", player_id, exc)
                rejected += 1

        logger.info(
            "Injuries ingested: %d valid, %d rejected, %d API calls, %d from cache",
            len(injuries),
            rejected,
            self._transport.calls_made,
            self._transport.cache_hits,
        )
        return injuries

    def ingest_transfers(
        self,
        team_ids: list[int],
        *,
        force_refresh: bool = False,
    ) -> list[RawAPIFootballTransfer]:
        """Ingest transfer records from ``/transfers``.

        The ``/transfers`` endpoint has no season filter — it returns
        the full transfer history for a given team.

        Args:
            team_ids: Team IDs to query.
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            List of validated transfer models.
        """
        transfers: list[RawAPIFootballTransfer] = []
        rejected = 0

        for team_id in team_ids:
            try:
                data = self._transport.request(
                    "transfers",
                    {"team": team_id},
                    force_refresh=force_refresh,
                )
            except APIFootballError as exc:
                # A restricted endpoint (e.g. /transfers on the free plan) or a
                # transient/daily-limit error must not crash the whole ingest.
                # Skip the team and continue so the task still succeeds.
                logger.warning("Skipping transfers for team %d: %s", team_id, exc)
                continue
            for player_entry in data.get("response", []):
                player_id = player_entry["player"]["id"]
                player_name = player_entry["player"]["name"]
                for transfer in player_entry.get("transfers", []):
                    try:
                        transfer_dict = self._extract_transfer(player_id, player_name, transfer)
                        transfers.append(RawAPIFootballTransfer.model_validate(transfer_dict))
                    except (ValidationError, KeyError) as exc:
                        logger.warning(
                            "Rejected transfer for player %s: %s",
                            player_id,
                            exc,
                        )
                        rejected += 1

        logger.info(
            "Transfers ingested: %d valid, %d rejected, %d API calls, %d from cache",
            len(transfers),
            rejected,
            self._transport.calls_made,
            self._transport.cache_hits,
        )
        return transfers

    def ingest_standings(self, *, force_refresh: bool = False) -> list[RawAPIFootballStandings]:
        """Fetch standings for the configured league and season from ``/standings``.

        One API call. Returns one RawAPIFootballStandings per team in the league.

        Returns:
            List of RawAPIFootballStandings, one per team.
        """
        params: dict[str, str | int] = {
            "league": self._config.league_id,
            "season": self._config.season,
        }
        raw = self._transport.request("standings", params, force_refresh=force_refresh)

        results: list[RawAPIFootballStandings] = []
        rejected = 0
        for league_block in raw.get("response", []):
            league_info = league_block.get("league", {})
            league_id = league_info.get("id")
            season = league_info.get("season")
            for group in league_info.get("standings", []):
                for entry in group:
                    team = entry.get("team", {})
                    all_stats = entry.get("all", {})
                    goals = all_stats.get("goals", {})
                    try:
                        rec = RawAPIFootballStandings(
                            league_id=league_id,
                            season=season,
                            team_id=team["id"],
                            team_name=team["name"],
                            rank=entry["rank"],
                            points=entry["points"],
                            played_total=all_stats.get("played", 0),
                            wins=all_stats.get("win", 0),
                            draws=all_stats.get("draw", 0),
                            losses=all_stats.get("lose", 0),
                            goals_for=goals.get("for", 0),
                            goals_against=goals.get("against", 0),
                            goal_diff=entry.get("goalsDiff", 0),
                            form=entry.get("form"),
                        )
                        results.append(rec)
                    except (ValidationError, KeyError) as exc:
                        logger.warning(
                            "Rejected standings entry team_id=%s: %s",
                            team.get("id"),
                            exc,
                        )
                        rejected += 1

        logger.info(
            "Standings: %d teams ingested, %d rejected, for league %d season %d",
            len(results),
            rejected,
            self._config.league_id,
            self._config.season,
        )
        logger.info(
            "Standings: %d API calls, %d from cache",
            self._transport.calls_made,
            self._transport.cache_hits,
        )
        return results

    # ─────────────────────────────────────────────────────────
    # Parquet output
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def save_parquet(models: Sequence[BaseModel], path: Path) -> None:
        """Serialise a sequence of Pydantic models to a Parquet file.

        Nested models are handled natively by pyarrow as struct columns.

        Takes a ``Sequence`` rather than a ``list`` because ``list`` is
        invariant: every caller passes a concrete ``list[RawAPIFootball...]``,
        which is not a ``list[BaseModel]``.

        Args:
            models: Validated Pydantic model instances.
            path: Destination ``.parquet`` file path.
        """
        if not models:
            logger.warning("No records to save to %s", path)
            return
        rows = [m.model_dump() for m in models]
        table = pa.Table.from_pylist(rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        logger.info("Saved %d records to %s", len(rows), path)

    # ─────────────────────────────────────────────────────────
    # Orchestration
    # ─────────────────────────────────────────────────────────

    def ingest_all(
        self,
        output_dir: Path | None = None,
        *,
        team_ids: list[int] | None = None,
        force_refresh: bool = False,
    ) -> dict[str, int]:
        """Run all configured endpoints and save Parquet files.

        If ``team_ids`` is not provided and either ``players_stats`` or
        ``transfers`` are in the configured endpoints, calls
        ``fetch_team_ids()`` first (1 API call) to enable per-team player
        pagination and transfers.

        Args:
            output_dir: Base directory for Parquet output.
                Defaults to ``data/raw/api_football``.
            team_ids: Team IDs for per-team pagination and transfers.
                If omitted, auto-discovered via ``/teams`` endpoint.
            force_refresh: If ``True``, skip cache for all requests.

        Returns:
            Dict mapping record type names to validated record counts.
        """
        out = output_dir or Path("data/raw/api_football")
        endpoints = self._config.endpoints
        counts: dict[str, int] = {}

        # Auto-discover team IDs if not provided
        if team_ids is None and ("players_stats" in endpoints or "transfers" in endpoints):
            team_ids = self.fetch_team_ids(force_refresh=force_refresh)

        if "players_stats" in endpoints:
            players, stats = self.ingest_players_with_recovery(team_ids=team_ids, force_refresh=force_refresh)
            self.save_parquet(players, out / "players.parquet")
            self.save_parquet(stats, out / "player_stats.parquet")
            counts["players"] = len(players)
            counts["player_stats"] = len(stats)

        if "injuries" in endpoints:
            injuries = self.ingest_injuries(force_refresh=force_refresh)
            self.save_parquet(injuries, out / "injuries.parquet")
            counts["injuries"] = len(injuries)

        if "transfers" in endpoints:
            if not team_ids:
                logger.warning("Transfers endpoint configured but no team_ids provided")
            else:
                transfers = self.ingest_transfers(team_ids, force_refresh=force_refresh)
                self.save_parquet(transfers, out / "transfers.parquet")
                counts["transfers"] = len(transfers)

        if "standings" in endpoints:
            standings = self.ingest_standings(force_refresh=force_refresh)
            self.save_parquet(standings, out / "standings.parquet")
            counts["standings"] = len(standings)

        logger.info("Ingest complete: %s", counts)
        return counts
