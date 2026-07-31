"""Loader for API-Football with cache-first HTTP and rate limiting.

Implements the RAW layer ingestion for API-Football endpoints:
``/players``, ``/injuries``, and ``/transfers``.  Each API response
is cached as JSON before extraction.  Extracted records are validated
with Pydantic models and persisted as Parquet files.

Cache strategy:
    Before every HTTP call the loader checks for a cached JSON file
    under ``config.cache_dir``.  If the file exists and its mtime is
    within ``cache_ttl_hours``, the cached response is returned directly.

Rate limiting:
    An in-memory counter tracks API calls per loader instance.  If
    ``max_calls_per_day`` is exceeded, the loader raises
    ``APIFootballError`` instead of silently skipping data.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from pipeline.config import ApiFootballConfig
from pipeline.models.raw import (
    RawAPIFootballInjury,
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballStandings,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://v3.football.api-sports.io"
_MAX_RETRIES = 3
_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0)

# Additive stat fields aggregated when reconstructing season totals from
# per-fixture responses (see ``_aggregate_fixture_stats``).  Fields not listed
# here (e.g. rating, accuracy) are averaged instead of summed.
_AGGREGATE_ADDITIVE_FIELDS: dict[str, tuple[str, ...]] = {
    "shots": ("total", "on"),
    "goals": ("total", "conceded", "assists", "saves"),
    "passes": ("total", "key"),
    "tackles": ("total", "blocks", "interceptions"),
    "duels": ("total", "won"),
    "dribbles": ("attempts", "success", "past"),
    "fouls": ("drawn", "committed"),
    "cards": ("yellow", "yellowred", "red"),
    "penalty": ("won", "committed", "scored", "missed", "saved"),
}


# ─────────────────────────────────────────────────────────────
# Exception
# ─────────────────────────────────────────────────────────────


class APIFootballError(Exception):
    """Raised when API-Football returns an error or is unreachable."""


class APIFootballPlanRestricted(APIFootballError):
    """Raised when the configured season is blocked by the API-Football plan.

    Unlike a generic :class:`APIFootballError`, this signals a known, expected
    limitation of the free tier (seasons are capped, e.g. 2022–2024) rather than
    a transient failure.  Callers should surface the message to the operator
    instead of retrying, since retrying cannot succeed for the same season.
    """


@dataclass
class _FixtureRecoveryAcc:
    """Accumulator for a single player during fixture-based recovery."""

    player: dict[str, Any]
    team_name: str
    stats: list[dict[str, Any]]


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
        self._api_key = api_key
        self._client = client or httpx.Client(
            base_url=_BASE_URL,
            headers={"x-apisports-key": api_key},
            timeout=30.0,
        )
        self._owns_client = client is None
        self._cache_ttl_seconds = config.cache_ttl_hours * 3600
        self._calls_made = 0
        self._cache_hits = 0
        self._last_call_time = 0.0
        # Teams whose /players pagination was truncated by the free-tier page
        # cap (paging.total > 3).  Used to trigger fixture-based recovery as a
        # fallback.  Empty on paid plans (no truncation), so recovery is a
        # no-op there.
        self._truncated_team_ids: set[int] = set()

    def close(self) -> None:
        """Close the HTTP client if it was created internally."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> APIFootballLoader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ─────────────────────────────────────────────────────────
    # HTTP + cache
    # ─────────────────────────────────────────────────────────

    def _cache_path(self, endpoint: str, params: dict[str, str | int]) -> Path:
        """Build a human-readable cache file path from endpoint and params.

        Examples::

            players/league_140_season_2024_page_1.json
            injuries/league_140_season_2024_page_1.json
            transfers/team_529.json
        """
        parts = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
        return Path(self._config.cache_dir) / endpoint / f"{parts}.json"

    def _read_cache(self, path: Path) -> dict[str, Any] | None:
        """Return cached JSON if the file exists and TTL has not expired."""
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self._cache_ttl_seconds:
            logger.debug("Cache expired: %s (age=%.0fs)", path, age)
            return None
        with path.open(encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
        self._cache_hits += 1
        logger.debug("Cache hit: %s", path)
        return data

    def _write_cache(self, path: Path, data: dict[str, Any]) -> None:
        """Write raw API response JSON to cache."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def _make_request(
        self,
        endpoint: str,
        params: dict[str, str | int],
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Execute a cache-first HTTP GET against API-Football.

        Args:
            endpoint: API endpoint name (e.g. ``"players"``).
            params: Query parameters for the request.
            force_refresh: If ``True``, skip the cache lookup.

        Returns:
            The full parsed JSON response dict (envelope included).

        Raises:
            APIFootballError: On rate limit exhaustion, HTTP errors after
                retries, or API-level errors.
        """
        cache_file = self._cache_path(endpoint, params)

        if not force_refresh:
            cached = self._read_cache(cache_file)
            if cached is not None:
                return cached

        # Rate limit check
        max_calls = self._config.rate_limit.max_calls_per_day
        if self._calls_made >= max_calls:
            msg = f"Daily rate limit exhausted ({max_calls} calls). Cannot request {endpoint} {params}"
            logger.error(msg)
            raise APIFootballError(msg)

        # Enforce delay between calls
        delay = self._config.rate_limit.delay_between_calls
        elapsed = time.time() - self._last_call_time
        if elapsed < delay and self._last_call_time > 0:
            time.sleep(delay - elapsed)

        # Retry loop
        last_error: Exception | None = None
        for attempt, backoff in enumerate(_RETRY_BACKOFF_SECONDS):
            try:
                response = self._client.get(f"/{endpoint}", params=params)
                response.raise_for_status()
                break
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_error = exc
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(
                        "Retry %d/%d for %s %s: %s",
                        attempt + 1,
                        _MAX_RETRIES,
                        endpoint,
                        params,
                        exc,
                    )
                    time.sleep(backoff)
        else:
            msg = f"Failed after {_MAX_RETRIES} retries: {endpoint} {params}"
            logger.error(msg)
            raise APIFootballError(msg) from last_error

        self._last_call_time = time.time()
        self._calls_made += 1

        data: dict[str, Any] = response.json()

        # Check API-level errors
        api_errors = data.get("errors")
        if api_errors and len(api_errors) > 0:
            errors_dict = api_errors if isinstance(api_errors, dict) else {}
            # Rate limit: wait 65s and retry once (rolling 1-minute window)
            if "rateLimit" in errors_dict:
                logger.warning("Rate limit hit for %s %s — sleeping 65s then retrying", endpoint, params)
                time.sleep(65)
                return self._make_request(endpoint, params, force_refresh=force_refresh)
            # Plan restriction: the free tier caps either the accessible season
            # range (e.g. 2022-2024) or the ``page`` parameter (max 3 for
            # /players).  This is permanent for the given request — raise a
            # clear, actionable error instead of a cryptic API envelope dump.
            # Note: pagination-triggered page limits are normally caught and
            # tolerated by ``_paginate`` (partial data returned), but the
            # message below still helps operators understand *why*.
            plan_msg = errors_dict.get("plan")
            if plan_msg:
                is_page_limit = "page" in plan_msg.lower()
                if is_page_limit:
                    guidance = (
                        "Free tier caps the 'page' parameter (max 3). "
                        "Player data is truncated to the first 3 pages per query; "
                        "this is expected on the free plan and the loader degrades gracefully. "
                        "For complete player coverage, upgrade to a paid plan."
                    )
                else:
                    guidance = (
                        f"The configured season {self._config.season} is outside the "
                        f"accessible range for the current plan. Use a season within the "
                        f"allowed range, or upgrade to a paid plan."
                    )
                msg = f"API-Football plan restriction: {plan_msg} {guidance} (request: {endpoint} {params})"
                logger.error(msg)
                raise APIFootballPlanRestricted(msg)
            # errors can be a list or a dict depending on the error type
            msg = f"API-Football error for {endpoint} {params}: {api_errors}"
            logger.error(msg)
            raise APIFootballError(msg)

        # Log rate limit headers
        remaining = response.headers.get("x-ratelimit-requests-remaining")
        if remaining is not None:
            remaining_int = int(remaining)
            if remaining_int < 20:
                logger.warning("API rate limit: %d calls remaining today", remaining_int)
            else:
                logger.debug("API rate limit: %d calls remaining today", remaining_int)

        paging = data.get("paging", {})
        results = data.get("results", 0)
        logger.debug(
            "GET /%s %s → %d results (page %d/%d)",
            endpoint,
            params,
            results,
            paging.get("current", 1),
            paging.get("total", 1),
        )

        self._write_cache(cache_file, data)
        return data

    def _paginate(
        self,
        endpoint: str,
        params: dict[str, str | int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch all pages for a paginated endpoint.

        Returns:
            A tuple of ``(items, total_pages)`` where ``items`` is the flat
            list of all ``response`` entries collected and ``total_pages`` is
            the ``paging.total`` reported by the API (the last value observed
            before any early stop).  ``total_pages`` lets callers detect
            truncation when the free-tier page cap kicks in.
        """
        all_items: list[dict[str, Any]] = []
        page = 1
        total_pages = 1

        while True:
            page_params = {**params, "page": page}
            try:
                data = self._make_request(endpoint, page_params, force_refresh=force_refresh)
            except APIFootballError as exc:
                if page > 1:
                    # Free plan limits pagination (e.g. max 3 pages). Return
                    # whatever we have collected so far rather than crashing.
                    logger.warning("Stopping pagination at page %d for %s: %s", page, endpoint, exc)
                    break
                raise
            all_items.extend(data.get("response", []))

            paging = data.get("paging", {})
            total_pages = paging.get("total", 1)
            if page >= total_pages:
                break
            page += 1

        return all_items, total_pages

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
        data = self._make_request("teams", params, force_refresh=force_refresh)
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
            raw_items = self._paginate("players", params, force_refresh=force_refresh)[0]

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
        ``recover_truncated_players``).

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
            team_items, total_pages = self._paginate("players", params, force_refresh=force_refresh)
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
            self._calls_made,
            self._cache_hits,
        )
        return players, stats

    # ─────────────────────────────────────────────────────────
    # Fixture-based recovery (free-tier fallback)
    # ─────────────────────────────────────────────────────────

    def fetch_fixtures(self, team_id: int, *, force_refresh: bool = False) -> list[int]:
        """Return all fixture IDs for a team/season (1 API call).

        Used by ``ingest_players_from_fixtures`` to enumerate the matches from
        which player statistics are reconstructed.

        Note: the ``/fixtures`` list endpoint rejects the ``page`` parameter
        (API-Football returns ``The Page field do not exist``), so this is a
        single, non-paginated request. A team's season fixtures across all
        competitions fit in one response (``paging.total == 1``).
        """
        params = cast("dict[str, str | int]", {"team": team_id, "season": self._config.season})
        data = self._make_request("fixtures", params, force_refresh=force_refresh)
        return [f["fixture"]["id"] for f in data.get("response", []) if f.get("fixture", {}).get("id") is not None]

    @staticmethod
    def _aggregate_fixture_stats(
        stat_list: list[dict[str, Any]],
        *,
        player_id: int,
        team_id: int,
        team_name: str,
        league_id: int,
        season: int,
    ) -> dict[str, Any]:
        """Reconstruct season-total statistics from per-fixture stat entries.

        ``/fixtures/players`` returns one ``statistics[]`` entry *per match*,
        whereas ``/players`` returns season totals.  This collapses the list
        into a single season-total dict matching the ``RawAPIFootballPlayerStats``
        shape: additive fields are summed, while ``rating`` and ``passes.accuracy``
        are averaged (weighted by appearances) and identity fields take the
        first non-null value.

        Args:
            stat_list: One ``statistics[0]`` dict per fixture the player featured in.
            player_id/team_id/team_name/league_id/season: context for the row.

        Returns:
            A dict suitable for ``RawAPIFootballPlayerStats.model_validate``.
        """
        games_add = {"appearences": 0, "lineups": 0, "minutes": 0}
        games_position: str | None = None
        games_number: int | None = None
        games_captain = False
        ratings: list[float] = []
        accuracies: list[int] = []
        agg: dict[str, dict[str, int]] = {
            cat: dict.fromkeys(fields, 0) for cat, fields in _AGGREGATE_ADDITIVE_FIELDS.items()
        }

        for s in stat_list:
            g = s.get("games") or {}
            for f in ("appearences", "lineups", "minutes"):
                v = g.get(f)
                if v is not None:
                    games_add[f] += v
            r = g.get("rating")
            if r is not None:
                with suppress(ValueError, TypeError):
                    ratings.append(float(r))
            a = s.get("passes", {}).get("accuracy")
            if a is not None:
                with suppress(ValueError, TypeError):
                    accuracies.append(int(a))
            if games_position is None and g.get("position"):
                games_position = g.get("position")
            if games_number is None and g.get("number") is not None:
                games_number = g.get("number")
            if g.get("captain"):
                games_captain = True
            for cat, fields in _AGGREGATE_ADDITIVE_FIELDS.items():
                sc = s.get(cat) or {}
                for f in fields:
                    v = sc.get(f)
                    if v is not None:
                        agg[cat][f] += v

        rating_avg = (sum(ratings) / len(ratings)) if ratings else None
        accuracy_avg = (round(sum(accuracies) / len(accuracies))) if accuracies else None

        return {
            "player_id": player_id,
            "team_id": team_id,
            "team_name": team_name,
            "league_id": league_id,
            "season": season,
            "games": {
                "appearances": games_add["appearences"] or None,
                "lineups": games_add["lineups"] or None,
                "minutes": games_add["minutes"] or None,
                "number": games_number,
                "position": games_position,
                "rating": (f"{rating_avg:.6f}" if rating_avg is not None else None),
                "captain": games_captain,
            },
            "shots": {"total": agg["shots"]["total"] or None, "on": agg["shots"]["on"] or None},
            "goals": {f: agg["goals"][f] or None for f in ("total", "conceded", "assists", "saves")},
            "passes": {
                "total": agg["passes"]["total"] or None,
                "key": agg["passes"]["key"] or None,
                "accuracy": accuracy_avg,
            },
            "tackles": {f: agg["tackles"][f] or None for f in ("total", "blocks", "interceptions")},
            "duels": {f: agg["duels"][f] or None for f in ("total", "won")},
            "dribbles": {f: agg["dribbles"][f] or None for f in ("attempts", "success", "past")},
            "fouls": {f: agg["fouls"][f] or None for f in ("drawn", "committed")},
            "cards": {f: agg["cards"][f] or None for f in ("yellow", "yellowred", "red")},
            "penalty": {f: agg["penalty"][f] or None for f in ("won", "committed", "scored", "missed", "saved")},
        }

    def ingest_players_from_fixtures(
        self,
        team_id: int,
        known_player_ids: set[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Recover players truncated by the free-tier page cap, via fixtures.

        For a team whose ``/players`` pagination was cut at page 3, this walks
        every fixture and collects the players that ``/players`` never returned
        (those beyond the page cap) from ``/fixtures/players``.  Their season
        statistics are reconstructed by aggregating per-fixture entries.

        This is a **fallback** for the free tier: on a paid plan ``/players``
        returns all pages, ``self._truncated_team_ids`` is empty, and this is
        never invoked.

        Args:
            team_id: Team whose truncated players should be recovered.
            known_player_ids: Player IDs already obtained from ``/players``;
                they are skipped to avoid duplicates.  Updated in place with
                any recovered IDs.
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            Tuple of (recovered player profiles, recovered season stats).
        """
        fixture_ids = self.fetch_fixtures(team_id, force_refresh=force_refresh)

        # pid -> accumulator: player info, team name, list of per-fixture stats
        acc: dict[int, _FixtureRecoveryAcc] = {}

        for fid in fixture_ids:
            try:
                data = self._make_request(
                    "fixtures/players", cast("dict[str, str | int]", {"fixture": fid}), force_refresh=force_refresh
                )
            except APIFootballError as exc:
                # Daily limit or transient error — stop recovery gracefully and
                # report the gap rather than crashing the whole ingest.
                logger.warning("Fixture recovery stopped at fixture %d (team %d): %s", fid, team_id, exc)
                break
            for block in data.get("response", []):
                if block.get("team", {}).get("id") != team_id:
                    continue
                team_name = block.get("team", {}).get("name", "")
                for entry in block.get("players", []):
                    pid = entry.get("player", {}).get("id")
                    if pid is None or pid in known_player_ids:
                        continue
                    rec = acc.setdefault(pid, _FixtureRecoveryAcc(entry["player"], team_name, []))
                    stat_entries = entry.get("statistics") or [{}]
                    if stat_entries:
                        rec.stats.append(stat_entries[0])

        players: list[RawAPIFootballPlayer] = []
        stats: list[RawAPIFootballPlayerStats] = []
        rejected = 0

        for pid, rec in acc.items():
            pinfo = rec.player
            name = pinfo.get("name") or f"player-{pid}"
            parts = name.split(" ", 1)
            firstname = parts[0] if len(parts) > 1 else None
            lastname = parts[1] if len(parts) > 1 else None
            try:
                player_model = RawAPIFootballPlayer(
                    player_id=pid,
                    name=name,
                    firstname=firstname,
                    lastname=lastname,
                    photo_url=pinfo.get("photo"),
                )
            except (ValidationError, KeyError) as exc:
                logger.warning("Rejected recovered player %s: %s", pid, exc)
                rejected += 1
                continue

            if not rec.stats:
                players.append(player_model)
                known_player_ids.add(pid)
                continue

            try:
                stat_dict = self._aggregate_fixture_stats(
                    rec.stats,
                    player_id=pid,
                    team_id=team_id,
                    team_name=rec.team_name,
                    league_id=self._config.league_id,
                    season=self._config.season,
                )
                stat_model = RawAPIFootballPlayerStats.model_validate(stat_dict)
            except (ValidationError, KeyError) as exc:
                logger.warning("Rejected recovered stats for player %s: %s", pid, exc)
                rejected += 1
                continue

            players.append(player_model)
            stats.append(stat_model)
            known_player_ids.add(pid)

        logger.info(
            "Fixture recovery for team %d: %d players, %d stats, %d rejected, %d fixtures scanned",
            team_id,
            len(players),
            len(stats),
            rejected,
            len(fixture_ids),
        )
        return players, stats

    def recover_truncated_players(
        self,
        known_player_ids: set[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Recover players for every team truncated by the free-tier page cap.

        Iterates ``self._truncated_team_ids`` (populated by ``ingest_players``)
        and delegates to ``ingest_players_from_fixtures``.  Stops gracefully if
        the daily API limit is hit mid-recovery so the pipeline still completes
        with whatever was recovered.

        Args:
            known_player_ids: Player IDs already obtained from ``/players``
                (mutated in place as players are recovered).
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            Tuple of (recovered player profiles, recovered season stats).
        """
        players: list[RawAPIFootballPlayer] = []
        stats: list[RawAPIFootballPlayerStats] = []

        if not self._truncated_team_ids:
            return players, stats

        for team_id in sorted(self._truncated_team_ids):
            try:
                recovered_players, recovered_stats = self.ingest_players_from_fixtures(
                    team_id, known_player_ids, force_refresh=force_refresh
                )
            except APIFootballError as exc:
                logger.warning("Stopping fixture recovery at team %d: %s", team_id, exc)
                break
            players.extend(recovered_players)
            stats.extend(recovered_stats)

        if players:
            logger.info(
                "Fixture-based recovery: %d players / %d stats recovered for %d truncated teams",
                len(players),
                len(stats),
                len(self._truncated_team_ids),
            )
        else:
            logger.info(
                "Fixture-based recovery: no additional players recovered (%d truncated teams)",
                len(self._truncated_team_ids),
            )
        return players, stats

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
        # /injuries does not support the `page` parameter — use _make_request directly.
        data = self._make_request("injuries", params, force_refresh=force_refresh)
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
            self._calls_made,
            self._cache_hits,
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
                data = self._make_request(
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
            self._calls_made,
            self._cache_hits,
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
        raw = self._make_request("standings", params=params, force_refresh=force_refresh)

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
            self._calls_made,
            self._cache_hits,
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
