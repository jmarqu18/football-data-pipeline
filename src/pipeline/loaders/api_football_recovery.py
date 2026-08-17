"""Fixture-based recovery for players truncated by the API-Football free-tier page cap.

``/players`` caps pagination at 3 pages on the free tier, silently truncating
teams with more players than that. This module reconstructs the missing
season totals from ``/fixtures`` + ``/fixtures/players``, which are not
subject to the same cap: it walks every fixture a team played and aggregates
the players' per-match statistics into season totals.

It holds no cache/rate-limit/retry mechanics of its own — those live in
``pipeline.loaders.api_football_transport``, injected here as a seam.  It also
holds no knowledge of the primary ``/players`` ingestion path: the loader in
``pipeline.loaders.api_football_loader`` decides which teams were truncated
and calls this module as a fallback.
"""

from __future__ import annotations

import logging
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, cast

from pydantic import ValidationError

from pipeline.config import ApiFootballConfig
from pipeline.loaders.api_football_transport import APIFootballError, ApiFootballTransport
from pipeline.models.raw import RawAPIFootballPlayer, RawAPIFootballPlayerStats

logger = logging.getLogger(__name__)

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


@dataclass
class _FixtureRecoveryAcc:
    """Accumulator for a single player during fixture-based recovery."""

    player: dict[str, Any]
    team_name: str
    stats: list[dict[str, Any]]


class FixtureRecovery:
    """Recovers players truncated by the free-tier ``/players`` page cap, via fixtures.

    Constructed once per loader instance, sharing its transport so recovery
    requests count against the same cache and rate-limit budget as the
    primary ingestion.

    Args:
        transport: Cache-first, rate-limited HTTP transport for API-Football.
        config: API-Football configuration from ``ingestion.yaml``.
    """

    def __init__(self, transport: ApiFootballTransport, config: ApiFootballConfig) -> None:
        self._transport = transport
        self._config = config

    def fetch_fixtures(self, team_id: int, *, force_refresh: bool = False) -> list[int]:
        """Return all fixture IDs for a team/season (1 API call).

        Note: the ``/fixtures`` list endpoint rejects the ``page`` parameter
        (API-Football returns ``The Page field do not exist``), so this is a
        single, non-paginated request. A team's season fixtures across all
        competitions fit in one response (``paging.total == 1``).
        """
        params = cast("dict[str, str | int]", {"team": team_id, "season": self._config.season})
        data = self._transport.request("fixtures", params, force_refresh=force_refresh)
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

    def _recover_team(
        self,
        team_id: int,
        known_player_ids: set[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Recover players truncated by the free-tier page cap, for one team, via fixtures.

        Walks every fixture the team played and collects the players that
        ``/players`` never returned (those beyond the page cap) from
        ``/fixtures/players``.  Their season statistics are reconstructed by
        aggregating per-fixture entries.

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
                data = self._transport.request(
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

    def recover(
        self,
        truncated_team_ids: set[int],
        known_player_ids: set[int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[RawAPIFootballPlayer], list[RawAPIFootballPlayerStats]]:
        """Recover players for every team truncated by the free-tier page cap.

        Stops gracefully if the daily API limit is hit mid-recovery so the
        pipeline still completes with whatever was recovered.

        Args:
            truncated_team_ids: Teams whose ``/players`` pagination was cut
                short by the free-tier page cap.
            known_player_ids: Player IDs already obtained from ``/players``
                (mutated in place as players are recovered).
            force_refresh: If ``True``, skip cache lookup.

        Returns:
            Tuple of (recovered player profiles, recovered season stats).
        """
        players: list[RawAPIFootballPlayer] = []
        stats: list[RawAPIFootballPlayerStats] = []

        if not truncated_team_ids:
            return players, stats

        for team_id in sorted(truncated_team_ids):
            try:
                recovered_players, recovered_stats = self._recover_team(
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
                len(truncated_team_ids),
            )
        else:
            logger.info(
                "Fixture-based recovery: no additional players recovered (%d truncated teams)",
                len(truncated_team_ids),
            )
        return players, stats
