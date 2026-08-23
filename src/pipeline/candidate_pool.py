"""Candidate pool: the indexed API-Football side of player resolution.

Resolution asks the same questions of the API-Football data over and over —
who plays for this team, what name variants does this player have, what were
their appearances at that club — and answering them by scanning the raw lists
each time would be quadratic. This module builds the indexes once and answers
those questions behind one interface.

It holds no matching state: which players are already taken belongs to
``pipeline.resolution_ledger``. It makes no matching decisions either: whether
two candidates are similar enough belongs to ``pipeline.match_scoring``, and
which pass wins belongs to ``pipeline.entity_resolution`` (see ADR-004).
"""

from __future__ import annotations

from collections.abc import Iterable, KeysView

from pipeline.models.raw import RawAPIFootballPlayer, RawAPIFootballPlayerStats
from pipeline.name_normalization import build_name_variants


class CandidatePool:
    """Indexes the API-Football players and season stats for one resolution run.

    Built once per run, then queried by every pass. The indexes are derived
    from two different sources — identity from ``api_players``, team membership
    and stats from ``api_stats`` — so a player may be known to one and not the
    other. Lookups that a pass performs before scoring are lenient; lookups it
    performs after choosing a match are not.
    """

    def __init__(
        self,
        api_players: Iterable[RawAPIFootballPlayer],
        api_stats: Iterable[RawAPIFootballPlayerStats],
    ) -> None:
        """Build the indexes.

        Args:
            api_players: Biographical records, the source of identity and names.
            api_stats: Season stats, the source of team membership and position.
                A player transferred mid-season has one row per club.
        """
        self._players: dict[int, RawAPIFootballPlayer] = {}
        self._variants: dict[int, list[str]] = {}
        for player in api_players:
            self._players[player.player_id] = player
            self._variants[player.player_id] = build_name_variants(player.name, player.firstname, player.lastname)

        self._stats: dict[int, list[RawAPIFootballPlayerStats]] = {}
        self._by_team: dict[int, set[int]] = {}
        for stat in api_stats:
            self._stats.setdefault(stat.player_id, []).append(stat)
            self._by_team.setdefault(stat.team_id, set()).add(stat.player_id)

    # ── Identity ──

    def player(self, api_id: int) -> RawAPIFootballPlayer:
        """Return the player behind an id.

        Args:
            api_id: API-Football player_id, drawn from this pool's own indexes.

        Returns:
            The biographical record.

        Raises:
            KeyError: If the id is unknown. Callers reach this only after
                choosing a candidate the pool itself offered, so a miss is a bug.
        """
        return self._players[api_id]

    def all_ids(self) -> KeysView[int]:
        """Return every known player id."""
        return self._players.keys()

    def variants(self, api_id: int) -> list[str]:
        """Return the normalized name variants for a player.

        Lenient by design: passes score every candidate before they know which
        one wins, and an id present in the stats but absent from the player
        list must score zero rather than raise.

        Args:
            api_id: API-Football player_id.

        Returns:
            The variants, or an empty list if the player is unknown.
        """
        return self._variants.get(api_id, [])

    # ── Season stats ──

    def in_team(self, api_team_id: int) -> set[int]:
        """Return the ids of players with season stats for a team.

        Args:
            api_team_id: API-Football team_id.

        Returns:
            The player ids, or an empty set for an unknown team. A player
            transferred mid-season appears under both clubs.
        """
        return self._by_team.get(api_team_id, set())

    def position_of(self, api_id: int) -> str | None:
        """Return a player's API-Football position, from their season stats.

        Args:
            api_id: API-Football player_id.

        Returns:
            The first non-empty position found, or None if unknown.
        """
        for stat in self._stats.get(api_id, []):
            if stat.games.position:
                return stat.games.position
        return None

    def stats_for_team(self, api_id: int, api_team_id: int) -> list[RawAPIFootballPlayerStats]:
        """Return a player's season stats at one club.

        Scoping to the club is what makes the numbers comparable to Understat's,
        which are always per club.

        Args:
            api_id: API-Football player_id.
            api_team_id: API-Football team_id.

        Returns:
            The matching rows, empty if the player never played there.
        """
        return [stat for stat in self._stats.get(api_id, []) if stat.team_id == api_team_id]
