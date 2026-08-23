"""Tests for CandidatePool — the indexed API-Football side of resolution.

The pool answers "who could this Understat player be?" from the raw
API-Football lists. It holds no matching state (that is the ledger's job) and
makes no matching decisions (that is entity_resolution's job).

TestPositionOf moved here from test_match_scoring.py when position lookup left
MatchScorer: the lookup needs the season-stats index, and the index belongs to
the pool.
"""

from __future__ import annotations

import pytest

from pipeline.candidate_pool import CandidatePool
from pipeline.models.raw import (
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    _APIFootballCards,
    _APIFootballDribbles,
    _APIFootballDuels,
    _APIFootballFouls,
    _APIFootballGames,
    _APIFootballGoals,
    _APIFootballPasses,
    _APIFootballPenalty,
    _APIFootballShots,
    _APIFootballTackles,
)

_EMPTY_STATS_KWARGS = {
    "shots": _APIFootballShots(),
    "goals": _APIFootballGoals(),
    "passes": _APIFootballPasses(),
    "tackles": _APIFootballTackles(),
    "duels": _APIFootballDuels(),
    "dribbles": _APIFootballDribbles(),
    "fouls": _APIFootballFouls(),
    "cards": _APIFootballCards(),
    "penalty": _APIFootballPenalty(),
}


def _player(
    player_id: int = 1,
    name: str = "Pedro González López",
    firstname: str | None = None,
    lastname: str | None = None,
) -> RawAPIFootballPlayer:
    return RawAPIFootballPlayer(
        player_id=player_id,
        name=name,
        firstname=firstname,
        lastname=lastname,
    )


def _stats(
    player_id: int = 1,
    team_id: int = 529,
    position: str | None = None,
    appearances: int = 0,
    minutes: int = 0,
) -> RawAPIFootballPlayerStats:
    return RawAPIFootballPlayerStats(
        player_id=player_id,
        team_id=team_id,
        team_name="Test FC",
        league_id=140,
        season=2024,
        games=_APIFootballGames(appearances=appearances, minutes=minutes, position=position),
        **_EMPTY_STATS_KWARGS,
    )


# ─────────────────────────────────────────────────────────────
# Identity lookup
# ─────────────────────────────────────────────────────────────


class TestPlayerLookup:
    def test_returns_the_player_behind_an_id(self):
        pool = CandidatePool([_player(player_id=1, name="Robert Lewandowski")], [])

        assert pool.player(1).name == "Robert Lewandowski"

    def test_unknown_player_id_raises(self):
        """Ids always come from the pool's own indexes, so a miss is a bug."""
        pool = CandidatePool([_player(player_id=1)], [])

        with pytest.raises(KeyError):
            pool.player(999)

    def test_all_ids_covers_every_player(self):
        pool = CandidatePool([_player(player_id=1), _player(player_id=2)], [])

        assert set(pool.all_ids()) == {1, 2}

    def test_all_ids_is_empty_without_players(self):
        assert set(CandidatePool([], []).all_ids()) == set()


# ─────────────────────────────────────────────────────────────
# Name variants
# ─────────────────────────────────────────────────────────────


class TestVariants:
    def test_builds_variants_from_the_three_name_fields(self):
        pool = CandidatePool(
            [_player(player_id=1, name="Pedro González López", firstname="Pedro", lastname="González López")],
            [],
        )

        variants = pool.variants(1)
        assert "pedro gonzalez lopez" in variants
        assert "pedro" in variants
        assert "gonzalez lopez" in variants

    def test_unknown_player_has_no_variants(self):
        """Unlike player(), this is lenient — passes score before they resolve."""
        pool = CandidatePool([_player(player_id=1)], [])

        assert pool.variants(999) == []


# ─────────────────────────────────────────────────────────────
# Team membership
# ─────────────────────────────────────────────────────────────


class TestInTeam:
    def test_groups_players_by_team(self):
        pool = CandidatePool(
            [_player(player_id=1), _player(player_id=2), _player(player_id=3)],
            [_stats(1, team_id=529), _stats(2, team_id=529), _stats(3, team_id=541)],
        )

        assert pool.in_team(529) == {1, 2}
        assert pool.in_team(541) == {3}

    def test_unknown_team_has_no_players(self):
        pool = CandidatePool([_player(player_id=1)], [_stats(1, team_id=529)])

        assert pool.in_team(999) == set()

    def test_a_transferred_player_belongs_to_both_teams(self):
        """Mid-season transfers produce one stats row per team."""
        pool = CandidatePool(
            [_player(player_id=1)],
            [_stats(1, team_id=529), _stats(1, team_id=541)],
        )

        assert pool.in_team(529) == {1}
        assert pool.in_team(541) == {1}


# ─────────────────────────────────────────────────────────────
# Position lookup (moved from test_match_scoring.py)
# ─────────────────────────────────────────────────────────────


class TestPositionOf:
    def test_returns_position_from_stats(self):
        pool = CandidatePool([_player(player_id=1)], [_stats(1, position="Midfielder")])

        assert pool.position_of(1) == "Midfielder"

    def test_returns_first_non_empty_position(self):
        pool = CandidatePool(
            [_player(player_id=1)],
            [_stats(1, position=None), _stats(1, position="Defender")],
        )

        assert pool.position_of(1) == "Defender"

    def test_unknown_player_returns_none(self):
        assert CandidatePool([], []).position_of(999) is None

    def test_player_without_position_returns_none(self):
        pool = CandidatePool([_player(player_id=1)], [_stats(1, position=None)])

        assert pool.position_of(1) is None


# ─────────────────────────────────────────────────────────────
# Season stats, scoped to a team
# ─────────────────────────────────────────────────────────────


class TestStatsForTeam:
    def test_returns_only_the_rows_for_that_team(self):
        pool = CandidatePool(
            [_player(player_id=1)],
            [_stats(1, team_id=529, appearances=10), _stats(1, team_id=541, appearances=20)],
        )

        rows = pool.stats_for_team(1, 529)
        assert [r.games.appearances for r in rows] == [10]

    def test_is_empty_when_the_player_never_played_for_that_team(self):
        pool = CandidatePool([_player(player_id=1)], [_stats(1, team_id=529)])

        assert pool.stats_for_team(1, 999) == []

    def test_is_empty_for_an_unknown_player(self):
        pool = CandidatePool([_player(player_id=1)], [_stats(1, team_id=529)])

        assert pool.stats_for_team(999, 529) == []

    def test_keeps_every_row_for_the_same_team(self):
        pool = CandidatePool(
            [_player(player_id=1)],
            [_stats(1, team_id=529, appearances=5), _stats(1, team_id=529, appearances=7)],
        )

        assert len(pool.stats_for_team(1, 529)) == 2
