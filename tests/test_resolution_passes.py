"""Tests for the four resolution passes, each through its own interface.

Every pass answers one question — is this Understat player this API-Football
candidate? — and returns a Match or None. Nothing is written: recording is the
driver's job, so these tests assert on a returned value rather than on ledger
state.

The pass ordering that consumes these is tested in TestRunPasses below, and
end-to-end in test_entity_resolution.py.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pipeline.candidate_pool import CandidatePool
from pipeline.match_scoring import MatchScorer
from pipeline.models.clean import ResolvedTeam
from pipeline.models.raw import (
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
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
from pipeline.name_normalization import normalize_name
from pipeline.resolution_ledger import ResolutionLedger
from pipeline.resolution_passes import (
    ContextualPass,
    ExactPass,
    FuzzyPass,
    ResolutionSubject,
    StatisticalPass,
)

_TEAM_ID = 529
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


def _player(player_id: int, name: str, firstname: str | None = None, lastname: str | None = None):
    return RawAPIFootballPlayer(player_id=player_id, name=name, firstname=firstname, lastname=lastname)


def _stats(
    player_id: int, team_id: int = _TEAM_ID, position: str | None = None, appearances: int = 0, minutes: int = 0
):
    return RawAPIFootballPlayerStats(
        player_id=player_id,
        team_id=team_id,
        team_name="Test FC",
        league_id=140,
        season=2024,
        games=_APIFootballGames(appearances=appearances, minutes=minutes, position=position),
        **_EMPTY_STATS_KWARGS,
    )


def _understat(
    player_id: int = 900,
    player_name: str = "Pedri",
    team: str = "Barcelona",
    games: int = 0,
    minutes: int = 0,
    position: str | None = None,
) -> RawUnderstatPlayerSeason:
    return RawUnderstatPlayerSeason(
        player_id=player_id,
        player_name=player_name,
        team=team,
        season="2024/2025",
        games=games,
        minutes=minutes,
        goals=0,
        assists=0,
        xg=0.0,
        xa=0.0,
        npxg=0.0,
        xg_chain=0.0,
        xg_buildup=0.0,
        shots=0,
        key_passes=0,
        yellow_cards=0,
        red_cards=0,
        position=position,
    )


def _subject(understat_player: RawUnderstatPlayerSeason, api_team_id: int | None = _TEAM_ID) -> ResolutionSubject:
    return ResolutionSubject(
        understat_player=understat_player,
        normalized_name=normalize_name(understat_player.player_name),
        api_team_id=api_team_id,
    )


def _ledger() -> ResolutionLedger:
    return ResolutionLedger(now=datetime(2026, 8, 23, tzinfo=UTC))


# ─────────────────────────────────────────────────────────────
# Pass 1 — exact name, same team
# ─────────────────────────────────────────────────────────────


class TestExactPass:
    def test_matches_an_identical_normalized_name(self):
        pool = CandidatePool([_player(1, "Pedri")], [_stats(1)])
        result = ExactPass(pool, _ledger()).attempt(_subject(_understat(player_name="Pedri")))

        assert result is not None
        assert result.api_player.player_id == 1
        assert result.confidence == 1.0
        assert result.method == "exact"

    def test_matches_through_a_name_variant(self):
        """Understat's short name can equal the API lastname variant."""
        pool = CandidatePool([_player(1, "Pedro González López", "Pedro", "González López")], [_stats(1)])
        result = ExactPass(pool, _ledger()).attempt(_subject(_understat(player_name="Pedro Gonzalez Lopez")))

        assert result is not None
        assert result.api_player.player_id == 1

    def test_ignores_a_merely_similar_name(self):
        pool = CandidatePool([_player(1, "Pedro González")], [_stats(1)])

        assert ExactPass(pool, _ledger()).attempt(_subject(_understat(player_name="Pedri"))) is None

    def test_skips_a_subject_with_no_resolved_team(self):
        pool = CandidatePool([_player(1, "Pedri")], [_stats(1)])
        subject = _subject(_understat(player_name="Pedri"), api_team_id=None)

        assert ExactPass(pool, _ledger()).attempt(subject) is None

    def test_ignores_a_candidate_from_another_team(self):
        pool = CandidatePool([_player(1, "Pedri")], [_stats(1, team_id=999)])

        assert ExactPass(pool, _ledger()).attempt(_subject(_understat(player_name="Pedri"))) is None

    def test_ignores_a_candidate_already_matched(self):
        pool = CandidatePool([_player(1, "Pedri")], [_stats(1)])
        ledger = _ledger()
        ledger.record_match(_player(1, "Pedri"), _understat(player_id=111), 1.0, "exact")

        assert ExactPass(pool, ledger).attempt(_subject(_understat(player_name="Pedri"))) is None


# ─────────────────────────────────────────────────────────────
# Pass 2 — fuzzy name, same team
# ─────────────────────────────────────────────────────────────


class TestFuzzyPass:
    def _pass(self, pool: CandidatePool, ledger: ResolutionLedger | None = None) -> FuzzyPass:
        return FuzzyPass(pool, MatchScorer(), ledger or _ledger())

    def test_matches_above_the_threshold(self):
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1)])
        result = self._pass(pool).attempt(_subject(_understat(player_name="Robert Lewandowski")))

        assert result is not None
        assert result.confidence == 0.90
        assert result.method == "fuzzy"

    def test_reports_the_score_in_the_detail(self):
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1)])
        result = self._pass(pool).attempt(_subject(_understat(player_name="Robert Lewandowski")))

        assert result is not None
        assert "score" in result.detail

    def test_rejects_a_name_below_the_threshold(self):
        pool = CandidatePool([_player(1, "Antoine Griezmann")], [_stats(1)])

        assert self._pass(pool).attempt(_subject(_understat(player_name="Pedri"))) is None

    def test_two_equally_scoring_candidates_are_ambiguous(self):
        pool = CandidatePool(
            [_player(1, "Carlos Gomez Herrera"), _player(2, "Carlos Gomez Pereira")],
            [_stats(1), _stats(2)],
        )

        assert self._pass(pool).attempt(_subject(_understat(player_name="Carlos Gomez"))) is None

    def test_position_breaks_a_tie(self):
        pool = CandidatePool(
            [_player(1, "Carlos Gomez Herrera"), _player(2, "Carlos Gomez Pereira")],
            [_stats(1, position="Midfielder"), _stats(2, position="Goalkeeper")],
        )
        result = self._pass(pool).attempt(_subject(_understat(player_name="Carlos Gomez", position="M")))

        assert result is not None
        assert result.api_player.player_id == 1
        assert result.confidence == 0.88

    def test_a_tie_between_compatible_positions_stays_ambiguous(self):
        pool = CandidatePool(
            [_player(1, "Carlos Gomez Herrera"), _player(2, "Carlos Gomez Pereira")],
            [_stats(1, position="Midfielder"), _stats(2, position="Midfielder")],
        )

        assert self._pass(pool).attempt(_subject(_understat(player_name="Carlos Gomez", position="M"))) is None

    def test_no_understat_position_means_no_tiebreak(self):
        pool = CandidatePool(
            [_player(1, "Carlos Gomez Herrera"), _player(2, "Carlos Gomez Pereira")],
            [_stats(1, position="Midfielder"), _stats(2, position="Goalkeeper")],
        )

        assert self._pass(pool).attempt(_subject(_understat(player_name="Carlos Gomez", position=None))) is None

    def test_skips_a_subject_with_no_resolved_team(self):
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1)])
        subject = _subject(_understat(player_name="Robert Lewandowski"), api_team_id=None)

        assert self._pass(pool).attempt(subject) is None


# ─────────────────────────────────────────────────────────────
# Pass 3 — cross-team fuzzy, confirmed by transfer history
# ─────────────────────────────────────────────────────────────


def _resolved_team(api_id: int, name: str, understat_name: str | None) -> ResolvedTeam:
    return ResolvedTeam(
        canonical_name=name,
        api_football_id=api_id,
        api_football_name=name,
        understat_name=understat_name,
    )


def _transfer(player_id: int, team_in_id: int, team_out_id: int) -> RawAPIFootballTransfer:
    return RawAPIFootballTransfer(
        player_id=player_id,
        player_name="Robert Lewandowsky",
        team_in_id=team_in_id,
        team_out_id=team_out_id,
    )


class TestContextualPass:
    def _pass(self, pool, transfers, teams, ledger=None) -> ContextualPass:
        return ContextualPass(pool, MatchScorer(), ledger or _ledger(), transfers, teams)

    def test_matches_cross_team_when_a_transfer_confirms_it(self):
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1, team_id=999)])
        teams = [_resolved_team(_TEAM_ID, "Barcelona", "Barcelona")]
        transfers = [_transfer(1, team_in_id=_TEAM_ID, team_out_id=999)]

        result = self._pass(pool, transfers, teams).attempt(
            _subject(_understat(player_name="Robert Lewandowski", team="Barcelona"))
        )

        assert result is not None
        assert result.confidence == 0.70
        assert result.method == "contextual"

    def test_rejects_when_no_transfer_confirms_it(self):
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1, team_id=999)])
        teams = [_resolved_team(_TEAM_ID, "Barcelona", "Barcelona")]

        result = self._pass(pool, [], teams).attempt(
            _subject(_understat(player_name="Robert Lewandowski", team="Barcelona"))
        )

        assert result is None

    def test_does_not_need_a_resolved_team_on_the_subject(self):
        """Pass 3 works cross-team, so a subject with no team_id still applies."""
        pool = CandidatePool([_player(1, "Robert Lewandowsky")], [_stats(1, team_id=999)])
        teams = [_resolved_team(_TEAM_ID, "Barcelona", "Barcelona")]
        transfers = [_transfer(1, team_in_id=_TEAM_ID, team_out_id=999)]

        result = self._pass(pool, transfers, teams).attempt(
            _subject(_understat(player_name="Robert Lewandowski", team="Barcelona"), api_team_id=None)
        )

        assert result is not None


# ─────────────────────────────────────────────────────────────
# Pass 4 — statistical fingerprint, same team
# ─────────────────────────────────────────────────────────────


class TestStatisticalPass:
    def _pass(self, pool: CandidatePool, ledger: ResolutionLedger | None = None) -> StatisticalPass:
        return StatisticalPass(pool, MatchScorer(), ledger or _ledger())

    def test_matches_on_games_and_minutes(self):
        pool = CandidatePool(
            [_player(1, "Jorge Resurreccion Merodio")],
            [_stats(1, appearances=30, minutes=2500, position="Midfielder")],
        )
        subject = _subject(_understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"))
        result = self._pass(pool).attempt(subject)

        assert result is not None
        assert result.confidence == 0.60
        assert result.method == "statistical"

    def test_rejects_when_the_position_is_incompatible(self):
        pool = CandidatePool(
            [_player(1, "Jorge Resurreccion Merodio")],
            [_stats(1, appearances=30, minutes=2500, position="Goalkeeper")],
        )
        subject = _subject(_understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"))

        assert self._pass(pool).attempt(subject) is None

    def test_rejects_when_the_stats_are_far_apart(self):
        pool = CandidatePool(
            [_player(1, "Jorge Resurreccion Merodio")],
            [_stats(1, appearances=5, minutes=200, position="Midfielder")],
        )
        subject = _subject(_understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"))

        assert self._pass(pool).attempt(subject) is None

    def test_rejects_when_the_name_is_below_the_floor(self):
        pool = CandidatePool(
            [_player(1, "Antoine Griezmann")],
            [_stats(1, appearances=30, minutes=2500, position="Midfielder")],
        )
        subject = _subject(_understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"))

        assert self._pass(pool).attempt(subject) is None

    def test_two_statistical_matches_are_ambiguous(self):
        pool = CandidatePool(
            [_player(1, "Jorge Resurreccion Merodio"), _player(2, "Jorge Resurreccion Molina")],
            [
                _stats(1, appearances=30, minutes=2500, position="Midfielder"),
                _stats(2, appearances=30, minutes=2500, position="Midfielder"),
            ],
        )
        subject = _subject(_understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"))

        assert self._pass(pool).attempt(subject) is None

    def test_skips_a_subject_with_no_resolved_team(self):
        pool = CandidatePool(
            [_player(1, "Jorge Resurreccion Merodio")],
            [_stats(1, appearances=30, minutes=2500, position="Midfielder")],
        )
        subject = _subject(
            _understat(player_name="Jorge Resurreccion", games=30, minutes=2500, position="M"),
            api_team_id=None,
        )

        assert self._pass(pool).attempt(subject) is None


# ─────────────────────────────────────────────────────────────
# The driver: pass-major ordering
# ─────────────────────────────────────────────────────────────


class _ScriptedPass:
    """A pass that matches one named subject, recording the order it ran in."""

    def __init__(self, name: str, matches: dict[int, RawAPIFootballPlayer], log: list[str]) -> None:
        self.name = name
        self._matches = matches
        self._log = log

    def attempt(self, subject: ResolutionSubject):
        from pipeline.resolution_passes import Match

        understat_id = subject.understat_player.player_id
        self._log.append(f"{self.name}:{understat_id}")
        api_player = self._matches.get(understat_id)
        if api_player is None:
            return None
        return Match(api_player=api_player, confidence=1.0, method="exact", detail="scripted")


class TestRunPasses:
    """The order is pass-major: every subject goes through pass 1 before pass 2.

    This is load-bearing. Player-major ordering would let a low-confidence
    match on an early player claim a candidate that a later player would have
    matched exactly.
    """

    def test_runs_every_subject_through_a_pass_before_the_next_pass(self):
        from pipeline.entity_resolution import _run_passes

        log: list[str] = []
        subjects = [_subject(_understat(player_id=1)), _subject(_understat(player_id=2))]
        passes = [_ScriptedPass("A", {}, log), _ScriptedPass("B", {}, log)]

        _run_passes(passes, subjects, _ledger())

        assert log == ["A:1", "A:2", "B:1", "B:2"]

    def test_a_matched_subject_is_not_offered_to_later_passes(self):
        from pipeline.entity_resolution import _run_passes

        log: list[str] = []
        subjects = [_subject(_understat(player_id=1)), _subject(_understat(player_id=2))]
        passes = [
            _ScriptedPass("A", {1: _player(10, "Winner")}, log),
            _ScriptedPass("B", {}, log),
        ]

        _run_passes(passes, subjects, _ledger())

        assert log == ["A:1", "A:2", "B:2"]

    def test_records_the_match_in_the_ledger(self):
        from pipeline.entity_resolution import _run_passes

        ledger = _ledger()
        subjects = [_subject(_understat(player_id=1))]
        passes = [_ScriptedPass("A", {1: _player(10, "Winner")}, [])]

        _run_passes(passes, subjects, ledger)

        resolved = ledger.resolved_players()
        assert len(resolved) == 1
        assert resolved[0].api_football_id == 10
        assert resolved[0].understat_id == 1

    def test_a_run_with_no_matches_records_nothing(self):
        from pipeline.entity_resolution import _run_passes

        ledger = _ledger()
        _run_passes([_ScriptedPass("A", {}, [])], [_subject(_understat(player_id=1))], ledger)

        assert ledger.resolved_players() == []
