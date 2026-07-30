"""Tests for candidate match scoring.

Covers the scoring calculation only — name similarity, position compatibility,
statistical fingerprinting and ambiguity detection. The 4-pass resolution
strategy that consumes this scoring is tested in test_entity_resolution.py.
"""

from __future__ import annotations

from pipeline.entity_resolution import build_name_variants, normalize_name
from pipeline.match_scoring import (
    MatchScorer,
    ScoringThresholds,
    _parse_api_position,
    _parse_understat_position,
)
from pipeline.models.raw import (
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


def _make_stats(player_id: int, position: str | None = None) -> RawAPIFootballPlayerStats:
    return RawAPIFootballPlayerStats(
        player_id=player_id,
        team_id=529,
        team_name="Test FC",
        league_id=140,
        season=2024,
        games=_APIFootballGames(appearances=0, minutes=0, position=position),
        **_EMPTY_STATS_KWARGS,
    )


def _scorer(
    api_stats_by_player: dict[int, list[RawAPIFootballPlayerStats]] | None = None,
    thresholds: ScoringThresholds | None = None,
) -> MatchScorer:
    return MatchScorer(api_stats_by_player or {}, thresholds=thresholds)


def _score(understat_name: str, api_variants: list[str]) -> float:
    """Score a raw Understat name, normalizing it first as resolve_players does."""
    return _scorer().fuzzy_score(normalize_name(understat_name), api_variants)


# ─────────────────────────────────────────────────────────────
# Test: fuzzy_score
# ─────────────────────────────────────────────────────────────


class TestFuzzyScore:
    def test_exact_match(self):
        assert _score("Jude Bellingham", ["jude bellingham"]) == 1.0

    def test_partial_ratio_catches_nickname(self):
        # "Pedri" vs "Pedro" — partial_ratio should give a high score
        score = _score("Pedri", ["pedro gonzalez lopez", "pedro", "gonzalez lopez"])
        assert score >= 0.80

    def test_accent_stripping_gives_high_score(self):
        assert _score("Vinicius Junior", ["vinicius junior"]) == 1.0

    def test_empty_inputs(self):
        assert _score("", ["test"]) == 0.0
        assert _score("test", []) == 0.0


# ─────────────────────────────────────────────────────────────
# Test: fuzzy_score length guard
# ─────────────────────────────────────────────────────────────


class TestFuzzyScoreLengthGuard:
    """partial_ratio must not inflate scores when variant is much shorter than target."""

    def test_short_lastname_variant_not_inflated(self):
        """'rodriguez' (lastname variant) must NOT score 1.0 against 'Ricardo Rodríguez'."""
        score = _score("Ricardo Rodríguez", ["rodriguez"])
        # Without guard: partial_ratio gives 1.0 (substring match)
        # With guard: token_sort_ratio gives ~0.69
        assert score < 0.85, f"Short variant 'rodriguez' should not inflate to {score:.3f}"

    def test_short_firstname_variant_not_inflated(self):
        """'david' (firstname variant) must NOT score 1.0 against 'David Alaba'."""
        score = _score("David Alaba", ["david"])
        assert score < 0.85, f"Short variant 'david' should not inflate to {score:.3f}"

    def test_similar_length_variant_still_uses_partial_ratio(self):
        """Variants of similar length should still benefit from partial_ratio."""
        score = _score("Ricardo Rodríguez", ["r. rodriguez"])
        assert score >= 0.85, f"Similar-length variant should still score high, got {score:.3f}"

    def test_nickname_matching_preserved(self):
        """Short nicknames that are genuine matches should still work via token_sort_ratio."""
        score = _score("Pedri", ["pedro gonzalez lopez", "pedro", "gonzalez lopez"])
        assert score >= 0.80, f"Nickname matching should be preserved, got {score:.3f}"

    def test_length_ratio_is_configurable(self):
        """Lowering the guard lets partial_ratio inflate a short variant again."""
        permissive = _scorer(thresholds=ScoringThresholds(partial_ratio_min_length_ratio=0.0))
        score = permissive.fuzzy_score(normalize_name("Ricardo Rodríguez"), ["rodriguez"])
        assert score == 1.0, f"Without the guard, substring match should reach 1.0, got {score:.3f}"


# ─────────────────────────────────────────────────────────────
# Test: false-conflict regression (name scoring)
# ─────────────────────────────────────────────────────────────


class TestFalseConflictRegression:
    """Regression: partial_ratio inflation must not tie a real match with an impostor.

    Before the length guard fix, short name variants (single firstname/lastname)
    scored 1.0 via partial_ratio substring matching, causing two distinct API-Football
    players to both tie at 1.0 against the same Understat player — a false conflict
    that blocked Pass 2 resolution.
    """

    def test_ricardo_rodriguez_outscores_impostor(self):
        """Regression: 'rodriguez' variant of a different player used to score 1.0."""
        # Real Betis player (correct match)
        real_variants = build_name_variants("Ricardo Rodríguez")
        # A different player who happens to have "Rodriguez" in his name
        impostor_variants = build_name_variants("José Rodríguez")

        understat_name = "Ricardo Rodriguez"

        real_score = _score(understat_name, real_variants)
        impostor_score = _score(understat_name, impostor_variants)

        # The real match must score strictly higher than the impostor
        assert real_score > impostor_score, (
            f"Real match (Ricardo Rodríguez: {real_score:.3f}) must outscore "
            f"impostor (José Rodríguez: {impostor_score:.3f})"
        )
        # And the real match must meet the fuzzy threshold
        assert real_score >= ScoringThresholds().player_fuzzy, (
            f"Real match should meet fuzzy threshold, got {real_score:.3f}"
        )

    def test_david_alaba_outscores_impostor(self):
        """Regression: 'david' variant used to score 1.0 when Understat name was 'David Alaba'."""
        real_variants = build_name_variants("David Alaba")
        impostor_variants = build_name_variants("David García")

        understat_name = "David Alaba"

        real_score = _score(understat_name, real_variants)
        impostor_score = _score(understat_name, impostor_variants)

        assert real_score > impostor_score, (
            f"Real match (David Alaba: {real_score:.3f}) must outscore impostor (David García: {impostor_score:.3f})"
        )
        assert real_score >= ScoringThresholds().player_fuzzy, (
            f"Real match should meet fuzzy threshold, got {real_score:.3f}"
        )


# ─────────────────────────────────────────────────────────────
# Test: Pass 4 name floor
# ─────────────────────────────────────────────────────────────


def test_pass4_floor_allows_plausible_and_blocks_unrelated() -> None:
    """Verify the Pass 4 name floor allows plausible names and blocks unrelated ones.

    Tests the threshold value directly through the scorer, since any realistic
    name pair with a shared surname resolves in Pass 1 or 2 before reaching
    Pass 4 (making end-to-end regression testing vacuous).

    Plausible pair: 'Gimenez' (Understat) vs 'J. Gimenez' (API-Football).
    These share the same underlying identity and score above the floor.

    Unrelated pair: 'Connor Gallagher' vs 'J. Gimenez' (the false positive
    this floor is designed to prevent). Score is well below the floor.
    """
    floor = ScoringThresholds().pass4_name_floor
    gimenez_variants = build_name_variants("J. Gimenez", "Jose Maria", "Gimenez de Vargas")

    # Plausible: same player, just abbreviated name → must be above floor
    plausible_score = _score("Gimenez", gimenez_variants)
    assert plausible_score >= floor, (
        f"'Gimenez' should pass the name floor against 'J. Gimenez' variants, got {plausible_score:.3f}"
    )

    # Unrelated: completely different player → must be below floor
    gallagher_score = _score("Connor Gallagher", gimenez_variants)
    assert gallagher_score < floor, f"'Connor Gallagher' should be blocked by name floor, got {gallagher_score:.3f}"


# ─────────────────────────────────────────────────────────────
# Test: position parsing and compatibility
# ─────────────────────────────────────────────────────────────


class TestPositionMapping:
    """Position parsing and compatibility checks."""

    def test_understat_midfielder_striker(self):
        assert _parse_understat_position("M S") == {"M", "F"}

    def test_understat_defender_midfielder(self):
        assert _parse_understat_position("D M S") == {"D", "M", "F"}

    def test_understat_goalkeeper(self):
        assert _parse_understat_position("G") == {"G"}

    def test_understat_none_returns_empty(self):
        assert _parse_understat_position(None) == set()

    def test_understat_unknown_code_ignored(self):
        assert _parse_understat_position("X Y G") == {"G"}

    def test_api_midfielder(self):
        assert _parse_api_position("Midfielder") == {"M"}

    def test_api_goalkeeper(self):
        assert _parse_api_position("Goalkeeper") == {"G"}

    def test_api_none_returns_empty(self):
        assert _parse_api_position(None) == set()

    def test_api_unknown_returns_empty(self):
        assert _parse_api_position("Unknown") == set()

    def test_compatible_midfielder_vs_ms(self):
        assert _scorer().positions_compatible("M S", "Midfielder") is True

    def test_compatible_striker_vs_attacker(self):
        assert _scorer().positions_compatible("F S", "Attacker") is True

    def test_incompatible_goalkeeper_vs_midfielder(self):
        assert _scorer().positions_compatible("G", "Midfielder") is False

    def test_incompatible_defender_vs_attacker(self):
        assert _scorer().positions_compatible("D", "Attacker") is False

    def test_none_understat_always_compatible(self):
        assert _scorer().positions_compatible(None, "Defender") is True

    def test_none_api_always_compatible(self):
        assert _scorer().positions_compatible("D", None) is True


# ─────────────────────────────────────────────────────────────
# Test: position_of
# ─────────────────────────────────────────────────────────────


class TestPositionOf:
    """Position lookup from the season-stats index."""

    def test_returns_position_from_stats(self):
        scorer = _scorer({1: [_make_stats(1, position="Midfielder")]})
        assert scorer.position_of(1) == "Midfielder"

    def test_returns_first_non_empty_position(self):
        scorer = _scorer({1: [_make_stats(1, position=None), _make_stats(1, position="Defender")]})
        assert scorer.position_of(1) == "Defender"

    def test_unknown_player_returns_none(self):
        assert _scorer().position_of(999) is None

    def test_player_without_position_returns_none(self):
        scorer = _scorer({1: [_make_stats(1, position=None)]})
        assert scorer.position_of(1) is None


# ─────────────────────────────────────────────────────────────
# Test: filter_by_position
# ─────────────────────────────────────────────────────────────


class TestFilterByPosition:
    """The shared filter used to break Pass 2 ties and veto Pass 4 matches."""

    def test_keeps_only_compatible_candidates(self):
        scorer = _scorer(
            {
                1: [_make_stats(1, position="Goalkeeper")],
                2: [_make_stats(2, position="Attacker")],
            }
        )
        assert scorer.filter_by_position([1, 2], "F S") == [2]

    def test_keeps_candidates_with_unknown_position(self):
        scorer = _scorer({1: [_make_stats(1, position=None)]})
        assert scorer.filter_by_position([1], "G") == [1]

    def test_unknown_understat_position_keeps_all(self):
        scorer = _scorer(
            {
                1: [_make_stats(1, position="Goalkeeper")],
                2: [_make_stats(2, position="Attacker")],
            }
        )
        assert scorer.filter_by_position([1, 2], None) == [1, 2]

    def test_preserves_input_order(self):
        scorer = _scorer(
            {
                1: [_make_stats(1, position="Defender")],
                2: [_make_stats(2, position="Defender")],
            }
        )
        assert scorer.filter_by_position([2, 1], "D") == [2, 1]

    def test_empty_when_nothing_compatible(self):
        scorer = _scorer({1: [_make_stats(1, position="Goalkeeper")]})
        assert scorer.filter_by_position([1], "F") == []


# ─────────────────────────────────────────────────────────────
# Test: has_conflict
# ─────────────────────────────────────────────────────────────


class TestHasConflict:
    def test_single_score_is_never_ambiguous(self):
        assert _scorer().has_conflict([0.9]) is False

    def test_close_scores_conflict(self):
        assert _scorer().has_conflict([0.90, 0.88]) is True

    def test_clear_winner_does_not_conflict(self):
        assert _scorer().has_conflict([0.95, 0.60]) is False

    def test_threshold_is_configurable(self):
        strict = _scorer(thresholds=ScoringThresholds(conflict=0.40))
        assert strict.has_conflict([0.95, 0.60]) is True


# ─────────────────────────────────────────────────────────────
# Test: stats_match
# ─────────────────────────────────────────────────────────────


class TestStatsMatch:
    def test_identical_stats_match(self):
        assert _scorer().stats_match(24, 2050, 24, 2050) is True

    def test_within_tolerance_matches(self):
        # 3 games apart, minutes within 20%
        assert _scorer().stats_match(24, 2050, 27, 1900) is True

    def test_games_outside_tolerance_rejected(self):
        assert _scorer().stats_match(24, 2050, 30, 2050) is False

    def test_minutes_outside_tolerance_rejected(self):
        assert _scorer().stats_match(24, 2050, 24, 1000) is False

    def test_missing_api_stats_rejected(self):
        assert _scorer().stats_match(None, 2050, 24, 2050) is False
        assert _scorer().stats_match(24, None, 24, 2050) is False

    def test_zero_minutes_both_sides_matches(self):
        assert _scorer().stats_match(0, 0, 0, 0) is True

    def test_tolerances_are_configurable(self):
        lenient = _scorer(thresholds=ScoringThresholds(stat_games_tolerance=10, stat_minutes_tolerance_pct=0.60))
        assert lenient.stats_match(24, 2050, 30, 1000) is True
