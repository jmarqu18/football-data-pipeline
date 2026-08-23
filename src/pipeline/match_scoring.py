"""Match scoring: similarity calculation between API-Football and Understat candidates.

This module answers a single question — given an Understat player and an
API-Football candidate, how well do they fit? — behind one interface.
It holds no resolution strategy: the pass ordering, the eligibility rules and
the confidence values live in ``pipeline.entity_resolution`` (see ADR-004).

It compares prepared values and looks nothing up. Name preparation lives in
``pipeline.name_normalization``; resolving an id to a position or a stats row
lives in ``pipeline.candidate_pool``.

See docs/entity-resolution-spec.md for the full design specification.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from rapidfuzz import fuzz


class ScoringThresholds(BaseModel):
    """Tuning constants for candidate scoring.

    Defaults are the production values documented in ADR-004. Tests can
    construct a variant to exercise threshold-boundary behaviour without
    reaching into module internals.
    """

    model_config = ConfigDict(frozen=True)

    player_fuzzy: float = Field(default=0.85, ge=0.0, le=1.0)
    """Minimum name score for a same-team fuzzy match (Pass 2)."""

    cross_team_fuzzy: float = Field(default=0.75, ge=0.0, le=1.0)
    """Minimum name score for a cross-team contextual match (Pass 3)."""

    conflict: float = Field(default=0.05, ge=0.0, le=1.0)
    """Maximum gap between the top two scores before they count as ambiguous."""

    stat_games_tolerance: int = Field(default=3, ge=0)
    """Maximum absolute difference in appearances for a statistical match (Pass 4)."""

    stat_minutes_tolerance_pct: float = Field(default=0.20, ge=0.0, le=1.0)
    """Maximum relative difference in minutes for a statistical match (Pass 4)."""

    pass4_name_floor: float = Field(default=0.50, ge=0.0, le=1.0)
    """Minimum name score required before statistical matching is considered (Pass 4)."""

    partial_ratio_min_length_ratio: float = Field(default=0.6, ge=0.0, le=1.0)
    """Skip partial_ratio when the shorter string is below this fraction of the longer."""


# ─────────────────────────────────────────────────────────────
# Position parsing
# ─────────────────────────────────────────────────────────────

# Understat position strings contain space-separated role codes: "M S", "D M S", etc.
# Map each code to a canonical bucket.
_UNDERSTAT_POSITION_MAP: dict[str, str] = {
    "G": "G",  # Goalkeeper
    "D": "D",  # Defender
    "M": "M",  # Midfielder
    "F": "F",  # Forward/Attacker
    "S": "F",  # Striker → Forward bucket
    "A": "F",  # Attacker → Forward bucket
}

# API-Football position strings → canonical bucket
_API_POSITION_MAP: dict[str, str] = {
    "Goalkeeper": "G",
    "Defender": "D",
    "Midfielder": "M",
    "Attacker": "F",
    "Forward": "F",
}


def _parse_understat_position(position: str | None) -> set[str]:
    """Parse an Understat composite position string into canonical buckets.

    Understat positions are space-separated codes, e.g. "M S", "D M S".
    Returns an empty set if position is None or unrecognized.

    Args:
        position: Understat position string like "M S" or None.

    Returns:
        Set of canonical position codes (subset of {"G", "D", "M", "F"}).
    """
    if not position:
        return set()
    return {_UNDERSTAT_POSITION_MAP[code] for code in position.upper().split() if code in _UNDERSTAT_POSITION_MAP}


def _parse_api_position(position: str | None) -> set[str]:
    """Parse an API-Football position string into canonical buckets.

    Args:
        position: API-Football position string like "Midfielder" or None.

    Returns:
        Set with one canonical code, or empty set if unrecognized/None.
    """
    if not position:
        return set()
    canonical = _API_POSITION_MAP.get(position)
    return {canonical} if canonical else set()


# ─────────────────────────────────────────────────────────────
# Scorer
# ─────────────────────────────────────────────────────────────


class MatchScorer:
    """Scores API-Football candidates against an Understat player.

    Stateless apart from its thresholds: every method compares values the
    caller has already looked up. Resolving an id to a name, a position or a
    stats row is ``pipeline.candidate_pool``'s job.
    """

    def __init__(self, thresholds: ScoringThresholds | None = None) -> None:
        """Initialize the scorer.

        Args:
            thresholds: Tuning constants; production defaults when omitted.
        """
        self.thresholds = thresholds or ScoringThresholds()

    def fuzzy_score(self, understat_name: str, api_variants: list[str]) -> float:
        """Return the best fuzzy match score between an Understat name and API-Football variants.

        Uses the maximum of token_sort_ratio and partial_ratio across all variants.
        partial_ratio is only considered when the shorter string is at least
        ``thresholds.partial_ratio_min_length_ratio`` of the longer string's length,
        preventing inflated scores from substring matches (e.g. variant "rodriguez"
        scoring 1.0 against "ricardo rodriguez").

        Args:
            understat_name: Understat player name, already normalized by
                ``entity_resolution.normalize_name``.
            api_variants: Normalized API-Football name variants.

        Returns:
            A value in [0.0, 1.0].
        """
        if not understat_name or not api_variants:
            return 0.0
        min_length_ratio = self.thresholds.partial_ratio_min_length_ratio
        best = 0.0
        for variant in api_variants:
            if not variant:
                continue
            score_token = fuzz.token_sort_ratio(understat_name, variant)
            shorter = min(len(understat_name), len(variant))
            longer = max(len(understat_name), len(variant))
            if longer > 0 and (shorter / longer) >= min_length_ratio:
                score_partial = fuzz.partial_ratio(understat_name, variant)
            else:
                score_partial = 0.0
            best = max(best, score_token, score_partial)
        return best / 100.0

    def has_conflict(self, scores: list[float]) -> bool:
        """Return True if the top two scores are too close, indicating ambiguity."""
        if len(scores) < 2:
            return False
        sorted_scores = sorted(scores, reverse=True)
        return (sorted_scores[0] - sorted_scores[1]) < self.thresholds.conflict

    def positions_compatible(self, understat_pos: str | None, api_pos: str | None) -> bool:
        """Return True if two position strings are compatible (share at least one bucket).

        If either position is unknown/None, returns True (no information = no penalty).

        Args:
            understat_pos: Understat position string (e.g. "M S").
            api_pos: API-Football position string (e.g. "Midfielder").

        Returns:
            True if compatible or if either is unknown.
        """
        us_buckets = _parse_understat_position(understat_pos)
        api_buckets = _parse_api_position(api_pos)
        if not us_buckets or not api_buckets:
            return True  # unknown position → no penalty
        return bool(us_buckets & api_buckets)

    def stats_match(
        self,
        api_appearances: int | None,
        api_minutes: int | None,
        understat_games: int,
        understat_minutes: int,
    ) -> bool:
        """Return True if two players have similar enough game/minute stats.

        Args:
            api_appearances: API-Football appearances, may be None.
            api_minutes: API-Football minutes, may be None.
            understat_games: Understat games played.
            understat_minutes: Understat minutes played.

        Returns:
            True when both appearances and minutes fall within tolerance.
        """
        if api_appearances is None or api_minutes is None:
            return False
        if abs(api_appearances - understat_games) > self.thresholds.stat_games_tolerance:
            return False
        if understat_minutes == 0 and api_minutes == 0:
            return True
        max_minutes = max(api_minutes, understat_minutes)
        if max_minutes == 0:
            return False
        minutes_diff = abs(api_minutes - understat_minutes) / max_minutes
        return minutes_diff <= self.thresholds.stat_minutes_tolerance_pct
