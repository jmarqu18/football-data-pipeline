"""The four resolution passes, each behind one interface.

Player resolution progresses from high-confidence exact matches to
low-confidence statistical fingerprinting (ADR-004). Each pass answers one
question — is this Understat player this API-Football candidate? — and returns
a :class:`Match` or ``None``.

A pass decides; it does not record. Writing to the ledger belongs to the driver
in ``pipeline.entity_resolution``, which keeps a single caller per kind of
write and lets a pass be tested on its return value alone. Passes do read the
ledger, to skip candidates an earlier pass already claimed.

The order in which the passes run, and the confidence each one carries, are
part of the strategy and live with the driver.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from pipeline.candidate_pool import CandidatePool
from pipeline.match_scoring import MatchScorer
from pipeline.models.clean import ResolutionMethod, ResolvedTeam
from pipeline.models.raw import RawAPIFootballPlayer, RawAPIFootballTransfer, RawUnderstatPlayerSeason
from pipeline.name_normalization import normalize_name
from pipeline.resolution_ledger import ResolutionLedger

logger = logging.getLogger(__name__)


class ResolutionSubject(BaseModel):
    """An Understat player awaiting an API-Football identity.

    Carries the values every pass would otherwise recompute: the normalized
    name, and the API-Football team_id the player's Understat club resolved to.
    """

    model_config = ConfigDict(frozen=True)

    understat_player: RawUnderstatPlayerSeason
    normalized_name: str
    api_team_id: int | None = None
    """None when the Understat club never resolved. Passes that reduce
    candidates by team skip such subjects; Pass 3 works cross-team and does not."""


class Match(BaseModel):
    """A pass's decision that two records are the same player."""

    model_config = ConfigDict(frozen=True)

    api_player: RawAPIFootballPlayer
    confidence: float
    method: ResolutionMethod
    detail: str = ""
    """Pass-specific diagnostics for the driver's log line, e.g. "score=0.903"."""


class ResolutionPass(Protocol):
    """One attempt at identifying an Understat player."""

    name: str
    """Label for the driver's log line, e.g. "Pass 2 fuzzy"."""

    def attempt(self, subject: ResolutionSubject) -> Match | None:
        """Return a match for this subject, or None to defer to a later pass."""
        ...


# ─────────────────────────────────────────────────────────────
# Pass 1 — exact name, same team
# ─────────────────────────────────────────────────────────────


class ExactPass:
    """Matches when the normalized Understat name equals an API name variant.

    Needs no scorer: the comparison is equality, not similarity.
    """

    name = "Pass 1 exact"

    def __init__(self, pool: CandidatePool, ledger: ResolutionLedger) -> None:
        self._pool = pool
        self._ledger = ledger

    def attempt(self, subject: ResolutionSubject) -> Match | None:
        """Return the first same-team candidate whose variants contain the name."""
        if subject.api_team_id is None:
            return None

        for api_id in self._ledger.unmatched_among(self._pool.in_team(subject.api_team_id)):
            if subject.normalized_name in self._pool.variants(api_id):
                return Match(
                    api_player=self._pool.player(api_id),
                    confidence=1.0,
                    method="exact",
                    detail=f"team={subject.understat_player.team}",
                )
        return None


# ─────────────────────────────────────────────────────────────
# Pass 2 — fuzzy name, same team
# ─────────────────────────────────────────────────────────────


class FuzzyPass:
    """Matches the best-scoring same-team candidate above the fuzzy threshold.

    When several candidates score within the conflict threshold of each other,
    the match is ambiguous and only a position tiebreak can settle it.
    """

    name = "Pass 2 fuzzy"

    def __init__(self, pool: CandidatePool, scorer: MatchScorer, ledger: ResolutionLedger) -> None:
        self._pool = pool
        self._scorer = scorer
        self._ledger = ledger

    def attempt(self, subject: ResolutionSubject) -> Match | None:
        """Return the best same-team candidate, or the position tiebreak winner."""
        if subject.api_team_id is None:
            return None

        scores_by_id: dict[int, float] = {
            api_id: self._scorer.fuzzy_score(subject.normalized_name, self._pool.variants(api_id))
            for api_id in self._ledger.unmatched_among(self._pool.in_team(subject.api_team_id))
        }
        if not scores_by_id:
            return None

        best_api_id = max(scores_by_id, key=lambda api_id: scores_by_id[api_id])
        best_score = scores_by_id[best_api_id]
        if best_score < self._scorer.thresholds.player_fuzzy:
            return None

        if not self._scorer.has_conflict(list(scores_by_id.values())):
            return Match(
                api_player=self._pool.player(best_api_id),
                confidence=0.90,
                method="fuzzy",
                detail=f"score={best_score:.3f}, team={subject.understat_player.team}",
            )

        return self._break_tie_by_position(subject, scores_by_id, best_score)

    def _break_tie_by_position(
        self,
        subject: ResolutionSubject,
        scores_by_id: dict[int, float],
        best_score: float,
    ) -> Match | None:
        """Settle an ambiguous score by position, when exactly one candidate fits."""
        understat_position = subject.understat_player.position
        if not understat_position:
            return None

        contenders = [
            api_id
            for api_id, score in scores_by_id.items()
            if score >= self._scorer.thresholds.player_fuzzy and (best_score - score) < self._scorer.thresholds.conflict
        ]
        compatible = [
            api_id
            for api_id in contenders
            if self._scorer.positions_compatible(understat_position, self._pool.position_of(api_id))
        ]
        if len(compatible) != 1:
            return None

        return Match(
            api_player=self._pool.player(compatible[0]),
            confidence=0.88,
            method="fuzzy",
            detail=f"score={best_score:.3f}, position tiebreak on {understat_position}",
        )


# ─────────────────────────────────────────────────────────────
# Pass 3 — cross-team fuzzy, confirmed by transfer history
# ─────────────────────────────────────────────────────────────


class ContextualPass:
    """Matches across teams when transfer history explains the discrepancy.

    The only pass that looks outside the subject's club, and the only one that
    needs the raw transfer records to do it.
    """

    name = "Pass 3 contextual"

    def __init__(
        self,
        pool: CandidatePool,
        scorer: MatchScorer,
        ledger: ResolutionLedger,
        raw_transfers: Sequence[RawAPIFootballTransfer],
        resolved_teams: Sequence[ResolvedTeam],
    ) -> None:
        self._pool = pool
        self._scorer = scorer
        self._ledger = ledger
        self._transfers = raw_transfers
        self._resolved_teams = resolved_teams

    def attempt(self, subject: ResolutionSubject) -> Match | None:
        """Return the best unmatched candidate whose transfers reach this club."""
        scores_by_id: dict[int, float] = {
            api_id: self._scorer.fuzzy_score(subject.normalized_name, self._pool.variants(api_id))
            for api_id in self._ledger.unmatched_among(self._pool.all_ids())
        }
        if not scores_by_id:
            return None

        best_api_id = max(scores_by_id, key=lambda api_id: scores_by_id[api_id])
        best_score = scores_by_id[best_api_id]
        if best_score < self._scorer.thresholds.cross_team_fuzzy:
            return None
        if self._scorer.has_conflict(list(scores_by_id.values())):
            return None
        if not self._transfer_confirms(best_api_id, subject.understat_player.team):
            return None

        return Match(
            api_player=self._pool.player(best_api_id),
            confidence=0.70,
            method="contextual",
            detail=f"score={best_score:.3f}, transfer confirmed",
        )

    def _transfer_confirms(self, api_player_id: int, understat_team_name: str) -> bool:
        """Check whether transfer history links a player to an Understat club."""
        norm_u_team = normalize_name(understat_team_name)

        team_ids = {
            team.api_football_id
            for team in self._resolved_teams
            if team.understat_name and normalize_name(team.understat_name) == norm_u_team
        }

        for transfer in self._transfers:
            if transfer.player_id != api_player_id:
                continue
            if transfer.team_in_id in team_ids or transfer.team_out_id in team_ids:
                return True
            # Also check by name if IDs don't match
            if transfer.team_in_name and normalize_name(transfer.team_in_name) == norm_u_team:
                return True
            if transfer.team_out_name and normalize_name(transfer.team_out_name) == norm_u_team:
                return True
        return False


# ─────────────────────────────────────────────────────────────
# Pass 4 — statistical fingerprint, same team
# ─────────────────────────────────────────────────────────────


class StatisticalPass:
    """Matches on appearances and minutes when the names barely agree.

    Catches nicknames with no phonetic overlap ("Koke" ↔ "Jorge Resurrección
    Merodio"). Requires a unique statistical candidate and a compatible
    position, since the name carries almost no signal here.
    """

    name = "Pass 4 statistical"

    def __init__(self, pool: CandidatePool, scorer: MatchScorer, ledger: ResolutionLedger) -> None:
        self._pool = pool
        self._scorer = scorer
        self._ledger = ledger

    def attempt(self, subject: ResolutionSubject) -> Match | None:
        """Return the sole same-team candidate whose season stats line up."""
        if subject.api_team_id is None:
            return None

        stat_matches = [
            api_id
            for api_id in self._ledger.unmatched_among(self._pool.in_team(subject.api_team_id))
            if self._fingerprint_matches(api_id, subject)
        ]

        if len(stat_matches) > 1:
            logger.debug(
                "Pass 4 conflict: '%s' has %d stat matches in team, skipping",
                subject.understat_player.player_name,
                len(stat_matches),
            )
            return None
        if not stat_matches:
            return None

        api_id = stat_matches[0]
        api_position = self._pool.position_of(api_id)
        api_player = self._pool.player(api_id)
        if not self._scorer.positions_compatible(subject.understat_player.position, api_position):
            logger.debug(
                "Pass 4 statistical rejected (position mismatch): '%s' (pos=%s) ↔ '%s' (pos=%s)",
                subject.understat_player.player_name,
                subject.understat_player.position,
                api_player.name,
                api_position,
            )
            return None

        understat = subject.understat_player
        return Match(
            api_player=api_player,
            confidence=0.60,
            method="statistical",
            detail=f"team={understat.team}, games={understat.games}, minutes={understat.minutes}",
        )

    def _fingerprint_matches(self, api_id: int, subject: ResolutionSubject) -> bool:
        """Check the name floor, then whether any club stats row lines up."""
        if subject.api_team_id is None:
            return False
        name_score = self._scorer.fuzzy_score(subject.normalized_name, self._pool.variants(api_id))
        if name_score < self._scorer.thresholds.pass4_name_floor:
            return False

        understat = subject.understat_player
        return any(
            self._scorer.stats_match(
                stat.games.appearances,
                stat.games.minutes,
                understat.games,
                understat.minutes,
            )
            for stat in self._pool.stats_for_team(api_id, subject.api_team_id)
        )
