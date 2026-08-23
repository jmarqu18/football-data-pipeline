"""Resolution ledger: owns which players are already matched, and what was resolved.

Player resolution runs four passes over the same population (see ADR-004).
Each pass must know who is still available and must record its matches without
disturbing the earlier passes' work. This module owns that state behind one
interface so the passes ask questions instead of mutating sets.

The invariant it protects: an API-Football player and an Understat player are
each matched at most once per run. ``players.api_football_id`` and
``players.understat_id`` are UNIQUE in the CLEAN schema, so a double match used
to surface far from its cause — as a constraint violation midway through the
PostgreSQL insert. Recording a duplicate now raises :class:`DoubleMatchError`
at the pass that caused it.

Building the ``ResolvedPlayer`` record belongs here too: the ledger is the only
writer, so the RAW→CLEAN field conversions (name decoding, birth date parsing)
happen in one place rather than at each call site.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, datetime

from pipeline.models.clean import ResolutionMethod, ResolvedPlayer
from pipeline.models.raw import RawAPIFootballPlayer, RawUnderstatPlayerSeason
from pipeline.name_normalization import decode_api_name

logger = logging.getLogger(__name__)


class DoubleMatchError(ValueError):
    """Raised when a pass tries to match a player that is already matched.

    This is a programming error in a resolution pass, not invalid source data:
    passes are expected to draw candidates from
    :meth:`ResolutionLedger.unmatched_among`, which already excludes them.
    """


def _parse_birth_date(raw_birth_date: str | None, api_player_id: int) -> date | None:
    """Convert an API-Football ISO birth date string into a date.

    API-Football returns birth dates as ISO strings ("2002-11-25"); the CLEAN
    layer stores them as dates. Doing the conversion here keeps the RAW→CLEAN
    transformation visible instead of leaving it to Pydantic coercion.

    A blank or unparseable value yields None rather than raising: one bad record
    must not abort resolution for every other player. Unparseable values are
    logged as WARNING so the bad data stays visible.

    Args:
        raw_birth_date: ISO date string from RawAPIFootballPlayer, or None.
        api_player_id: API-Football player_id, for the warning message.

    Returns:
        The parsed date, or None when the source value is missing or invalid.
    """
    if raw_birth_date is None or not raw_birth_date.strip():
        return None
    try:
        return date.fromisoformat(raw_birth_date)
    except ValueError:
        logger.warning(
            "Invalid birth_date %r for API-Football player %d, storing NULL",
            raw_birth_date,
            api_player_id,
        )
        return None


class ResolutionLedger:
    """Tracks matched players and accumulates the resolved records.

    Constructed once per resolution run. All four passes share one instance,
    and every write to the resolved set goes through it.
    """

    def __init__(self, now: datetime) -> None:
        """Initialize an empty ledger.

        Args:
            now: Timestamp stamped on every record, so a run has a single
                ``resolved_at``. Injected rather than read from the clock to
                keep the output deterministic in tests.
        """
        self._now = now
        self._matched_api: set[int] = set()
        self._matched_understat: set[int] = set()
        self._resolved: list[ResolvedPlayer] = []

    # ── Writes ──

    def record_match(
        self,
        api_player: RawAPIFootballPlayer,
        understat_player: RawUnderstatPlayerSeason,
        confidence: float,
        method: ResolutionMethod,
    ) -> None:
        """Record a cross-source match, marking both players as taken.

        Args:
            api_player: The API-Football side of the match.
            understat_player: The Understat side of the match.
            confidence: Resolution confidence for this pass (see ADR-004).
            method: Which pass produced the match.

        Raises:
            DoubleMatchError: If either player is already matched. Nothing is
                written when this happens.
        """
        self._reject_if_taken(api_player.player_id, understat_player.player_id)

        decoded_name = decode_api_name(api_player.name)
        self._resolved.append(
            ResolvedPlayer(
                canonical_name=decoded_name,
                known_name=(understat_player.player_name if understat_player.player_name != decoded_name else None),
                api_football_id=api_player.player_id,
                understat_id=understat_player.player_id,
                birth_date=_parse_birth_date(api_player.birth_date, api_player.player_id),
                nationality=api_player.nationality,
                photo_url=api_player.photo_url,
                resolution_confidence=confidence,
                resolution_method=method,
                resolved_at=self._now,
            )
        )
        self._matched_api.add(api_player.player_id)
        self._matched_understat.add(understat_player.player_id)

    def record_single_source(self, api_player: RawAPIFootballPlayer) -> None:
        """Record an API-Football player that no pass could match to Understat.

        These still reach the CLEAN ``players`` table — with
        ``resolution_method='unresolved'`` and no confidence — so that
        API-Football-only stats stay linkable downstream.

        Args:
            api_player: The unmatched API-Football player.

        Raises:
            DoubleMatchError: If the player is already matched.
        """
        self._reject_if_taken(api_player.player_id, None)

        self._resolved.append(
            ResolvedPlayer(
                canonical_name=decode_api_name(api_player.name),
                known_name=None,
                api_football_id=api_player.player_id,
                understat_id=None,
                birth_date=_parse_birth_date(api_player.birth_date, api_player.player_id),
                nationality=api_player.nationality,
                photo_url=api_player.photo_url,
                resolution_confidence=None,
                resolution_method="unresolved",
                resolved_at=self._now,
            )
        )
        self._matched_api.add(api_player.player_id)

    # ── Queries ──

    def has_understat(self, understat_id: int) -> bool:
        """Return True if this Understat player has already been matched."""
        return understat_id in self._matched_understat

    def unmatched_among(self, api_ids: Iterable[int]) -> set[int]:
        """Return the subset of ``api_ids`` that is still available.

        The passes use this instead of subtracting sets themselves, so the
        definition of "still available" lives in one place.

        Args:
            api_ids: Candidate API-Football player_ids to filter.

        Returns:
            Those ids not yet matched. Ids the caller did not offer are never
            added.
        """
        return {api_id for api_id in api_ids if api_id not in self._matched_api}

    def resolved_players(self) -> list[ResolvedPlayer]:
        """Return every recorded player, in recording order.

        Returns a copy: callers cannot alter the ledger's state through it.
        """
        return list(self._resolved)

    # ── Internals ──

    def _reject_if_taken(self, api_id: int, understat_id: int | None) -> None:
        """Raise before any write if either side is already matched."""
        if api_id in self._matched_api:
            msg = f"API-Football player {api_id} is already matched"
            raise DoubleMatchError(msg)
        if understat_id is not None and understat_id in self._matched_understat:
            msg = f"Understat player {understat_id} is already matched"
            raise DoubleMatchError(msg)
