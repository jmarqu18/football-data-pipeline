"""Tests for ResolutionLedger — the owner of player matching state.

The ledger is what makes "an API-Football player is matched at most once"
an invariant rather than a convention. Before it existed, the four passes
threaded three mutable structures by hand, and a forgotten update surfaced
much later as a UNIQUE violation on players.api_football_id.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest

from pipeline.models.raw import RawAPIFootballPlayer, RawUnderstatPlayerSeason
from pipeline.resolution_ledger import DoubleMatchError, ResolutionLedger

_NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=UTC)


def _api_player(
    player_id: int = 100,
    name: str = "Pedro González López",
    birth_date: str | None = "2002-11-25",
    **kwargs,
) -> RawAPIFootballPlayer:
    """Build a RawAPIFootballPlayer with sensible defaults."""
    return RawAPIFootballPlayer(
        player_id=player_id,
        name=name,
        birth_date=birth_date,
        nationality=kwargs.pop("nationality", "Spain"),
        photo_url=kwargs.pop("photo_url", "https://example.test/100.png"),
        **kwargs,
    )


def _understat_player(
    player_id: int = 8872,
    player_name: str = "Pedri",
    team: str = "Barcelona",
) -> RawUnderstatPlayerSeason:
    """Build a RawUnderstatPlayerSeason with zeroed stats."""
    return RawUnderstatPlayerSeason(
        player_id=player_id,
        player_name=player_name,
        team=team,
        season="2024/2025",
        games=0,
        minutes=0,
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
    )


# ─────────────────────────────────────────────────────────────
# Recording a cross-source match
# ─────────────────────────────────────────────────────────────


class TestRecordMatch:
    def test_carries_both_source_ids(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(player_id=8872), 1.0, "exact")

        player = ledger.resolved_players()[0]
        assert player.api_football_id == 100
        assert player.understat_id == 8872

    def test_carries_confidence_and_method(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(), _understat_player(), 0.88, "fuzzy")

        player = ledger.resolved_players()[0]
        assert player.resolution_confidence == 0.88
        assert player.resolution_method == "fuzzy"

    def test_stamps_the_injected_run_timestamp(self):
        """resolved_at comes from the ledger's `now`, so a run has one timestamp."""
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(), _understat_player(), 1.0, "exact")

        assert ledger.resolved_players()[0].resolved_at == _NOW

    def test_decodes_html_entities_in_canonical_name(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(name="E. Eto&apos;o Pineda"), _understat_player(), 1.0, "exact")

        assert ledger.resolved_players()[0].canonical_name == "E. Eto'o Pineda"

    def test_keeps_understat_name_as_known_name_when_it_differs(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(
            _api_player(name="Pedro González López"),
            _understat_player(player_name="Pedri"),
            1.0,
            "exact",
        )

        assert ledger.resolved_players()[0].known_name == "Pedri"

    def test_leaves_known_name_none_when_names_agree(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(
            _api_player(name="Robert Lewandowski"),
            _understat_player(player_name="Robert Lewandowski"),
            1.0,
            "exact",
        )

        assert ledger.resolved_players()[0].known_name is None


# ─────────────────────────────────────────────────────────────
# birth_date RAW→CLEAN conversion (absorbed from entity_resolution)
# ─────────────────────────────────────────────────────────────


class TestBirthDateConversion:
    def test_parses_iso_string_into_a_date(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(birth_date="2002-11-25"), _understat_player(), 1.0, "exact")

        assert ledger.resolved_players()[0].birth_date == datetime(2002, 11, 25).date()

    def test_missing_birth_date_stays_none(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(birth_date=None), _understat_player(), 1.0, "exact")

        assert ledger.resolved_players()[0].birth_date is None

    def test_blank_birth_date_is_treated_as_missing(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(birth_date="   "), _understat_player(), 1.0, "exact")

        assert ledger.resolved_players()[0].birth_date is None

    def test_malformed_birth_date_is_dropped_and_logged(self, caplog):
        """One bad date must not abort the run — it is logged and stored as NULL."""
        ledger = ResolutionLedger(now=_NOW)
        with caplog.at_level(logging.WARNING):
            ledger.record_match(
                _api_player(player_id=777, birth_date="not-a-date"),
                _understat_player(),
                1.0,
                "exact",
            )

        assert ledger.resolved_players()[0].birth_date is None
        assert "777" in caplog.text


# ─────────────────────────────────────────────────────────────
# Single-source players (API-Football only)
# ─────────────────────────────────────────────────────────────


class TestRecordSingleSource:
    def test_is_marked_unresolved_with_no_confidence(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_single_source(_api_player(player_id=100))

        player = ledger.resolved_players()[0]
        assert player.resolution_method == "unresolved"
        assert player.resolution_confidence is None

    def test_has_no_understat_id_and_no_known_name(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_single_source(_api_player(player_id=100))

        player = ledger.resolved_players()[0]
        assert player.understat_id is None
        assert player.known_name is None

    def test_still_carries_the_biographical_fields(self):
        """Single-source players reach the players table too — the data must survive."""
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_single_source(_api_player(player_id=100, name="Robert Lewandowski"))

        player = ledger.resolved_players()[0]
        assert player.canonical_name == "Robert Lewandowski"
        assert player.birth_date == datetime(2002, 11, 25).date()
        assert player.nationality == "Spain"
        assert player.resolved_at == _NOW

    def test_marks_the_player_as_taken(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_single_source(_api_player(player_id=100))

        assert ledger.unmatched_among([100, 200]) == {200}


# ─────────────────────────────────────────────────────────────
# The invariant: nobody is matched twice
# ─────────────────────────────────────────────────────────────


class TestDoubleMatchIsRejected:
    def test_rejects_reusing_an_api_player(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(player_id=1), 1.0, "exact")

        with pytest.raises(DoubleMatchError, match="100"):
            ledger.record_match(_api_player(player_id=100), _understat_player(player_id=2), 0.9, "fuzzy")

    def test_rejects_reusing_an_understat_player(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(player_id=8872), 1.0, "exact")

        with pytest.raises(DoubleMatchError, match="8872"):
            ledger.record_match(_api_player(player_id=200), _understat_player(player_id=8872), 0.9, "fuzzy")

    def test_rejects_single_source_for_an_already_matched_player(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(), 1.0, "exact")

        with pytest.raises(DoubleMatchError, match="100"):
            ledger.record_single_source(_api_player(player_id=100))

    def test_a_rejected_match_leaves_no_trace(self):
        """The raise happens before anything is written — no half-recorded state."""
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(player_id=1), 1.0, "exact")

        with pytest.raises(DoubleMatchError):
            ledger.record_match(_api_player(player_id=100), _understat_player(player_id=2), 0.9, "fuzzy")

        assert len(ledger.resolved_players()) == 1
        assert not ledger.has_understat(2)


# ─────────────────────────────────────────────────────────────
# Membership queries — the passes never do set arithmetic
# ─────────────────────────────────────────────────────────────


class TestMembershipQueries:
    def test_has_understat_is_false_before_recording(self):
        ledger = ResolutionLedger(now=_NOW)

        assert not ledger.has_understat(8872)

    def test_has_understat_is_true_after_recording(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(), _understat_player(player_id=8872), 1.0, "exact")

        assert ledger.has_understat(8872)

    def test_unmatched_among_returns_everything_when_nothing_recorded(self):
        ledger = ResolutionLedger(now=_NOW)

        assert ledger.unmatched_among([1, 2, 3]) == {1, 2, 3}

    def test_unmatched_among_excludes_matched_players(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=2), _understat_player(), 1.0, "exact")

        assert ledger.unmatched_among([1, 2, 3]) == {1, 3}

    def test_unmatched_among_never_invents_ids(self):
        """Only ids the caller offered come back, matched or not."""
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=99), _understat_player(), 1.0, "exact")

        assert ledger.unmatched_among([1, 2]) == {1, 2}


# ─────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────


class TestResolvedPlayers:
    def test_is_empty_for_a_fresh_ledger(self):
        assert ResolutionLedger(now=_NOW).resolved_players() == []

    def test_returns_matches_and_single_source_in_recording_order(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(player_id=100), _understat_player(player_id=1), 1.0, "exact")
        ledger.record_single_source(_api_player(player_id=200))
        ledger.record_match(_api_player(player_id=300), _understat_player(player_id=3), 0.9, "fuzzy")

        assert [p.api_football_id for p in ledger.resolved_players()] == [100, 200, 300]

    def test_mutating_the_returned_list_does_not_affect_the_ledger(self):
        ledger = ResolutionLedger(now=_NOW)
        ledger.record_match(_api_player(), _understat_player(), 1.0, "exact")

        ledger.resolved_players().clear()

        assert len(ledger.resolved_players()) == 1
