"""Unit tests for RAW layer Pydantic models.

Each test uses realistic data modelled from the actual sources
(API-Football, Understat) to validate schema and basic types.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.models.raw import (
    RawAPIFootballInjury,
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballStandings,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
    RawUnderstatShot,
    _APIFootballGames,
    _APIFootballGoals,
    _APIFootballPasses,
    _APIFootballPenalty,
    _APIFootballShots,
)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> list[dict]:
    """Load a JSON fixture file and return its contents."""
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


# Reusable dicts for building valid sub-models inline.
_VALID_GAMES = dict(
    appearances=28,
    lineups=25,
    minutes=2100,
    number=8,
    position="Midfielder",
    rating="7.342857",
    captain=False,
)
_VALID_SHOTS = dict(total=32, on=14)
_VALID_GOALS = dict(total=5, conceded=0, assists=7, saves=None)
_VALID_PASSES = dict(total=1850, key=42, accuracy=89)
_VALID_TACKLES = dict(total=35, blocks=3, interceptions=22)
_VALID_DUELS = dict(total=195, won=105)
_VALID_DRIBBLES = dict(attempts=55, success=38, past=None)
_VALID_FOULS = dict(drawn=30, committed=15)
_VALID_CARDS = dict(yellow=4, yellowred=0, red=0)
_VALID_PENALTY = dict(won=None, committed=None, scored=0, missed=0, saved=None)

_VALID_PLAYER_STATS = dict(
    player_id=1100,
    team_id=529,
    team_name="Barcelona",
    league_id=140,
    season=2024,
    games=_VALID_GAMES,
    shots=_VALID_SHOTS,
    goals=_VALID_GOALS,
    passes=_VALID_PASSES,
    tackles=_VALID_TACKLES,
    duels=_VALID_DUELS,
    dribbles=_VALID_DRIBBLES,
    fouls=_VALID_FOULS,
    cards=_VALID_CARDS,
    penalty=_VALID_PENALTY,
)


# ─────────────────────────────────────────────────────────────
# RawAPIFootballPlayer
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballPlayer:
    """Tests for biographical data from API-Football."""

    def test_valid_player_all_fields(self):
        """A fully specified player is created correctly."""
        p = RawAPIFootballPlayer(
            player_id=1100,
            name="Pedro González López",
            firstname="Pedro",
            lastname="González López",
            age=22,
            birth_date="2002-11-25",
            nationality="Spain",
            height="174 cm",
            weight="60 kg",
            photo_url="https://media.api-sports.io/football/players/1100.png",
        )
        assert p.player_id == 1100
        assert p.name == "Pedro González López"
        assert p.firstname == "Pedro"
        assert p.age == 22
        assert p.height == "174 cm"

    def test_valid_player_from_fixture(self):
        """Player loaded from a realistic API-Football fixture validates."""
        data = _load_fixture("api_football_player.json")
        p = RawAPIFootballPlayer(**data[0]["player"])
        assert p.player_id == 1100
        assert p.name == "Pedro González López"

    def test_player_all_optional_fields_null(self):
        """All optional fields can be None (edge-case player entry)."""
        p = RawAPIFootballPlayer(player_id=1, name="Neymar")
        assert p.firstname is None
        assert p.lastname is None
        assert p.age is None
        assert p.birth_date is None
        assert p.nationality is None
        assert p.height is None
        assert p.weight is None
        assert p.photo_url is None

    def test_player_from_fixture_with_nulls(self):
        """Edge-case fixture with all optional fields null validates."""
        data = _load_fixture("api_football_player.json")
        p = RawAPIFootballPlayer(**data[1]["player"])
        assert p.player_id == 99999
        assert p.firstname is None
        assert p.age is None

    def test_player_rejects_missing_player_id(self):
        """player_id is required."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(name="Test")  # type: ignore[call-arg]

    def test_player_rejects_missing_name(self):
        """name is required."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(player_id=1)  # type: ignore[call-arg]

    def test_player_rejects_player_id_zero(self):
        """player_id must be >= 1."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(player_id=0, name="Test")

    def test_player_rejects_negative_age(self):
        """age cannot be negative."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(player_id=1, name="Test", age=-1)

    def test_player_rejects_age_over_100(self):
        """age cannot exceed 100."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(player_id=1, name="Test", age=101)

    def test_player_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayer(player_id=1, name="Test", unknown="x")

    def test_player_is_frozen(self):
        """Model is immutable after construction."""
        p = RawAPIFootballPlayer(player_id=1, name="Test")
        with pytest.raises(ValidationError):
            p.name = "Changed"  # type: ignore[misc]

    def test_player_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        p = RawAPIFootballPlayer(
            player_id=1100,
            name="Pedro González López",
            firstname="Pedro",
            lastname="González López",
            age=22,
            birth_date="2002-11-25",
            nationality="Spain",
            height="174 cm",
            weight="60 kg",
        )
        restored = RawAPIFootballPlayer.model_validate_json(p.model_dump_json())
        assert restored == p

    def test_player_json_schema_generation(self):
        """Model produces a valid JSON Schema."""
        schema = RawAPIFootballPlayer.model_json_schema()
        assert schema["type"] == "object"
        assert "player_id" in schema["properties"]
        assert "name" in schema["properties"]


# ─────────────────────────────────────────────────────────────
# RawAPIFootballPlayerStats
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballPlayerStats:
    """Tests for nested season statistics from API-Football."""

    def test_valid_stats_all_fields(self):
        """Fully populated stats with all sub-models are created correctly."""
        s = RawAPIFootballPlayerStats(**_VALID_PLAYER_STATS)
        assert s.player_id == 1100
        assert s.team_name == "Barcelona"
        assert s.season == 2024
        assert s.games.appearances == 28
        assert s.shots.total == 32
        assert s.goals.total == 5
        assert s.goals.assists == 7
        assert s.passes.accuracy == 89
        assert s.tackles.interceptions == 22
        assert s.duels.won == 105
        assert s.dribbles.success == 38
        assert s.fouls.committed == 15
        assert s.cards.yellow == 4
        assert s.penalty.scored == 0

    def test_valid_stats_from_fixture(self):
        """Stats loaded from a realistic API-Football fixture validate."""
        data = _load_fixture("api_football_player.json")
        stats_dict = data[0]["statistics"][0]
        s = RawAPIFootballPlayerStats(**stats_dict)
        assert s.player_id == 1100
        assert s.games.appearances == 28

    def test_stats_all_nullable_sub_fields_null(self):
        """Edge-case: all nullable stat sub-fields are None (e.g. GK without dribbles)."""
        data = _load_fixture("api_football_player.json")
        stats_dict = data[1]["statistics"][0]
        s = RawAPIFootballPlayerStats(**stats_dict)
        assert s.shots.total is None
        assert s.goals.total is None
        assert s.dribbles.attempts is None
        assert s.penalty.won is None

    def test_stats_rejects_missing_player_id(self):
        """player_id is required."""
        bad = {**_VALID_PLAYER_STATS}
        del bad["player_id"]
        with pytest.raises(ValidationError):
            RawAPIFootballPlayerStats(**bad)

    def test_stats_rejects_missing_games(self):
        """games sub-model is required."""
        bad = {**_VALID_PLAYER_STATS}
        del bad["games"]
        with pytest.raises(ValidationError):
            RawAPIFootballPlayerStats(**bad)

    def test_stats_rejects_negative_appearances(self):
        """games.appearances cannot be negative."""
        bad_games = {**_VALID_GAMES, "appearances": -1}
        with pytest.raises(ValidationError):
            _APIFootballGames(**bad_games)

    def test_stats_rejects_negative_shots_total(self):
        """shots.total cannot be negative."""
        with pytest.raises(ValidationError):
            _APIFootballShots(total=-1, on=0)

    def test_stats_rejects_passes_accuracy_over_100(self):
        """passes.accuracy cannot exceed 100."""
        with pytest.raises(ValidationError):
            _APIFootballPasses(total=100, key=5, accuracy=101)

    def test_stats_rejects_extra_fields_top_level(self):
        """Extra keys at the top-level stats model are rejected."""
        with pytest.raises(ValidationError):
            RawAPIFootballPlayerStats(**_VALID_PLAYER_STATS, unknown="x")

    def test_stats_rejects_extra_fields_nested(self):
        """Extra keys inside a nested sub-model are rejected."""
        bad_games = {**_VALID_GAMES, "extra_field": "value"}
        with pytest.raises(ValidationError):
            _APIFootballGames(**bad_games)

    def test_stats_is_frozen(self):
        """Top-level model is immutable after construction."""
        s = RawAPIFootballPlayerStats(**_VALID_PLAYER_STATS)
        with pytest.raises(ValidationError):
            s.player_id = 999  # type: ignore[misc]

    def test_stats_nested_is_frozen(self):
        """Nested sub-model is also immutable."""
        s = RawAPIFootballPlayerStats(**_VALID_PLAYER_STATS)
        with pytest.raises(ValidationError):
            s.games.appearances = 99  # type: ignore[misc]

    def test_stats_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        s = RawAPIFootballPlayerStats(**_VALID_PLAYER_STATS)
        restored = RawAPIFootballPlayerStats.model_validate_json(s.model_dump_json())
        assert restored == s

    def test_stats_json_schema_generation(self):
        """Model produces a valid JSON Schema with nested definitions."""
        schema = RawAPIFootballPlayerStats.model_json_schema()
        assert schema["type"] == "object"
        assert "player_id" in schema["properties"]
        assert "games" in schema["properties"]

    def test_sub_model_games_captain_defaults_false(self):
        """captain defaults to False when omitted."""
        g = _APIFootballGames()
        assert g.captain is False

    def test_sub_model_goals_saves_nullable(self):
        """saves is null for outfield players."""
        g = _APIFootballGoals(total=5, conceded=0, assists=3, saves=None)
        assert g.saves is None

    def test_sub_model_penalty_all_null(self):
        """All penalty fields can be null."""
        p = _APIFootballPenalty()
        assert p.won is None
        assert p.scored is None


# ─────────────────────────────────────────────────────────────
# RawAPIFootballInjury
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballInjury:
    """Tests for injury records from API-Football."""

    def test_valid_injury_all_fields(self):
        """A fully specified injury record is created correctly."""
        inj = RawAPIFootballInjury(
            player_id=1100,
            player_name="Pedro González López",
            team_id=529,
            team_name="Barcelona",
            fixture_id=1035042,
            league_id=140,
            reason="Knee Injury",
            type="Missing Fixture",
            date="2025-01-15",
        )
        assert inj.player_id == 1100
        assert inj.reason == "Knee Injury"
        assert inj.fixture_id == 1035042

    def test_valid_injury_from_fixture(self):
        """Injury loaded from fixture validates."""
        data = _load_fixture("api_football_injury.json")
        inj = RawAPIFootballInjury(**data[0])
        assert inj.player_id == 1100
        assert inj.reason == "Knee Injury"

    def test_injury_nullable_fixture_id(self):
        """fixture_id can be None (training injury, not fixture-linked)."""
        data = _load_fixture("api_football_injury.json")
        inj = RawAPIFootballInjury(**data[1])
        assert inj.fixture_id is None
        assert inj.reason == "ACL Injury"

    def test_injury_rejects_missing_reason(self):
        """reason is required."""
        with pytest.raises(ValidationError):
            RawAPIFootballInjury(
                player_id=1,
                player_name="X",
                team_id=1,
                team_name="T",
                league_id=1,
                type="Missing Fixture",
                date="2025-01-01",
            )  # type: ignore[call-arg]

    def test_injury_rejects_missing_type(self):
        """type is required."""
        with pytest.raises(ValidationError):
            RawAPIFootballInjury(
                player_id=1,
                player_name="X",
                team_id=1,
                team_name="T",
                league_id=1,
                reason="Knee",
                date="2025-01-01",
            )  # type: ignore[call-arg]

    def test_injury_rejects_player_id_zero(self):
        """player_id must be >= 1."""
        with pytest.raises(ValidationError):
            RawAPIFootballInjury(
                player_id=0,
                player_name="X",
                team_id=1,
                team_name="T",
                league_id=1,
                reason="Knee",
                type="Missing Fixture",
                date="2025-01-01",
            )

    def test_injury_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RawAPIFootballInjury(
                player_id=1,
                player_name="X",
                team_id=1,
                team_name="T",
                league_id=1,
                reason="Knee",
                type="Missing Fixture",
                date="2025-01-01",
                unknown="x",
            )

    def test_injury_is_frozen(self):
        """Model is immutable after construction."""
        inj = RawAPIFootballInjury(
            player_id=1,
            player_name="X",
            team_id=1,
            team_name="T",
            league_id=1,
            reason="Knee",
            type="Missing Fixture",
            date="2025-01-01",
        )
        with pytest.raises(ValidationError):
            inj.reason = "Changed"  # type: ignore[misc]

    def test_injury_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        inj = RawAPIFootballInjury(
            player_id=1100,
            player_name="Pedri",
            team_id=529,
            team_name="Barcelona",
            fixture_id=1035042,
            league_id=140,
            reason="Knee Injury",
            type="Missing Fixture",
            date="2025-01-15",
        )
        restored = RawAPIFootballInjury.model_validate_json(inj.model_dump_json())
        assert restored == inj


# ─────────────────────────────────────────────────────────────
# RawAPIFootballTransfer
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballTransfer:
    """Tests for transfer records from API-Football."""

    def test_valid_transfer_all_fields(self):
        """A fully specified transfer is created correctly."""
        t = RawAPIFootballTransfer(
            player_id=276,
            player_name="Neymar",
            date="2017-08-03",
            team_in_id=85,
            team_in_name="Paris Saint Germain",
            team_out_id=529,
            team_out_name="Barcelona",
            type="€ 222M",
        )
        assert t.player_id == 276
        assert t.team_in_name == "Paris Saint Germain"
        assert t.type == "€ 222M"

    def test_valid_transfer_from_fixture(self):
        """Transfer loaded from fixture validates."""
        data = _load_fixture("api_football_transfer.json")
        t = RawAPIFootballTransfer(**data[0])
        assert t.player_id == 276
        assert t.type == "€ 222M"

    def test_transfer_nullable_team_out(self):
        """team_out_id/name can be None (youth academy promotion)."""
        data = _load_fixture("api_football_transfer.json")
        t = RawAPIFootballTransfer(**data[1])
        assert t.team_out_id is None
        assert t.team_out_name is None

    def test_transfer_all_optional_fields_null(self):
        """date, team_in_id, team_out_id, type can all be None."""
        t = RawAPIFootballTransfer(player_id=1, player_name="Test")
        assert t.date is None
        assert t.team_in_id is None
        assert t.team_out_id is None
        assert t.type is None

    def test_transfer_rejects_missing_player_id(self):
        """player_id is required."""
        with pytest.raises(ValidationError):
            RawAPIFootballTransfer(player_name="Test")  # type: ignore[call-arg]

    def test_transfer_rejects_player_id_zero(self):
        """player_id must be >= 1."""
        with pytest.raises(ValidationError):
            RawAPIFootballTransfer(player_id=0, player_name="Test")

    def test_transfer_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RawAPIFootballTransfer(player_id=1, player_name="Test", unknown="x")

    def test_transfer_is_frozen(self):
        """Model is immutable after construction."""
        t = RawAPIFootballTransfer(player_id=1, player_name="Test")
        with pytest.raises(ValidationError):
            t.player_name = "Changed"  # type: ignore[misc]

    def test_transfer_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        t = RawAPIFootballTransfer(
            player_id=276,
            player_name="Neymar",
            date="2017-08-03",
            team_in_id=85,
            team_in_name="PSG",
            team_out_id=529,
            team_out_name="Barcelona",
            type="€ 222M",
        )
        restored = RawAPIFootballTransfer.model_validate_json(t.model_dump_json())
        assert restored == t


# ─────────────────────────────────────────────────────────────
# RawAPIFootballTeam
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballTeam:
    """Tests for team metadata from API-Football /teams endpoint."""

    def test_full_team_validates(self):
        team = RawAPIFootballTeam(
            team_id=529,
            name="Barcelona",
            code="BAR",
            country="Spain",
            founded=1899,
            national=False,
            logo_url="https://media.api-sports.io/football/teams/529.png",
            venue_name="Camp Nou",
            venue_address="Les Corts, 08028",
            venue_city="Barcelona",
            venue_capacity=55926,
            venue_surface="grass",
            venue_image_url="https://media.api-sports.io/football/venues/19939.png",
        )
        assert team.team_id == 529
        assert team.country == "Spain"
        assert team.venue_capacity == 55926

    def test_nullable_fields_default_to_none(self):
        team = RawAPIFootballTeam(team_id=1, name="FC Test")
        assert team.code is None
        assert team.country is None
        assert team.founded is None
        assert team.venue_name is None

    def test_national_defaults_false(self):
        team = RawAPIFootballTeam(team_id=1, name="FC Test")
        assert team.national is False

    def test_rejects_team_id_zero(self):
        with pytest.raises(ValidationError):
            RawAPIFootballTeam(team_id=0, name="Test")

    def test_rejects_founded_out_of_range(self):
        with pytest.raises(ValidationError):
            RawAPIFootballTeam(team_id=1, name="Test", founded=1700)

    def test_rejects_negative_venue_capacity(self):
        with pytest.raises(ValidationError):
            RawAPIFootballTeam(team_id=1, name="Test", venue_capacity=-1)

    def test_rejects_extra_fields(self):
        with pytest.raises(ValidationError):
            RawAPIFootballTeam(team_id=1, name="Test", unknown_field="x")

    def test_is_frozen(self):
        team = RawAPIFootballTeam(team_id=1, name="FC Test")
        with pytest.raises(ValidationError):
            team.name = "Changed"  # type: ignore[misc]

    def test_json_roundtrip(self):
        team = RawAPIFootballTeam(
            team_id=529, name="Barcelona", code="BAR", country="Spain",
            founded=1899, national=False, venue_name="Camp Nou",
            venue_city="Barcelona", venue_capacity=55926, venue_surface="grass",
        )
        restored = RawAPIFootballTeam.model_validate_json(team.model_dump_json())
        assert restored == team


# ─────────────────────────────────────────────────────────────
# RawUnderstatPlayerSeason
# ─────────────────────────────────────────────────────────────


class TestRawUnderstatPlayerSeason:
    """Tests for season-level advanced stats from Understat."""

    def test_valid_player_season_all_fields(self):
        """A fully populated season record is created correctly."""
        ps = RawUnderstatPlayerSeason(
            player_id=227,
            player_name="Robert Lewandowski",
            team="Barcelona",
            season="2024/2025",
            games=30,
            minutes=2520,
            goals=19,
            assists=4,
            xg=18.53,
            xa=3.12,
            npxg=16.08,
            xg_chain=22.35,
            xg_buildup=8.72,
            shots=98,
            key_passes=25,
            yellow_cards=3,
            red_cards=0,
        )
        assert ps.player_id == 227
        assert ps.player_name == "Robert Lewandowski"
        assert ps.xg == pytest.approx(18.53)
        assert ps.xg_chain == pytest.approx(22.35)
        assert ps.goals == 19

    def test_valid_player_season_from_fixture(self):
        """Season stats loaded from fixture validate."""
        data = _load_fixture("understat_player_season.json")
        ps = RawUnderstatPlayerSeason(**data[0])
        assert ps.player_id == 227
        assert ps.xg == pytest.approx(18.53)

    def test_player_season_xg_above_one_is_valid(self):
        """Season xG can exceed 1.0 (it is a season total, not per-shot)."""
        ps = RawUnderstatPlayerSeason(
            player_id=1,
            player_name="Striker",
            team="Club",
            season="2024/2025",
            games=30,
            minutes=2500,
            goals=20,
            assists=5,
            xg=25.0,
            xa=8.0,
            npxg=22.0,
            xg_chain=30.0,
            xg_buildup=12.0,
            shots=120,
            key_passes=30,
            yellow_cards=2,
            red_cards=0,
        )
        assert ps.xg == pytest.approx(25.0)

    def test_player_season_minimal_values(self):
        """Edge-case: minimum viable season (bench player)."""
        data = _load_fixture("understat_player_season.json")
        ps = RawUnderstatPlayerSeason(**data[1])
        assert ps.games == 2
        assert ps.xg == pytest.approx(0.0)

    def test_player_season_rejects_negative_xg(self):
        """xG cannot be negative."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
                games=1,
                minutes=90,
                goals=0,
                assists=0,
                xg=-0.1,
                xa=0.0,
                npxg=0.0,
                xg_chain=0.0,
                xg_buildup=0.0,
                shots=1,
                key_passes=0,
                yellow_cards=0,
                red_cards=0,
            )

    def test_player_season_rejects_negative_goals(self):
        """goals cannot be negative."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
                games=1,
                minutes=90,
                goals=-1,
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

    def test_player_season_rejects_negative_minutes(self):
        """minutes cannot be negative."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
                games=0,
                minutes=-1,
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

    def test_player_season_rejects_negative_games(self):
        """games cannot be negative."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
                games=-1,
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

    def test_player_season_rejects_negative_shots(self):
        """shots cannot be negative."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
                games=0,
                minutes=0,
                goals=0,
                assists=0,
                xg=0.0,
                xa=0.0,
                npxg=0.0,
                xg_chain=0.0,
                xg_buildup=0.0,
                shots=-1,
                key_passes=0,
                yellow_cards=0,
                red_cards=0,
            )

    def test_player_season_rejects_missing_player_id(self):
        """player_id is required."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_name="T",
                team="C",
                season="2024",
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
            )  # type: ignore[call-arg]

    def test_player_season_rejects_missing_player_name(self):
        """player_name is required."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                team="C",
                season="2024",
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
            )  # type: ignore[call-arg]

    def test_player_season_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RawUnderstatPlayerSeason(
                player_id=1,
                player_name="T",
                team="C",
                season="2024",
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
                unknown="x",
            )

    def test_player_season_is_frozen(self):
        """Model is immutable after construction."""
        ps = RawUnderstatPlayerSeason(
            player_id=1,
            player_name="T",
            team="C",
            season="2024",
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
        with pytest.raises(ValidationError):
            ps.goals = 99  # type: ignore[misc]

    def test_player_season_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        ps = RawUnderstatPlayerSeason(
            player_id=227,
            player_name="Robert Lewandowski",
            team="Barcelona",
            season="2024/2025",
            games=30,
            minutes=2520,
            goals=19,
            assists=4,
            xg=18.53,
            xa=3.12,
            npxg=16.08,
            xg_chain=22.35,
            xg_buildup=8.72,
            shots=98,
            key_passes=25,
            yellow_cards=3,
            red_cards=0,
        )
        restored = RawUnderstatPlayerSeason.model_validate_json(ps.model_dump_json())
        assert restored == ps

    def test_player_season_json_schema_generation(self):
        """Model produces a valid JSON Schema."""
        schema = RawUnderstatPlayerSeason.model_json_schema()
        assert schema["type"] == "object"
        assert "xg" in schema["properties"]
        assert "xg_chain" in schema["properties"]
        assert "xg_buildup" in schema["properties"]


# ─────────────────────────────────────────────────────────────
# RawUnderstatShot
# ─────────────────────────────────────────────────────────────


class TestRawUnderstatShot:
    """Tests para tiros crudos provenientes de Understat."""

    def test_valid_shot_with_all_fields(self):
        """Un tiro completo de Understat se crea correctamente."""
        shot = RawUnderstatShot(
            id=452109,
            minute=23,
            result="Goal",
            x=0.915,
            y=0.443,
            xg=0.7623,
            player="Mohamed Salah",
            player_id=1250,
            situation="OpenPlay",
            body_part="Right Foot",
        )
        assert shot.id == 452109
        assert shot.minute == 23
        assert shot.result == "Goal"
        assert shot.x == pytest.approx(0.915)
        assert shot.y == pytest.approx(0.443)
        assert shot.xg == pytest.approx(0.7623)
        assert shot.player == "Mohamed Salah"
        assert shot.player_id == 1250
        assert shot.situation == "OpenPlay"
        assert shot.body_part == "Right Foot"

    def test_valid_shot_body_part_optional(self):
        """body_part es opcional; por defecto es None."""
        shot = RawUnderstatShot(
            id=1,
            minute=10,
            result="SavedShot",
            x=0.5,
            y=0.5,
            xg=0.15,
            player="Test Player",
            player_id=99,
            situation="OpenPlay",
        )
        assert shot.body_part is None

    def test_shot_coordinate_bounds_valid(self):
        """Coordenadas x,y deben estar entre 0 y 1 (normalizadas Understat)."""
        shot = RawUnderstatShot(
            id=1,
            minute=0,
            result="MissedShots",
            x=0.0,
            y=0.0,
            xg=0.02,
            player="Test Player",
            player_id=99,
            situation="SetPiece",
        )
        assert shot.x == 0.0
        assert shot.y == 0.0

    def test_shot_rejects_x_above_1(self):
        """x no puede superar 1.0."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=2,
                minute=10,
                result="SavedShot",
                x=1.1,
                y=0.5,
                xg=0.1,
                player="Test",
                player_id=1,
                situation="OpenPlay",
            )

    def test_shot_rejects_y_below_0(self):
        """y no puede ser menor que 0."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=3,
                minute=10,
                result="SavedShot",
                x=0.5,
                y=-0.1,
                xg=0.1,
                player="Test",
                player_id=1,
                situation="OpenPlay",
            )

    def test_shot_rejects_xg_above_1(self):
        """xG no puede superar 1.0."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=4,
                minute=10,
                result="Goal",
                x=0.9,
                y=0.5,
                xg=1.5,
                player="Test",
                player_id=1,
                situation="OpenPlay",
            )

    def test_shot_rejects_negative_xg(self):
        """xG no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=5,
                minute=10,
                result="Goal",
                x=0.9,
                y=0.5,
                xg=-0.1,
                player="Test",
                player_id=1,
                situation="OpenPlay",
            )

    def test_shot_rejects_negative_minute(self):
        """minute no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=6,
                minute=-1,
                result="Goal",
                x=0.9,
                y=0.5,
                xg=0.5,
                player="Test",
                player_id=1,
                situation="OpenPlay",
            )

    def test_shot_rejects_extra_fields(self):
        """Campos extra no declarados son rechazados."""
        with pytest.raises(ValidationError):
            RawUnderstatShot(
                id=7,
                minute=10,
                result="Goal",
                x=0.9,
                y=0.5,
                xg=0.5,
                player="Test",
                player_id=1,
                situation="OpenPlay",
                unknown="value",
            )

    def test_shot_json_roundtrip(self):
        """Serialización JSON ida y vuelta."""
        shot = RawUnderstatShot(
            id=99999,
            minute=88,
            result="BlockedShot",
            x=0.82,
            y=0.61,
            xg=0.04,
            player="Erling Haaland",
            player_id=8260,
            situation="FromCorner",
        )
        json_str = shot.model_dump_json()
        restored = RawUnderstatShot.model_validate_json(json_str)
        assert restored == shot

    def test_shot_json_schema_generation(self):
        """El modelo genera un JSON Schema válido."""
        schema = RawUnderstatShot.model_json_schema()
        assert schema["type"] == "object"
        assert "xg" in schema["properties"]


# ─────────────────────────────────────────────────────────────
# RawAPIFootballStandings
# ─────────────────────────────────────────────────────────────


class TestRawAPIFootballStandings:
    """Tests for league standings records from API-Football."""

    def test_raw_standings_model(self):
        """A fully specified standings record is created correctly."""
        s = RawAPIFootballStandings(
            league_id=140,
            season=2024,
            team_id=529,
            team_name="Barcelona",
            rank=1,
            points=68,
            played_total=28,
            wins=21,
            draws=5,
            losses=2,
            goals_for=72,
            goals_against=31,
            goal_diff=41,
            form="WWWDW",
        )
        assert s.played_total == 28
        assert s.goal_diff == 41

    def test_standings_form_is_optional(self):
        """form can be None early in the season."""
        s = RawAPIFootballStandings(
            league_id=140,
            season=2024,
            team_id=529,
            team_name="Barcelona",
            rank=1,
            points=0,
            played_total=0,
            wins=0,
            draws=0,
            losses=0,
            goals_for=0,
            goals_against=0,
            goal_diff=0,
            form=None,
        )
        assert s.form is None

    def test_standings_goal_diff_can_be_negative(self):
        """goal_diff can be negative (bottom of the table)."""
        s = RawAPIFootballStandings(
            league_id=140,
            season=2024,
            team_id=999,
            team_name="Relegated FC",
            rank=20,
            points=10,
            played_total=28,
            wins=2,
            draws=4,
            losses=22,
            goals_for=15,
            goals_against=65,
            goal_diff=-50,
            form="LLLLL",
        )
        assert s.goal_diff == -50

    def test_standings_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RawAPIFootballStandings(
                league_id=140,
                season=2024,
                team_id=529,
                team_name="Barcelona",
                rank=1,
                points=68,
                played_total=28,
                wins=21,
                draws=5,
                losses=2,
                goals_for=72,
                goals_against=31,
                goal_diff=41,
                unknown="x",
            )

    def test_standings_is_frozen(self):
        """Model is immutable after construction."""
        s = RawAPIFootballStandings(
            league_id=140,
            season=2024,
            team_id=529,
            team_name="Barcelona",
            rank=1,
            points=68,
            played_total=28,
            wins=21,
            draws=5,
            losses=2,
            goals_for=72,
            goals_against=31,
            goal_diff=41,
        )
        with pytest.raises(ValidationError):
            s.points = 99  # type: ignore[misc]

    def test_standings_json_roundtrip(self):
        """JSON serialisation round-trip preserves all values."""
        s = RawAPIFootballStandings(
            league_id=140,
            season=2024,
            team_id=529,
            team_name="Barcelona",
            rank=1,
            points=68,
            played_total=28,
            wins=21,
            draws=5,
            losses=2,
            goals_for=72,
            goals_against=31,
            goal_diff=41,
            form="WWWDW",
        )
        restored = RawAPIFootballStandings.model_validate_json(s.model_dump_json())
        assert restored == s
