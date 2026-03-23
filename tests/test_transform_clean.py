"""Tests for the transform_clean module (RAW → CLEAN pipeline)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.transform_clean import (
    parse_date,
    parse_measurement,
    parse_rating,
    parse_transfer_type,
    read_parquet_models,
)

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_parquet(tmp_path: Path):
    """Helper to write a list of dicts to a temporary Parquet file."""

    def _write(rows: list[dict], filename: str = "test.parquet") -> Path:
        path = tmp_path / filename
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        return path

    return _write


# ─────────────────────────────────────────────────────────────
# read_parquet_models
# ─────────────────────────────────────────────────────────────


class TestReadParquetModels:
    """Tests for Parquet → Pydantic deserialization."""

    def test_simple_model(self, tmp_parquet):
        """Valid rows are deserialized into Pydantic models."""
        from pipeline.models.raw import RawAPIFootballPlayer

        rows = [
            {
                "player_id": 1,
                "name": "Test Player",
                "firstname": "Test",
                "lastname": "Player",
                "age": 25,
                "birth_date": "1999-01-15",
                "nationality": "Spain",
                "height": "180 cm",
                "weight": "75 kg",
                "photo_url": None,
            },
            {
                "player_id": 2,
                "name": "Another Player",
                "firstname": None,
                "lastname": None,
                "age": None,
                "birth_date": None,
                "nationality": None,
                "height": None,
                "weight": None,
                "photo_url": None,
            },
        ]
        path = tmp_parquet(rows)
        models = read_parquet_models(path, RawAPIFootballPlayer)

        assert len(models) == 2
        assert models[0].player_id == 1
        assert models[0].name == "Test Player"
        assert models[1].player_id == 2

    def test_invalid_rows_skipped(self, tmp_parquet):
        """Rows that fail Pydantic validation are skipped, not raised."""
        from pipeline.models.raw import RawAPIFootballPlayer

        rows = [
            {
                "player_id": 1,
                "name": "Valid Player",
                "firstname": None,
                "lastname": None,
                "age": None,
                "birth_date": None,
                "nationality": None,
                "height": None,
                "weight": None,
                "photo_url": None,
            },
            # Invalid: player_id < 1
            {
                "player_id": 0,
                "name": "Invalid Player",
                "firstname": None,
                "lastname": None,
                "age": None,
                "birth_date": None,
                "nationality": None,
                "height": None,
                "weight": None,
                "photo_url": None,
            },
        ]
        path = tmp_parquet(rows)
        models = read_parquet_models(path, RawAPIFootballPlayer)

        assert len(models) == 1
        assert models[0].player_id == 1

    def test_nested_models(self, tmp_parquet):
        """RawAPIFootballPlayerStats with nested structs deserializes correctly."""
        from pipeline.models.raw import RawAPIFootballPlayerStats

        rows = [
            {
                "player_id": 100,
                "team_id": 529,
                "team_name": "Barcelona",
                "league_id": 140,
                "season": 2024,
                "games": {
                    "appearances": 30,
                    "lineups": 28,
                    "minutes": 2500,
                    "number": 8,
                    "position": "Midfielder",
                    "rating": "7.5",
                    "captain": False,
                },
                "shots": {"total": 40, "on": 20},
                "goals": {"total": 10, "conceded": 0, "assists": 5, "saves": 0},
                "passes": {"total": 1500, "key": 30, "accuracy": 85},
                "tackles": {"total": 50, "blocks": 10, "interceptions": 20},
                "duels": {"total": 200, "won": 120},
                "dribbles": {"attempts": 60, "success": 40, "past": 5},
                "fouls": {"drawn": 25, "committed": 15},
                "cards": {"yellow": 3, "yellowred": 0, "red": 0},
                "penalty": {"won": 1, "committed": 0, "scored": 1, "missed": 0, "saved": 0},
            }
        ]
        path = tmp_parquet(rows)
        models = read_parquet_models(path, RawAPIFootballPlayerStats)

        assert len(models) == 1
        s = models[0]
        assert s.player_id == 100
        assert s.team_name == "Barcelona"
        assert s.games.appearances == 30
        assert s.games.rating == "7.5"
        assert s.goals.total == 10
        assert s.dribbles.attempts == 60


# ─────────────────────────────────────────────────────────────
# Parsing helpers
# ─────────────────────────────────────────────────────────────


class TestParseMeasurement:
    """Tests for height/weight string parsing."""

    def test_height(self):
        assert parse_measurement("174 cm") == 174

    def test_weight(self):
        assert parse_measurement("60 kg") == 60

    def test_none(self):
        assert parse_measurement(None) is None

    def test_empty(self):
        assert parse_measurement("") is None

    def test_no_digits(self):
        assert parse_measurement("unknown") is None


class TestParseRating:
    """Tests for rating string parsing."""

    def test_valid(self):
        assert parse_rating("7.342857") == Decimal("7.342857")

    def test_none(self):
        assert parse_rating(None) is None

    def test_empty(self):
        assert parse_rating("") is None

    def test_invalid(self):
        assert parse_rating("N/A") is None


class TestParseDate:
    """Tests for ISO date string parsing."""

    def test_valid(self):
        from datetime import date

        assert parse_date("2002-11-25") == date(2002, 11, 25)

    def test_none(self):
        assert parse_date(None) is None

    def test_empty(self):
        assert parse_date("") is None

    def test_invalid(self):
        assert parse_date("not-a-date") is None


class TestParseTransferType:
    """Tests for transfer type/fee parsing."""

    def test_loan(self):
        assert parse_transfer_type("Loan") == ("Loan", None)

    def test_free(self):
        assert parse_transfer_type("Free") == ("Free", None)

    def test_fee_euro(self):
        assert parse_transfer_type("€ 222M") == (None, "€ 222M")

    def test_fee_pound(self):
        assert parse_transfer_type("£50M") == (None, "£50M")

    def test_none(self):
        assert parse_transfer_type(None) == (None, None)

    def test_na(self):
        assert parse_transfer_type("N/A") == ("N/A", None)
