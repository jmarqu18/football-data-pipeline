"""Unit tests for the Understat loader module.

Tests validate extraction logic, Pydantic validation, Parquet output,
and orchestration — all using a mocked soccerdata client.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from pipeline.config import UnderstatConfig
from pipeline.loaders.understat_loader import UnderstatLoader
from pipeline.models.raw import RawUnderstatPlayerSeason, RawUnderstatShot

_FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> list[dict]:
    """Load a JSON fixture file and return the parsed list."""
    with open(_FIXTURES / name, encoding="utf-8") as f:
        return json.load(f)


def _make_config() -> UnderstatConfig:
    return UnderstatConfig(league="La Liga", season="2024/2025")


def _shots_dataframe() -> pd.DataFrame:
    """Build a DataFrame mimicking soccerdata read_shot_events() output."""
    return pd.DataFrame(_load_fixture("understat_shots_dataframe.json"))


def _season_dataframe() -> pd.DataFrame:
    """Build a DataFrame mimicking soccerdata read_player_season_stats() output."""
    return pd.DataFrame(_load_fixture("understat_season_dataframe.json"))


def _mock_client(
    shots_df: pd.DataFrame | None = None,
    season_df: pd.DataFrame | None = None,
) -> MagicMock:
    """Create a mock soccerdata Understat client."""
    client = MagicMock()
    client.read_shot_events.return_value = shots_df if shots_df is not None else _shots_dataframe()
    client.read_player_season_stats.return_value = (
        season_df if season_df is not None else _season_dataframe()
    )
    return client


# ─────────────────────────────────────────────────────────────
# Shot extraction
# ─────────────────────────────────────────────────────────────


class TestShotExtraction:
    """Tests for _extract_shot column mapping."""

    def test_extract_shot_maps_columns_correctly(self):
        """soccerdata column names are mapped to Pydantic field names."""
        row = _load_fixture("understat_shots_dataframe.json")[0]
        extracted = UnderstatLoader._extract_shot(row)

        assert extracted["id"] == 452109
        assert extracted["x"] == 0.915
        assert extracted["y"] == 0.443
        assert extracted["xg"] == 0.7623
        assert extracted["player"] == "Robert Lewandowski"
        assert extracted["player_id"] == 227
        assert extracted["minute"] == 23
        assert extracted["result"] == "Goal"
        assert extracted["situation"] == "OpenPlay"
        assert extracted["body_part"] == "Right Foot"

    def test_extract_shot_validates_as_model(self):
        """Extracted dict passes Pydantic validation."""
        row = _load_fixture("understat_shots_dataframe.json")[0]
        extracted = UnderstatLoader._extract_shot(row)
        shot = RawUnderstatShot.model_validate(extracted)

        assert shot.id == 452109
        assert shot.player == "Robert Lewandowski"

    def test_extract_shot_drops_extra_columns(self):
        """soccerdata columns not in the model are excluded."""
        row = _load_fixture("understat_shots_dataframe.json")[0]
        extracted = UnderstatLoader._extract_shot(row)

        extra_keys = {
            "league",
            "season",
            "game",
            "team",
            "date",
            "assist_player",
            "assist_player_id",
            "league_id",
            "season_id",
            "game_id",
            "team_id",
            "shot_id",
            "location_x",
            "location_y",
        }
        assert not extra_keys & set(extracted.keys())

    def test_extract_shot_body_part_is_none_when_missing(self):
        """body_part defaults to None when the column is absent."""
        row = _load_fixture("understat_shots_dataframe.json")[0].copy()
        row.pop("body_part", None)
        extracted = UnderstatLoader._extract_shot(row)

        assert extracted["body_part"] is None
        shot = RawUnderstatShot.model_validate(extracted)
        assert shot.body_part is None


# ─────────────────────────────────────────────────────────────
# Player season extraction
# ─────────────────────────────────────────────────────────────


class TestPlayerSeasonExtraction:
    """Tests for _extract_player_season column mapping."""

    def test_extract_player_season_maps_columns_correctly(self):
        """soccerdata column names are mapped to Pydantic field names."""
        row = _load_fixture("understat_season_dataframe.json")[0]
        extracted = UnderstatLoader._extract_player_season(row)

        assert extracted["player_id"] == 227
        assert extracted["player_name"] == "Robert Lewandowski"
        assert extracted["team"] == "Barcelona"
        assert extracted["season"] == "2024/2025"
        assert extracted["games"] == 30
        assert extracted["minutes"] == 2520
        assert extracted["goals"] == 19
        assert extracted["assists"] == 4
        assert extracted["xg"] == 18.53
        assert extracted["xa"] == 3.12
        assert extracted["npxg"] == 16.08
        assert extracted["xg_chain"] == 22.35
        assert extracted["xg_buildup"] == 8.72
        assert extracted["shots"] == 98
        assert extracted["key_passes"] == 25
        assert extracted["yellow_cards"] == 3
        assert extracted["red_cards"] == 0

    def test_extract_player_season_validates_as_model(self):
        """Extracted dict passes Pydantic validation."""
        row = _load_fixture("understat_season_dataframe.json")[0]
        extracted = UnderstatLoader._extract_player_season(row)
        stats = RawUnderstatPlayerSeason.model_validate(extracted)

        assert stats.player_name == "Robert Lewandowski"
        assert stats.xg == 18.53

    def test_extract_player_season_drops_extra_columns(self):
        """soccerdata columns not in the model are excluded."""
        row = _load_fixture("understat_season_dataframe.json")[0]
        extracted = UnderstatLoader._extract_player_season(row)

        extra_keys = {
            "league",
            "league_id",
            "season_id",
            "team_id",
            "position",
            "np_goals",
            "np_xg",
            "matches",
            "player",
        }
        assert not extra_keys & set(extracted.keys())


# ─────────────────────────────────────────────────────────────
# Ingest shots
# ─────────────────────────────────────────────────────────────


class TestIngestShots:
    """Tests for the ingest_shots() public method."""

    def test_ingest_shots_returns_validated_models(self):
        """All valid shots are returned as RawUnderstatShot instances."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        shots = loader.ingest_shots()

        assert len(shots) == 3
        assert all(isinstance(s, RawUnderstatShot) for s in shots)
        assert shots[0].player == "Robert Lewandowski"
        assert shots[1].player == "Lamine Yamal"
        assert shots[2].player == "Vinícius Júnior"

    def test_ingest_shots_rejects_invalid_record(self, caplog):
        """A shot with xg > 1.0 is rejected and logged."""
        fixture = _load_fixture("understat_shots_dataframe.json")
        fixture[0]["xg"] = 1.5  # invalid: per-shot xG must be <= 1.0
        df = pd.DataFrame(fixture)
        client = _mock_client(shots_df=df)

        loader = UnderstatLoader(_make_config(), client=client)
        with caplog.at_level(logging.WARNING):
            shots = loader.ingest_shots()

        assert len(shots) == 2  # 3 - 1 rejected
        assert "Rejected shot" in caplog.text

    def test_ingest_shots_logs_summary(self, caplog):
        """INFO log with valid/rejected counts is emitted."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        with caplog.at_level(logging.INFO):
            loader.ingest_shots()

        assert "Shots ingested" in caplog.text
        assert "3 valid" in caplog.text

    def test_ingest_shots_empty_dataframe(self, caplog):
        """Empty DataFrame returns empty list with INFO log."""
        client = _mock_client(shots_df=pd.DataFrame())
        loader = UnderstatLoader(_make_config(), client=client)

        with caplog.at_level(logging.INFO):
            shots = loader.ingest_shots()

        assert shots == []
        assert "0 valid" in caplog.text


# ─────────────────────────────────────────────────────────────
# Ingest player season stats
# ─────────────────────────────────────────────────────────────


class TestIngestPlayerSeasonStats:
    """Tests for the ingest_player_season_stats() public method."""

    def test_ingest_player_season_returns_validated_models(self):
        """All valid records are returned as RawUnderstatPlayerSeason."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        stats = loader.ingest_player_season_stats()

        assert len(stats) == 2
        assert all(isinstance(s, RawUnderstatPlayerSeason) for s in stats)
        assert stats[0].player_name == "Robert Lewandowski"
        assert stats[1].player_name == "Bench Player"

    def test_ingest_player_season_rejects_invalid_record(self, caplog):
        """A record with negative games is rejected and logged."""
        fixture = _load_fixture("understat_season_dataframe.json")
        fixture[0]["matches"] = -1  # invalid: games must be >= 0
        df = pd.DataFrame(fixture)
        client = _mock_client(season_df=df)

        loader = UnderstatLoader(_make_config(), client=client)
        with caplog.at_level(logging.WARNING):
            stats = loader.ingest_player_season_stats()

        assert len(stats) == 1  # 2 - 1 rejected
        assert "Rejected player season" in caplog.text

    def test_ingest_player_season_logs_summary(self, caplog):
        """INFO log with valid/rejected counts is emitted."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        with caplog.at_level(logging.INFO):
            loader.ingest_player_season_stats()

        assert "Player season stats ingested" in caplog.text
        assert "2 valid" in caplog.text


# ─────────────────────────────────────────────────────────────
# Parquet output
# ─────────────────────────────────────────────────────────────


class TestSaveParquet:
    """Tests for the save_parquet() static method."""

    def test_save_parquet_creates_file(self, tmp_path: Path):
        """Parquet file is created with the correct number of records."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        shots = loader.ingest_shots()
        out = tmp_path / "shots.parquet"

        UnderstatLoader.save_parquet(shots, out)

        assert out.exists()
        import pyarrow.parquet as pq

        table = pq.read_table(out)
        assert table.num_rows == 3

    def test_save_parquet_empty_list_logs_warning(self, tmp_path: Path, caplog):
        """Empty model list logs a warning and creates no file."""
        out = tmp_path / "empty.parquet"

        with caplog.at_level(logging.WARNING):
            UnderstatLoader.save_parquet([], out)

        assert not out.exists()
        assert "No records to save" in caplog.text


# ─────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────


class TestIngestAll:
    """Tests for the ingest_all() orchestration method."""

    def test_ingest_all_saves_both_parquet_files(self, tmp_path: Path):
        """Both shots.parquet and player_season.parquet are created."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        loader.ingest_all(output_dir=tmp_path)

        assert (tmp_path / "shots.parquet").exists()
        assert (tmp_path / "player_season.parquet").exists()

    def test_ingest_all_returns_counts(self):
        """Return dict has correct keys and counts."""
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        counts = loader.ingest_all()

        assert counts["shots"] == 3
        assert counts["player_season"] == 2

    def test_ingest_all_custom_output_dir(self, tmp_path: Path):
        """Custom output_dir is respected."""
        custom = tmp_path / "custom" / "understat"
        loader = UnderstatLoader(_make_config(), client=_mock_client())
        loader.ingest_all(output_dir=custom)

        assert (custom / "shots.parquet").exists()
        assert (custom / "player_season.parquet").exists()


# ─────────────────────────────────────────────────────────────
# Error handling
# ─────────────────────────────────────────────────────────────


class TestErrorHandling:
    """Tests for graceful error handling on network failures."""

    def test_network_error_caught_and_logged(self, caplog):
        """soccerdata exception is caught; empty list returned."""
        client = MagicMock()
        client.read_shot_events.side_effect = ConnectionError("timeout")

        loader = UnderstatLoader(_make_config(), client=client)
        with caplog.at_level(logging.ERROR):
            shots = loader.ingest_shots()

        assert shots == []
        assert "Failed to fetch shot events" in caplog.text

    def test_season_stats_network_error(self, caplog):
        """Network error in season stats returns empty list."""
        client = MagicMock()
        client.read_player_season_stats.side_effect = ConnectionError("timeout")

        loader = UnderstatLoader(_make_config(), client=client)
        with caplog.at_level(logging.ERROR):
            stats = loader.ingest_player_season_stats()

        assert stats == []
        assert "Failed to fetch player season stats" in caplog.text
