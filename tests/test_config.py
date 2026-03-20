"""Unit tests for ingestion configuration models and YAML loader.

Tests validate:
- Pydantic model construction, field constraints, and coercions.
- ``load_config`` happy path and error handling.
- ``get_config`` singleton behaviour.

All model tests use inline construction (no shared fixtures).
File I/O tests use ``tmp_path`` (pytest built-in).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.config import (
    ApiFootballConfig,
    ConfigurationError,
    IngestionConfig,
    RateLimitConfig,
    SourcesConfig,
    UnderstatConfig,
    get_config,
    load_config,
)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

_VALID_RATE_LIMIT = dict(max_calls_per_day=100, delay_between_calls=1.0)

_VALID_API_FOOTBALL = dict(
    league_id=140,
    season=2024,
    endpoints=["players_stats", "injuries", "transfers"],
    cache_dir="data/cache/api_football",
    cache_ttl_hours=168,
    rate_limit=_VALID_RATE_LIMIT,
)

_VALID_UNDERSTAT = dict(league="La Liga", season="2024/2025")

_VALID_SOURCES = dict(
    api_football=_VALID_API_FOOTBALL,
    understat=_VALID_UNDERSTAT,
)

_VALID_CONFIG_DICT = dict(sources=_VALID_SOURCES)

_VALID_YAML = """\
sources:
  api_football:
    league_id: 140
    season: 2024
    endpoints:
      - players_stats
      - injuries
      - transfers
    cache_dir: data/cache/api_football
    cache_ttl_hours: 168
    rate_limit:
      max_calls_per_day: 100
      delay_between_calls: 1.0
  understat:
    league: "La Liga"
    season: "2024/2025"
"""


# ─────────────────────────────────────────────────────────────
# RateLimitConfig
# ─────────────────────────────────────────────────────────────


class TestRateLimitConfig:
    """Tests for RateLimitConfig validation and constraints."""

    def test_valid_construction(self):
        """A valid rate-limit config is created correctly."""
        cfg = RateLimitConfig(max_calls_per_day=100, delay_between_calls=1.0)
        assert cfg.max_calls_per_day == 100
        assert cfg.delay_between_calls == pytest.approx(1.0)

    def test_zero_delay_is_valid(self):
        """delay_between_calls=0.0 is allowed (no intentional delay)."""
        cfg = RateLimitConfig(max_calls_per_day=50, delay_between_calls=0.0)
        assert cfg.delay_between_calls == pytest.approx(0.0)

    def test_rejects_max_calls_zero(self):
        """max_calls_per_day must be at least 1."""
        with pytest.raises(ValidationError):
            RateLimitConfig(max_calls_per_day=0, delay_between_calls=1.0)

    def test_rejects_negative_delay(self):
        """delay_between_calls cannot be negative."""
        with pytest.raises(ValidationError):
            RateLimitConfig(max_calls_per_day=100, delay_between_calls=-0.5)

    def test_rejects_max_calls_above_limit(self):
        """max_calls_per_day is capped at 10_000."""
        with pytest.raises(ValidationError):
            RateLimitConfig(max_calls_per_day=10_001, delay_between_calls=0.0)

    def test_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            RateLimitConfig(max_calls_per_day=100, delay_between_calls=1.0, unknown="x")

    def test_is_frozen(self):
        """Model is immutable after construction."""
        cfg = RateLimitConfig(max_calls_per_day=100, delay_between_calls=1.0)
        with pytest.raises(ValidationError):
            cfg.max_calls_per_day = 999  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────
# ApiFootballConfig
# ─────────────────────────────────────────────────────────────


class TestApiFootballConfig:
    """Tests for ApiFootballConfig validation, coercions, and constraints."""

    def test_valid_construction(self):
        """A fully specified API-Football config is created correctly."""
        cfg = ApiFootballConfig(**_VALID_API_FOOTBALL)
        assert cfg.league_id == 140
        assert cfg.season == 2024
        assert cfg.endpoints == ("players_stats", "injuries", "transfers")
        assert cfg.cache_dir == Path("data/cache/api_football")
        assert cfg.cache_ttl_hours == 168

    def test_endpoints_list_coerced_to_tuple(self):
        """A list from YAML is transparently coerced to a tuple."""
        cfg = ApiFootballConfig(**_VALID_API_FOOTBALL)
        assert isinstance(cfg.endpoints, tuple)

    def test_endpoints_already_tuple_is_accepted(self):
        """Passing a tuple directly (not from YAML) also works."""
        data = {**_VALID_API_FOOTBALL, "endpoints": ("players_stats",)}
        cfg = ApiFootballConfig(**data)
        assert cfg.endpoints == ("players_stats",)

    def test_cache_dir_string_coerced_to_path(self):
        """A string cache_dir is coerced to pathlib.Path by Pydantic."""
        cfg = ApiFootballConfig(**_VALID_API_FOOTBALL)
        assert isinstance(cfg.cache_dir, Path)

    def test_rejects_league_id_zero(self):
        """league_id must be at least 1."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "league_id": 0})

    def test_rejects_season_too_old(self):
        """season must be >= 2000."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "season": 1999})

    def test_rejects_season_too_far_future(self):
        """season must be <= 2100."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "season": 2101})

    def test_rejects_empty_endpoints(self):
        """endpoints must have at least one entry."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "endpoints": []})

    def test_rejects_cache_ttl_zero(self):
        """cache_ttl_hours must be at least 1."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "cache_ttl_hours": 0})

    def test_rejects_cache_ttl_above_year(self):
        """cache_ttl_hours is capped at 8760 (one year)."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**{**_VALID_API_FOOTBALL, "cache_ttl_hours": 8761})

    def test_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            ApiFootballConfig(**_VALID_API_FOOTBALL, unknown="x")

    def test_is_frozen(self):
        """Model is immutable after construction."""
        cfg = ApiFootballConfig(**_VALID_API_FOOTBALL)
        with pytest.raises(ValidationError):
            cfg.league_id = 999  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────
# UnderstatConfig
# ─────────────────────────────────────────────────────────────


class TestUnderstatConfig:
    """Tests for UnderstatConfig validation."""

    def test_valid_construction(self):
        """A valid Understat config is created correctly."""
        cfg = UnderstatConfig(league="La Liga", season="2024/2025")
        assert cfg.league == "La Liga"
        assert cfg.season == "2024/2025"

    def test_rejects_missing_league(self):
        """league is required."""
        with pytest.raises(ValidationError):
            UnderstatConfig(season="2024/2025")  # type: ignore[call-arg]

    def test_rejects_missing_season(self):
        """season is required."""
        with pytest.raises(ValidationError):
            UnderstatConfig(league="La Liga")  # type: ignore[call-arg]

    def test_rejects_extra_fields(self):
        """Extra fields are forbidden."""
        with pytest.raises(ValidationError):
            UnderstatConfig(league="La Liga", season="2024/2025", unknown="x")

    def test_is_frozen(self):
        """Model is immutable after construction."""
        cfg = UnderstatConfig(league="La Liga", season="2024/2025")
        with pytest.raises(ValidationError):
            cfg.league = "Premier League"  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────
# SourcesConfig
# ─────────────────────────────────────────────────────────────


class TestSourcesConfig:
    """Tests for SourcesConfig validation."""

    def test_valid_construction(self):
        """Both source configs are required and accepted."""
        cfg = SourcesConfig(**_VALID_SOURCES)
        assert cfg.api_football.league_id == 140
        assert cfg.understat.league == "La Liga"

    def test_rejects_extra_source_keys(self):
        """Unknown source keys (e.g. a new data source) are rejected."""
        with pytest.raises(ValidationError):
            SourcesConfig(**_VALID_SOURCES, new_source={"key": "val"})

    def test_is_frozen(self):
        """Model is immutable after construction."""
        cfg = SourcesConfig(**_VALID_SOURCES)
        with pytest.raises(ValidationError):
            cfg.understat = UnderstatConfig(league="Premier League", season="2024/2025")  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────
# IngestionConfig
# ─────────────────────────────────────────────────────────────


class TestIngestionConfig:
    """Tests for the root IngestionConfig model."""

    def test_valid_construction(self):
        """Full config dict validates correctly."""
        cfg = IngestionConfig.model_validate(_VALID_CONFIG_DICT)
        assert cfg.sources.api_football.league_id == 140
        assert cfg.sources.api_football.endpoints == (
            "players_stats",
            "injuries",
            "transfers",
        )
        assert cfg.sources.understat.season == "2024/2025"

    def test_json_roundtrip(self):
        """Serialise to JSON and back; all values are preserved."""
        cfg = IngestionConfig.model_validate(_VALID_CONFIG_DICT)
        restored = IngestionConfig.model_validate_json(cfg.model_dump_json())
        assert restored == cfg

    def test_json_schema_generation(self):
        """model_json_schema returns a valid object schema."""
        schema = IngestionConfig.model_json_schema()
        assert schema["type"] == "object"
        assert "sources" in schema["properties"]

    def test_rejects_extra_top_level_keys(self):
        """Top-level keys other than 'sources' are rejected."""
        with pytest.raises(ValidationError):
            IngestionConfig.model_validate({**_VALID_CONFIG_DICT, "metadata": {}})

    def test_is_frozen(self):
        """Root config is immutable after construction."""
        cfg = IngestionConfig.model_validate(_VALID_CONFIG_DICT)
        with pytest.raises(ValidationError):
            cfg.sources = SourcesConfig(**_VALID_SOURCES)  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────
# load_config
# ─────────────────────────────────────────────────────────────


class TestLoadConfig:
    """Tests for the load_config() YAML file loader."""

    def test_loads_valid_yaml(self, tmp_path: Path):
        """load_config returns a valid IngestionConfig for a correct YAML file."""
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(_VALID_YAML, encoding="utf-8")

        cfg = load_config(config_file)

        assert cfg.sources.api_football.league_id == 140
        assert cfg.sources.api_football.season == 2024
        assert cfg.sources.api_football.cache_ttl_hours == 168
        assert cfg.sources.understat.season == "2024/2025"

    def test_raises_on_invalid_season(self, tmp_path: Path):
        """load_config raises ConfigurationError when a field fails validation."""
        bad_yaml = _VALID_YAML.replace("season: 2024", "season: 1990")
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(bad_yaml, encoding="utf-8")

        with pytest.raises(ConfigurationError):
            load_config(config_file)

    def test_raises_cause_is_validation_error(self, tmp_path: Path):
        """ConfigurationError chains the original ValidationError via __cause__."""
        bad_yaml = _VALID_YAML.replace("season: 2024", "season: 1990")
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(bad_yaml, encoding="utf-8")

        with pytest.raises(ConfigurationError) as exc_info:
            load_config(config_file)

        assert isinstance(exc_info.value.__cause__, ValidationError)

    def test_raises_on_missing_file(self, tmp_path: Path):
        """load_config raises ConfigurationError when the file does not exist."""
        with pytest.raises(ConfigurationError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_raises_cause_is_file_not_found(self, tmp_path: Path):
        """ConfigurationError chains the original FileNotFoundError via __cause__."""
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(tmp_path / "nonexistent.yaml")

        assert isinstance(exc_info.value.__cause__, FileNotFoundError)

    def test_raises_on_missing_required_field(self, tmp_path: Path):
        """Missing required YAML field raises ConfigurationError."""
        bad_yaml = _VALID_YAML.replace("league_id: 140\n", "")
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(bad_yaml, encoding="utf-8")

        with pytest.raises(ConfigurationError):
            load_config(config_file)


# ─────────────────────────────────────────────────────────────
# get_config (singleton)
# ─────────────────────────────────────────────────────────────


class TestGetConfig:
    """Tests for the get_config() singleton accessor.

    Each test uses the ``reset_config_singleton`` fixture (defined in conftest.py)
    to ensure a clean module state before and after the test.
    """

    def test_returns_ingestion_config(self, tmp_path: Path, reset_config_singleton: None):
        """get_config returns a valid IngestionConfig instance."""
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(_VALID_YAML, encoding="utf-8")

        cfg = get_config(config_file)

        assert isinstance(cfg, IngestionConfig)
        assert cfg.sources.api_football.league_id == 140

    def test_same_object_returned_on_second_call(
        self, tmp_path: Path, reset_config_singleton: None
    ):
        """Two calls to get_config() return the identical object (singleton)."""
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(_VALID_YAML, encoding="utf-8")

        first = get_config(config_file)
        second = get_config(config_file)

        assert first is second

    def test_second_call_ignores_new_path(self, tmp_path: Path, reset_config_singleton: None):
        """After the first call, a different path argument is ignored."""
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(_VALID_YAML, encoding="utf-8")

        first = get_config(config_file)
        # Pass a non-existent path — should not raise because cache is already warm.
        second = get_config(tmp_path / "does_not_exist.yaml")

        assert first is second
