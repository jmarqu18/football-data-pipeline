"""Ingestion configuration: Pydantic models and YAML loader.

The single source of truth for pipeline scope is ``config/ingestion.yaml``.
This module provides:

- A hierarchy of frozen Pydantic models that mirror the YAML structure.
- ``load_config(path)`` — pure function that reads and validates a YAML file.
- ``get_config()`` — module-level singleton accessor (loaded once per process).

Path resolution (``get_config`` with no argument):
  1. ``PIPELINE_CONFIG_PATH`` environment variable, if set.
  2. ``<repo_root>/config/ingestion.yaml``, where repo root is inferred from
     ``Path(__file__).resolve().parents[2]``
     (``src/pipeline/config.py`` → ``src/pipeline/`` → ``src/`` → repo root).
     This works for both editable installs (development) and Airflow containers
     with a mounted volume at the same relative path.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic import ValidationError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Exception
# ─────────────────────────────────────────────────────────────


class ConfigurationError(ValueError):
    """Raised when the ingestion config file is missing or contains invalid values."""


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────


class RateLimitConfig(BaseModel):
    """Rate-limiting parameters for the API-Football HTTP client.

    Args:
        max_calls_per_day: Maximum API calls allowed in a 24-hour window.
        delay_between_calls: Minimum pause in seconds between consecutive calls.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_calls_per_day: int = Field(ge=1, le=10_000)
    delay_between_calls: float = Field(ge=0.0)


class ApiFootballConfig(BaseModel):
    """Configuration for the API-Football data source.

    Args:
        league_id: Numeric league identifier (140 = La Liga).
        season: Season start year (e.g. 2024 for 2024/25).
        endpoints: Ordered list of endpoint keys to ingest.
        cache_dir: Directory for caching raw JSON responses.
        cache_ttl_hours: Cache time-to-live in hours (168 = 7 days).
        rate_limit: Rate-limiting parameters.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    league_id: int = Field(ge=1)
    season: int = Field(ge=2000, le=2100)
    endpoints: tuple[str, ...] = Field(min_length=1)
    cache_dir: Path
    cache_ttl_hours: int = Field(ge=1, le=8760)
    rate_limit: RateLimitConfig

    @field_validator("endpoints", mode="before")
    @classmethod
    def _coerce_endpoints_to_tuple(cls, v: object) -> tuple[str, ...]:
        """Convert a YAML list to a tuple.

        Pydantic frozen models require immutable collections.  YAML always
        deserialises sequences as ``list``, so this validator performs the
        coercion before the field type is checked.
        """
        if isinstance(v, list):
            return tuple(v)
        return v  # type: ignore[return-value]


class UnderstatConfig(BaseModel):
    """Configuration for the Understat data source (scraped via soccerdata).

    Args:
        league: Human-readable league name as expected by soccerdata.
        season: Season string in Understat format (e.g. ``"2024/2025"``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    league: str
    season: str


class FBrefConfig(BaseModel):
    """Configuration for the FBref data source (scraped via soccerdata).

    Args:
        league: Human-readable league name as expected by soccerdata.
        season: Season string in FBref format (e.g. ``"2024-2025"``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    league: str
    season: str


class SourcesConfig(BaseModel):
    """Container for all per-source configuration blocks.

    Args:
        api_football: API-Football source settings.
        understat: Understat source settings.
        fbref: FBref source settings.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    api_football: ApiFootballConfig
    understat: UnderstatConfig
    fbref: FBrefConfig


class IngestionConfig(BaseModel):
    """Root configuration model for the ingestion pipeline.

    Loaded from ``config/ingestion.yaml`` and validated at startup.

    Args:
        sources: Per-source configuration blocks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    sources: SourcesConfig


# ─────────────────────────────────────────────────────────────
# Path resolution
# ─────────────────────────────────────────────────────────────


def _default_config_path() -> Path:
    """Resolve the default config path.

    Returns:
        Path from ``PIPELINE_CONFIG_PATH`` env var if set, otherwise
        ``<repo_root>/config/ingestion.yaml`` derived from this file's location.
    """
    if env := os.environ.get("PIPELINE_CONFIG_PATH"):
        return Path(env)
    # src/pipeline/config.py → parents[0]=src/pipeline, [1]=src, [2]=repo root
    return Path(__file__).resolve().parents[2] / "config" / "ingestion.yaml"


# ─────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────


def load_config(path: Path) -> IngestionConfig:
    """Load and validate the ingestion configuration from a YAML file.

    Args:
        path: Absolute or relative path to the YAML configuration file.

    Returns:
        A validated, frozen ``IngestionConfig`` instance.

    Raises:
        ConfigurationError: If the file does not exist or contains invalid values.
            The original exception is always chained via ``__cause__``.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigurationError(f"Configuration file not found: {path}") from exc

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Failed to parse YAML from {path}: {exc}") from exc

    try:
        config = IngestionConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigurationError(
            f"Invalid configuration in {path}:\n{exc}"
        ) from exc

    logger.info(
        "Configuration loaded: league_id=%d season=%d endpoints=%s",
        config.sources.api_football.league_id,
        config.sources.api_football.season,
        config.sources.api_football.endpoints,
    )
    return config


# ─────────────────────────────────────────────────────────────
# Singleton accessor
# ─────────────────────────────────────────────────────────────

_config: IngestionConfig | None = None


def get_config(path: Path | None = None) -> IngestionConfig:
    """Return the cached ``IngestionConfig`` singleton.

    Loads the configuration from disk on the first call and caches it for
    the lifetime of the process.  Subsequent calls return the same instance.

    Args:
        path: Optional path to the YAML file.  If omitted, the default path
            is resolved via ``_default_config_path()``.

    Returns:
        The cached ``IngestionConfig`` instance.

    Raises:
        ConfigurationError: Propagated from ``load_config`` on first call.
    """
    global _config
    if _config is None:
        _config = load_config(path or _default_config_path())
    return _config
