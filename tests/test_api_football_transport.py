"""Tests for the API-Football transport (cache, rate limit, retry, pagination).

All tests use an injected mock ``httpx.Client`` — no real HTTP calls are made.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.config import ApiFootballConfig, RateLimitConfig
from pipeline.loaders.api_football_transport import (
    APIFootballError,
    APIFootballPlanRestricted,
    ApiFootballTransport,
)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict:
    with (_FIXTURES_DIR / name).open(encoding="utf-8") as f:
        return json.load(f)


def _make_config(tmp_path: Path, **overrides) -> ApiFootballConfig:
    """Build a minimal ``ApiFootballConfig`` pointing cache to *tmp_path*."""
    defaults = {
        "league_id": 140,
        "season": 2024,
        "endpoints": ("players_stats", "injuries", "transfers"),
        "cache_dir": tmp_path / "cache",
        "cache_ttl_hours": 168,
        "rate_limit": RateLimitConfig(max_calls_per_day=100, delay_between_calls=0.0),
    }
    defaults.update(overrides)
    return ApiFootballConfig(**defaults)


def _mock_response(data: dict, headers: dict | None = None) -> MagicMock:
    """Create a mock ``httpx.Response`` that behaves like the real thing."""
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    resp.headers = headers or {
        "x-ratelimit-requests-remaining": "95",
    }
    return resp


def _mock_client(responses: list[dict]) -> MagicMock:
    """Create a mock ``httpx.Client`` that returns *responses* in order."""
    client = MagicMock()
    client.get.side_effect = [_mock_response(d) for d in responses]
    return client


# ─────────────────────────────────────────────────────────────
# Cache logic
# ─────────────────────────────────────────────────────────────


class TestCacheLogic:
    """Tests for cache-first HTTP behaviour."""

    def test_cache_miss_makes_api_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)

        transport = ApiFootballTransport(config, "test-key", client=client)
        result = transport.request("players", {"league": 140, "season": 2024, "page": 1})

        assert result["results"] == 2
        client.get.assert_called_once()

    def test_cache_hit_skips_api_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path)

        # Pre-populate cache
        cache_file = tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)

        client = _mock_client([])  # No responses needed
        transport = ApiFootballTransport(config, "test-key", client=client)
        result = transport.request("players", {"league": 140, "page": 1, "season": 2024})

        assert result["results"] == 2
        client.get.assert_not_called()

    def test_cache_expired_makes_api_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path, cache_ttl_hours=1)

        # Pre-populate with old mtime
        cache_file = tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)
        # Set mtime to 2 hours ago
        old_time = time.time() - 7200
        os.utime(cache_file, (old_time, old_time))

        client = _mock_client([fixture])
        transport = ApiFootballTransport(config, "test-key", client=client)
        transport.request("players", {"league": 140, "page": 1, "season": 2024})

        client.get.assert_called_once()

    def test_cache_file_created_after_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)

        transport = ApiFootballTransport(config, "test-key", client=client)
        transport.request("players", {"league": 140, "page": 1, "season": 2024})

        cache_file = tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        assert cache_file.exists()
        with cache_file.open(encoding="utf-8") as f:
            cached = json.load(f)
        assert cached["results"] == 2

    def test_force_refresh_ignores_cache(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path)

        # Pre-populate cache
        cache_file = tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)

        client = _mock_client([fixture])
        transport = ApiFootballTransport(config, "test-key", client=client)
        transport.request(
            "players",
            {"league": 140, "page": 1, "season": 2024},
            force_refresh=True,
        )

        client.get.assert_called_once()


# ─────────────────────────────────────────────────────────────
# Rate limiting
# ─────────────────────────────────────────────────────────────


class TestRateLimiting:
    """Tests for rate limit enforcement."""

    def test_raises_when_daily_limit_exceeded(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(
            tmp_path,
            rate_limit=RateLimitConfig(max_calls_per_day=2, delay_between_calls=0.0),
        )
        client = _mock_client([fixture, fixture, fixture])
        transport = ApiFootballTransport(config, "test-key", client=client)

        # First two calls succeed
        transport.request("players", {"league": 140, "page": 1, "season": 2024})
        transport.request("players", {"league": 140, "page": 2, "season": 2024})

        # Third should raise
        with pytest.raises(APIFootballError, match="rate limit exhausted"):
            transport.request("players", {"league": 140, "page": 3, "season": 2024})

    def test_api_errors_raise(self, tmp_path: Path) -> None:
        error_response = {
            "get": "players",
            "parameters": {},
            "errors": {"token": "Error/Missing application key"},
            "results": 0,
            "paging": {"current": 1, "total": 1},
            "response": [],
        }
        client = _mock_client([error_response])
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "bad-key", client=client)

        with pytest.raises(APIFootballError, match="API-Football error"):
            transport.request("players", {"league": 140, "page": 1, "season": 2024})

    def test_plan_restriction_raises_actionable_error(self, tmp_path: Path) -> None:
        error_response = {
            "get": "teams",
            "parameters": {},
            "errors": {"plan": "Free plans do not have access to this season, try from 2022 to 2024."},
            "results": 0,
            "paging": {"current": 1, "total": 1},
            "response": [],
        }
        client = _mock_client([error_response])
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "bad-key", client=client)

        with pytest.raises(APIFootballPlanRestricted, match="plan restriction") as exc_info:
            transport.request("teams", {"league": 140, "season": 2025})
        assert "2025" in str(exc_info.value)
        assert "2022" in str(exc_info.value)

    def test_page_limit_restriction_message(self, tmp_path: Path) -> None:
        error_response = {
            "get": "players",
            "parameters": {},
            "errors": {"plan": "Free plans are limited to a maximum value of 3 for the Page parameter"},
            "results": 0,
            "paging": {"current": 1, "total": 1},
            "response": [],
        }
        client = _mock_client([error_response])
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "bad-key", client=client)

        with pytest.raises(APIFootballPlanRestricted, match="page") as exc_info:
            transport.request("players", {"league": 140, "season": 2024, "team": 530, "page": 4})
        assert "3" in str(exc_info.value)
        assert "degrades gracefully" in str(exc_info.value)


# ─────────────────────────────────────────────────────────────
# Pagination
# ─────────────────────────────────────────────────────────────


class TestPagination:
    """Tests for automatic pagination."""

    def test_paginates_until_last_page(self, tmp_path: Path) -> None:
        base = _load_fixture("api_football_players_response.json")

        page1 = {**base, "paging": {"current": 1, "total": 3}}
        page2 = {**base, "paging": {"current": 2, "total": 3}}
        page3 = {**base, "paging": {"current": 3, "total": 3}}

        client = _mock_client([page1, page2, page3])
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "test-key", client=client)

        items, total_pages = transport.paginate("players", {"league": 140, "season": 2024})

        assert client.get.call_count == 3
        # 2 items per page × 3 pages = 6 items
        assert len(items) == 6
        assert total_pages == 3

    def test_single_page_no_extra_calls(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        # paging.total is already 1 in the fixture
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "test-key", client=client)

        items, total_pages = transport.paginate("players", {"league": 140, "season": 2024})

        assert client.get.call_count == 1
        assert len(items) == 2
        assert total_pages == 1


# ─────────────────────────────────────────────────────────────
# Context manager / client ownership
# ─────────────────────────────────────────────────────────────


class TestContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_closes_owned_client(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        with ApiFootballTransport(config, "test-key") as transport:
            assert transport._owns_client is True
        # Should not raise after exit

    def test_injected_client_not_closed(self, tmp_path: Path) -> None:
        client = MagicMock()
        config = _make_config(tmp_path)
        transport = ApiFootballTransport(config, "test-key", client=client)
        transport.close()

        client.close.assert_not_called()
