"""Tests for fixture-based recovery of players truncated by the free-tier page cap.

All tests use an injected mock ``httpx.Client`` on the transport — no real
HTTP calls are made.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from pipeline.config import ApiFootballConfig, RateLimitConfig
from pipeline.loaders.api_football_recovery import FixtureRecovery
from pipeline.loaders.api_football_transport import ApiFootballTransport

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


def _make_recovery(tmp_path: Path, responses: list[dict], **config_overrides) -> FixtureRecovery:
    config = _make_config(tmp_path, **config_overrides)
    transport = ApiFootballTransport(config, "test-key", client=_mock_client(responses))
    return FixtureRecovery(transport, config)


# ─────────────────────────────────────────────────────────────
# fetch_fixtures
# ─────────────────────────────────────────────────────────────


class TestFetchFixtures:
    def test_fetch_fixtures_omits_page_param(self, tmp_path: Path) -> None:
        # The /fixtures list endpoint rejects the `page` parameter
        # ("The Page field do not exist"), so it must not be sent.
        fixtures = {
            "get": "fixtures",
            "parameters": {},
            "errors": [],
            "results": 0,
            "paging": {"current": 1, "total": 1},
            "response": [],
        }
        config = _make_config(tmp_path)
        client = _mock_client([fixtures])
        transport = ApiFootballTransport(config, "test-key", client=client)
        recovery = FixtureRecovery(transport, config)

        recovery.fetch_fixtures(530)

        args, kwargs = client.get.call_args
        assert args[0] == "/fixtures"
        assert "page" not in kwargs.get("params", {})


# ─────────────────────────────────────────────────────────────
# _aggregate_fixture_stats
# ─────────────────────────────────────────────────────────────


class TestAggregateFixtureStats:
    def test_aggregate_fixture_stats_sums_and_averages(self, tmp_path: Path) -> None:
        s1 = {
            "games": {"appearences": 1, "lineups": 1, "minutes": 90, "position": "Defender", "rating": "7.0"},
            "shots": {"total": 2, "on": 1},
            "goals": {"total": 1},
            "passes": {"total": 50, "key": 3, "accuracy": 80},
            "tackles": {"total": 5, "blocks": 1, "interceptions": 2},
            "duels": {"total": 10, "won": 6},
            "dribbles": {"attempts": 3, "success": 2, "past": 1},
            "fouls": {"drawn": 2, "committed": 1},
            "cards": {"yellow": 1},
            "penalty": {"won": 1},
        }
        s2 = {
            "games": {"appearences": 1, "lineups": 0, "minutes": 20, "position": "Defender", "rating": "6.0"},
            "shots": {"total": 1, "on": 0},
            "goals": {"total": 0},
            "passes": {"total": 10, "key": 0, "accuracy": 70},
            "tackles": {"total": 1},
            "duels": {"total": 3, "won": 1},
            "dribbles": {"attempts": 1, "success": 0, "past": 0},
            "fouls": {"drawn": 0, "committed": 2},
            "cards": {"yellow": 0},
            "penalty": {"won": 0},
        }
        agg = FixtureRecovery._aggregate_fixture_stats(
            [s1, s2], player_id=777, team_id=530, team_name="Atletico Madrid", league_id=140, season=2024
        )
        assert agg["games"]["appearances"] == 2
        assert agg["games"]["minutes"] == 110
        assert agg["games"]["position"] == "Defender"
        assert abs(float(agg["games"]["rating"]) - 6.5) < 1e-6
        assert agg["goals"]["total"] == 1
        assert agg["shots"]["total"] == 3
        assert agg["passes"]["total"] == 60
        assert agg["passes"]["accuracy"] == 75
        assert agg["tackles"]["total"] == 6
        assert agg["cards"]["yellow"] == 1
        assert agg["penalty"]["won"] == 1


# ─────────────────────────────────────────────────────────────
# recover
# ─────────────────────────────────────────────────────────────


class TestRecover:
    def test_recover_via_fixtures(self, tmp_path: Path) -> None:
        fixtures = _load_fixture("api_football_fixtures_response.json")
        fp1 = _load_fixture("api_football_fixtures_players_9001.json")
        fp2 = _load_fixture("api_football_fixtures_players_9002.json")
        recovery = _make_recovery(tmp_path, [fixtures, fp1, fp2])
        known = {1100}  # base /players already returned this one

        players, stats = recovery.recover({530}, known)

        assert len(players) == 1
        assert players[0].player_id == 777
        assert players[0].name == "Recovery Test"
        assert 777 in known  # mutated in place
        assert len(stats) == 1
        assert stats[0].games.appearances == 2
        assert stats[0].games.minutes == 110
        assert abs(float(stats[0].games.rating) - 6.5) < 1e-6
        assert stats[0].goals.total == 1
        assert stats[0].passes.total == 60
        assert stats[0].passes.accuracy == 75
        # opponent player (999) must be ignored — different team_id
        assert all(s.team_id == 530 for s in stats)

    def test_recover_is_noop_when_nothing_truncated(self, tmp_path: Path) -> None:
        recovery = _make_recovery(tmp_path, [])

        players, stats = recovery.recover(set(), {1, 2, 3})

        assert players == []
        assert stats == []
