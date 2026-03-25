"""Tests for the API-Football loader.

All tests use an injected mock ``httpx.Client`` — no real HTTP calls are made.
Fixtures replicate the raw API-Football response envelope with its known
field-name typos (``appearences``, ``commited``).
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest

from pipeline.config import ApiFootballConfig, RateLimitConfig
from pipeline.loaders.api_football_loader import APIFootballError, APIFootballLoader
from pipeline.models.raw import (
    RawAPIFootballInjury,
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTransfer,
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
        "rate_limit": RateLimitConfig(
            max_calls_per_day=100, delay_between_calls=0.0
        ),
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

        loader = APIFootballLoader(config, "test-key", client=client)
        result = loader._make_request("players", {"league": 140, "season": 2024, "page": 1})

        assert result["results"] == 2
        client.get.assert_called_once()

    def test_cache_hit_skips_api_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path)

        # Pre-populate cache
        cache_file = (
            tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)

        client = _mock_client([])  # No responses needed
        loader = APIFootballLoader(config, "test-key", client=client)
        result = loader._make_request("players", {"league": 140, "page": 1, "season": 2024})

        assert result["results"] == 2
        client.get.assert_not_called()

    def test_cache_expired_makes_api_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path, cache_ttl_hours=1)

        # Pre-populate with old mtime
        cache_file = (
            tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)
        # Set mtime to 2 hours ago
        old_time = time.time() - 7200
        os.utime(cache_file, (old_time, old_time))

        client = _mock_client([fixture])
        loader = APIFootballLoader(config, "test-key", client=client)
        loader._make_request("players", {"league": 140, "page": 1, "season": 2024})

        client.get.assert_called_once()

    def test_cache_file_created_after_call(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)

        loader = APIFootballLoader(config, "test-key", client=client)
        loader._make_request("players", {"league": 140, "page": 1, "season": 2024})

        cache_file = (
            tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        )
        assert cache_file.exists()
        with cache_file.open(encoding="utf-8") as f:
            cached = json.load(f)
        assert cached["results"] == 2

    def test_force_refresh_ignores_cache(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        config = _make_config(tmp_path)

        # Pre-populate cache
        cache_file = (
            tmp_path / "cache" / "players" / "league_140_page_1_season_2024.json"
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)

        client = _mock_client([fixture])
        loader = APIFootballLoader(config, "test-key", client=client)
        loader._make_request(
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
            rate_limit=RateLimitConfig(
                max_calls_per_day=2, delay_between_calls=0.0
            ),
        )
        client = _mock_client([fixture, fixture, fixture])
        loader = APIFootballLoader(config, "test-key", client=client)

        # First two calls succeed
        loader._make_request("players", {"league": 140, "page": 1, "season": 2024})
        loader._make_request("players", {"league": 140, "page": 2, "season": 2024})

        # Third should raise
        with pytest.raises(APIFootballError, match="rate limit exhausted"):
            loader._make_request("players", {"league": 140, "page": 3, "season": 2024})

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
        loader = APIFootballLoader(config, "bad-key", client=client)

        with pytest.raises(APIFootballError, match="API-Football error"):
            loader._make_request("players", {"league": 140, "page": 1, "season": 2024})


# ─────────────────────────────────────────────────────────────
# Player extraction
# ─────────────────────────────────────────────────────────────


class TestPlayerExtraction:
    """Tests for player and player stats extraction from raw API JSON."""

    def test_extract_player_from_api_response(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        raw_item = fixture["response"][0]
        result = APIFootballLoader._extract_player(raw_item)

        assert result["player_id"] == 1100
        assert result["name"] == "Pedro González López"
        assert result["birth_date"] == "2002-11-25"
        assert result["photo_url"] == "https://media.api-sports.io/football/players/1100.png"
        assert result["height"] == "174 cm"
        assert result["weight"] == "60 kg"

    def test_extract_player_validates_as_model(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        raw_item = fixture["response"][0]
        result = APIFootballLoader._extract_player(raw_item)
        player = RawAPIFootballPlayer.model_validate(result)

        assert player.player_id == 1100
        assert player.name == "Pedro González López"

    def test_extract_player_stats_fixes_typos(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        stat = fixture["response"][0]["statistics"][0]
        result = APIFootballLoader._extract_player_stats(1100, stat)

        # Typo "appearences" should be mapped to "appearances"
        assert result["games"]["appearances"] == 28
        # Typo "commited" should be mapped to "committed"
        assert result["penalty"]["committed"] is None

    def test_extract_player_stats_validates_as_model(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        stat = fixture["response"][0]["statistics"][0]
        result = APIFootballLoader._extract_player_stats(1100, stat)
        model = RawAPIFootballPlayerStats.model_validate(result)

        assert model.player_id == 1100
        assert model.team_id == 529
        assert model.season == 2024
        assert model.games.appearances == 28
        assert model.games.position == "Midfielder"

    def test_extract_player_handles_nulls(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        raw_item = fixture["response"][1]  # Edge case player with nulls
        result = APIFootballLoader._extract_player(raw_item)

        assert result["player_id"] == 99999
        assert result["firstname"] is None
        assert result["birth_date"] is None
        assert result["photo_url"] is None

        player = RawAPIFootballPlayer.model_validate(result)
        assert player.age is None
        assert player.nationality is None

    def test_extract_player_stats_handles_nulls(self) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        stat = fixture["response"][1]["statistics"][0]
        result = APIFootballLoader._extract_player_stats(99999, stat)
        model = RawAPIFootballPlayerStats.model_validate(result)

        assert model.games.appearances is None
        assert model.shots.total is None
        assert model.goals.total is None

    def test_invalid_player_logged_and_skipped(self, tmp_path: Path) -> None:
        # Create a response with an invalid player (missing required 'name')
        bad_response = {
            "get": "players",
            "parameters": {},
            "errors": [],
            "results": 1,
            "paging": {"current": 1, "total": 1},
            "response": [
                {
                    "player": {
                        "id": -1,
                        "name": "Bad Player",
                        "firstname": None,
                        "lastname": None,
                        "age": None,
                        "birth": {"date": None, "place": None, "country": None},
                        "nationality": None,
                        "height": None,
                        "weight": None,
                        "injured": False,
                        "photo": None,
                    },
                    "statistics": [],
                }
            ],
        }
        client = _mock_client([bad_response])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        players, stats = loader.ingest_players()

        # Invalid player_id=-1 (ge=1 constraint) → rejected
        assert len(players) == 0
        assert len(stats) == 0

    def test_ingest_players_per_team_two_calls(self, tmp_path: Path) -> None:
        """ingest_players(team_ids=...) makes one paginated call per team."""
        fixture = _load_fixture("api_football_players_response.json")
        team1_resp = {**fixture, "paging": {"current": 1, "total": 1}}
        team2_resp = {**fixture, "paging": {"current": 1, "total": 1}}

        client = _mock_client([team1_resp, team2_resp])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        players, stats = loader.ingest_players(team_ids=[529, 541])

        # 2 unique player profiles (deduped by player_id across teams)
        assert len(players) == 2
        # 2 stats per team × 2 teams = 4 stat rows (player×team granularity)
        assert len(stats) == 4
        assert client.get.call_count == 2

    def test_ingest_players_per_team_keeps_stats_per_team(self, tmp_path: Path) -> None:
        """A transferred player produces one profile but one stat row per team."""
        fixture = _load_fixture("api_football_players_response.json")
        team1_resp = {**fixture, "paging": {"current": 1, "total": 1}}
        # Team 541 returns only player 1100 with team-541 stats
        team2_item = {
            "player": fixture["response"][0]["player"],
            "statistics": [{
                **fixture["response"][0]["statistics"][0],
                "team": {"id": 541, "name": "Real Madrid",
                         "logo": "https://media.api-sports.io/football/teams/541.png"},
            }],
        }
        team2_resp = {
            **fixture,
            "results": 1,
            "paging": {"current": 1, "total": 1},
            "response": [team2_item],
        }

        client = _mock_client([team1_resp, team2_resp])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        players, stats = loader.ingest_players(team_ids=[529, 541])

        # Player 1100 appears in both teams → 1 profile, 2 separate stat rows
        assert len([p for p in players if p.player_id == 1100]) == 1
        player_1100_stats = [s for s in stats if s.player_id == 1100]
        assert len(player_1100_stats) == 2  # team 529 + team 541
        assert {s.team_id for s in player_1100_stats} == {529, 541}

    def test_ingest_players_league_fallback(self, tmp_path: Path) -> None:
        """ingest_players() without team_ids falls back to league-level pagination."""
        fixture = _load_fixture("api_football_players_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        players, stats = loader.ingest_players()

        assert len(players) == 2
        assert client.get.call_count == 1


# ─────────────────────────────────────────────────────────────
# Injury extraction
# ─────────────────────────────────────────────────────────────


class TestInjuryExtraction:
    """Tests for injury extraction from raw API JSON."""

    def test_extract_injury_from_api_response(self) -> None:
        fixture = _load_fixture("api_football_injuries_response.json")
        raw_item = fixture["response"][0]
        result = APIFootballLoader._extract_injury(raw_item)

        assert result["player_id"] == 1100
        assert result["player_name"] == "Pedri"
        assert result["team_id"] == 529
        assert result["team_name"] == "Barcelona"
        assert result["fixture_id"] == 12345
        assert result["league_id"] == 140
        assert result["reason"] == "Muscle Injury"
        assert result["type"] == "Muscle Injury"
        assert result["date"] == "2024-10-15"

    def test_extract_injury_validates_as_model(self) -> None:
        fixture = _load_fixture("api_football_injuries_response.json")
        raw_item = fixture["response"][0]
        result = APIFootballLoader._extract_injury(raw_item)
        model = RawAPIFootballInjury.model_validate(result)

        assert model.player_id == 1100
        assert model.fixture_id == 12345

    def test_extract_injury_nullable_fixture(self) -> None:
        fixture = _load_fixture("api_football_injuries_response.json")
        raw_item = fixture["response"][1]  # Gavi — null fixture
        result = APIFootballLoader._extract_injury(raw_item)

        assert result["fixture_id"] is None
        assert result["date"] == "unknown"

        model = RawAPIFootballInjury.model_validate(result)
        assert model.fixture_id is None

    def test_ingest_injuries_end_to_end(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_injuries_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        injuries = loader.ingest_injuries()

        assert len(injuries) == 2
        assert all(isinstance(i, RawAPIFootballInjury) for i in injuries)


# ─────────────────────────────────────────────────────────────
# Transfer extraction
# ─────────────────────────────────────────────────────────────


class TestTransferExtraction:
    """Tests for transfer extraction from raw API JSON."""

    def test_extract_transfer_from_api_response(self) -> None:
        fixture = _load_fixture("api_football_transfers_response.json")
        player = fixture["response"][0]
        transfer = player["transfers"][0]
        result = APIFootballLoader._extract_transfer(
            player["player"]["id"], player["player"]["name"], transfer
        )

        assert result["player_id"] == 276
        assert result["player_name"] == "Neymar"
        assert result["date"] == "2017-08-03"
        assert result["team_in_id"] == 85
        assert result["team_in_name"] == "Paris Saint Germain"
        assert result["team_out_id"] == 529
        assert result["team_out_name"] == "Barcelona"
        assert result["type"] == "€ 222M"

    def test_extract_transfer_validates_as_model(self) -> None:
        fixture = _load_fixture("api_football_transfers_response.json")
        player = fixture["response"][0]
        transfer = player["transfers"][0]
        result = APIFootballLoader._extract_transfer(
            player["player"]["id"], player["player"]["name"], transfer
        )
        model = RawAPIFootballTransfer.model_validate(result)

        assert model.player_id == 276
        assert model.team_in_id == 85

    def test_extract_transfer_null_team_out(self) -> None:
        fixture = _load_fixture("api_football_transfers_response.json")
        player = fixture["response"][1]  # Lamine Yamal — null team_out
        transfer = player["transfers"][0]
        result = APIFootballLoader._extract_transfer(
            player["player"]["id"], player["player"]["name"], transfer
        )

        assert result["team_out_id"] is None
        assert result["team_out_name"] is None

        model = RawAPIFootballTransfer.model_validate(result)
        assert model.team_out_id is None

    def test_ingest_transfers_end_to_end(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_transfers_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        transfers = loader.ingest_transfers([529])

        assert len(transfers) == 2
        assert all(isinstance(t, RawAPIFootballTransfer) for t in transfers)


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
        loader = APIFootballLoader(config, "test-key", client=client)

        items = loader._paginate("players", {"league": 140, "season": 2024})

        assert client.get.call_count == 3
        # 2 items per page × 3 pages = 6 items
        assert len(items) == 6

    def test_single_page_no_extra_calls(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        # paging.total is already 1 in the fixture
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        items = loader._paginate("players", {"league": 140, "season": 2024})

        assert client.get.call_count == 1
        assert len(items) == 2


# ─────────────────────────────────────────────────────────────
# Parquet output
# ─────────────────────────────────────────────────────────────


class TestParquetOutput:
    """Tests for Parquet serialisation."""

    def test_save_parquet_creates_file(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        raw_item = fixture["response"][0]
        player_dict = APIFootballLoader._extract_player(raw_item)
        player = RawAPIFootballPlayer.model_validate(player_dict)

        out_path = tmp_path / "output" / "players.parquet"
        APIFootballLoader.save_parquet([player], out_path)

        assert out_path.exists()
        table = pq.read_table(out_path)
        assert table.num_rows == 1
        assert "player_id" in table.column_names

    def test_save_parquet_nested_structs(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_players_response.json")
        stat_dict = APIFootballLoader._extract_player_stats(
            1100, fixture["response"][0]["statistics"][0]
        )
        stat = RawAPIFootballPlayerStats.model_validate(stat_dict)

        out_path = tmp_path / "output" / "stats.parquet"
        APIFootballLoader.save_parquet([stat], out_path)

        table = pq.read_table(out_path)
        assert table.num_rows == 1
        assert "games" in table.column_names
        assert "penalty" in table.column_names

    def test_save_parquet_empty_list_logs_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        out_path = tmp_path / "output" / "empty.parquet"
        APIFootballLoader.save_parquet([], out_path)

        assert not out_path.exists()
        assert "No records to save" in caplog.text


# ─────────────────────────────────────────────────────────────
# Ingest all
# ─────────────────────────────────────────────────────────────


class TestIngestAll:
    """Tests for the ``ingest_all`` orchestration method."""

    def test_ingest_all_runs_configured_endpoints(self, tmp_path: Path) -> None:
        teams_resp = _load_fixture("api_football_teams_response.json")
        players_resp = _load_fixture("api_football_players_response.json")
        injuries_resp = _load_fixture("api_football_injuries_response.json")
        transfers_resp = _load_fixture("api_football_transfers_response.json")

        # Call order: teams (1) → players for team 529 (1) → players for team 541 (1)
        #             → injuries (1) → transfers for team 529 (1) → transfers for team 541 (1)
        players_page = {**players_resp, "paging": {"current": 1, "total": 1}}
        client = _mock_client([
            teams_resp,      # fetch_team_ids → [529, 541]
            players_page,    # players for team 529
            players_page,    # players for team 541
            injuries_resp,   # injuries
            transfers_resp,  # transfers for team 529
            transfers_resp,  # transfers for team 541
        ])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        out_dir = tmp_path / "raw"
        counts = loader.ingest_all(output_dir=out_dir)

        assert counts["players"] == 2    # 2 unique profiles (deduped by player_id)
        assert counts["player_stats"] == 4  # 2 players × 2 teams
        assert counts["injuries"] == 2
        assert counts["transfers"] == 4  # 2 per team × 2 teams

        assert (out_dir / "players.parquet").exists()
        assert (out_dir / "player_stats.parquet").exists()
        assert (out_dir / "injuries.parquet").exists()
        assert (out_dir / "transfers.parquet").exists()

    def test_ingest_all_logs_summary(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        teams_resp = _load_fixture("api_football_teams_response.json")
        players_resp = _load_fixture("api_football_players_response.json")
        injuries_resp = _load_fixture("api_football_injuries_response.json")
        transfers_resp = _load_fixture("api_football_transfers_response.json")

        players_page = {**players_resp, "paging": {"current": 1, "total": 1}}
        client = _mock_client([
            teams_resp, players_page, players_page,
            injuries_resp, transfers_resp, transfers_resp,
        ])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        with caplog.at_level("INFO"):
            loader.ingest_all(output_dir=tmp_path / "raw")

        assert "Ingest complete" in caplog.text

    def test_ingest_all_injuries_only_no_team_fetch(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        injuries_resp = _load_fixture("api_football_injuries_response.json")

        client = _mock_client([injuries_resp])
        config = _make_config(tmp_path, endpoints=("injuries",))
        loader = APIFootballLoader(config, "test-key", client=client)

        counts = loader.ingest_all(output_dir=tmp_path / "raw")

        assert counts["injuries"] == 2
        assert "players" not in counts
        # Only 1 call (injuries), no teams call
        assert client.get.call_count == 1


# ─────────────────────────────────────────────────────────────
# Team IDs
# ─────────────────────────────────────────────────────────────


class TestFetchTeamIds:
    """Tests for fetch_team_ids — single-call team discovery."""

    def test_fetch_team_ids_returns_sorted_ids(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        fixture = _load_fixture("api_football_teams_response.json")
        client = _mock_client([fixture])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        with caplog.at_level("INFO"):
            team_ids = loader.fetch_team_ids()

        assert team_ids == [529, 541]
        client.get.assert_called_once()
        assert "Fetched 2 team IDs" in caplog.text

    def test_fetch_team_ids_uses_cache(self, tmp_path: Path) -> None:
        fixture = _load_fixture("api_football_teams_response.json")
        config = _make_config(tmp_path)

        # Pre-populate cache
        cache_file = (
            tmp_path / "cache" / "teams" / "league_140_season_2024.json"
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(fixture, f)

        client = _mock_client([])
        loader = APIFootballLoader(config, "test-key", client=client)

        team_ids = loader.fetch_team_ids()

        assert team_ids == [529, 541]
        client.get.assert_not_called()

    def test_fetch_team_ids_empty_response_raises(self, tmp_path: Path) -> None:
        empty_response = {
            "get": "teams",
            "parameters": {"league": "140", "season": "2024"},
            "errors": [],
            "results": 0,
            "paging": {"current": 1, "total": 1},
            "response": [],
        }
        client = _mock_client([empty_response])
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)

        with pytest.raises(APIFootballError, match="No teams found"):
            loader.fetch_team_ids()


# ─────────────────────────────────────────────────────────────
# Context manager
# ─────────────────────────────────────────────────────────────


class TestContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_closes_owned_client(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        with APIFootballLoader(config, "test-key") as loader:
            assert loader._owns_client is True
        # Should not raise after exit

    def test_injected_client_not_closed(self, tmp_path: Path) -> None:
        client = MagicMock()
        config = _make_config(tmp_path)
        loader = APIFootballLoader(config, "test-key", client=client)
        loader.close()

        client.close.assert_not_called()
