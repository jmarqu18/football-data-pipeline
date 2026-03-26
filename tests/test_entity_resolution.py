"""Tests for entity resolution between API-Football and Understat.

Covers:
- Name normalization utilities
- Name variant generation
- Fuzzy scoring
- Team resolution (exact + fuzzy)
- Player resolution with 20 known La Liga 2024/25 players
- Unresolved candidates report generation
"""

from __future__ import annotations

import csv

import pytest

from pipeline.entity_resolution import (
    best_match_score,
    build_name_variants,
    decode_api_name,
    normalize_name,
    resolve_players,
    resolve_teams,
    write_unresolved_report,
)
from pipeline.models.raw import (
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
    _APIFootballCards,
    _APIFootballDribbles,
    _APIFootballDuels,
    _APIFootballFouls,
    _APIFootballGames,
    _APIFootballGoals,
    _APIFootballPasses,
    _APIFootballPenalty,
    _APIFootballShots,
    _APIFootballTackles,
)

# ─────────────────────────────────────────────────────────────
# Helpers to build fixture objects
# ─────────────────────────────────────────────────────────────


def _make_api_player(
    player_id: int,
    name: str,
    firstname: str | None = None,
    lastname: str | None = None,
    birth_date: str | None = None,
    nationality: str | None = None,
) -> RawAPIFootballPlayer:
    return RawAPIFootballPlayer(
        player_id=player_id,
        name=name,
        firstname=firstname,
        lastname=lastname,
        birth_date=birth_date,
        nationality=nationality,
    )


_EMPTY_STATS_KWARGS = dict(
    shots=_APIFootballShots(),
    goals=_APIFootballGoals(),
    passes=_APIFootballPasses(),
    tackles=_APIFootballTackles(),
    duels=_APIFootballDuels(),
    dribbles=_APIFootballDribbles(),
    fouls=_APIFootballFouls(),
    cards=_APIFootballCards(),
    penalty=_APIFootballPenalty(),
)


def _make_api_stats(
    player_id: int,
    team_id: int,
    team_name: str,
    appearances: int = 0,
    minutes: int = 0,
) -> RawAPIFootballPlayerStats:
    return RawAPIFootballPlayerStats(
        player_id=player_id,
        team_id=team_id,
        team_name=team_name,
        league_id=140,
        season=2024,
        games=_APIFootballGames(appearances=appearances, minutes=minutes),
        **_EMPTY_STATS_KWARGS,
    )


def _make_understat_player(
    player_id: int,
    player_name: str,
    team: str,
    games: int = 0,
    minutes: int = 0,
) -> RawUnderstatPlayerSeason:
    return RawUnderstatPlayerSeason(
        player_id=player_id,
        player_name=player_name,
        team=team,
        season="2024/2025",
        games=games,
        minutes=minutes,
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
# Test: normalize_name
# ─────────────────────────────────────────────────────────────


class TestNormalizeName:
    def test_strips_diacritics(self):
        assert normalize_name("Vinícius Júnior") == "vinicius junior"

    def test_strips_spanish_accents(self):
        assert normalize_name("Álvaro Morata") == "alvaro morata"

    def test_handles_n_tilde(self):
        assert normalize_name("Iñaki Williams") == "inaki williams"

    def test_handles_nordic_characters(self):
        assert normalize_name("Alexander Sørloth") == "alexander sorloth"

    def test_lowercases(self):
        assert normalize_name("JUDE BELLINGHAM") == "jude bellingham"

    def test_collapses_whitespace(self):
        assert normalize_name("  Pedro   González   López  ") == "pedro gonzalez lopez"

    def test_empty_string(self):
        assert normalize_name("") == ""

    def test_decodes_html_entities_before_comparison(self):
        """HTML entities from API-Football are decoded so matching works correctly."""
        assert normalize_name("E. Eto&apos;o Pineda") == "e. eto'o pineda"
        assert normalize_name("Marcelo &amp; Silva") == "marcelo & silva"


# ─────────────────────────────────────────────────────────────
# Test: decode_api_name
# ─────────────────────────────────────────────────────────────


class TestDecodeApiName:
    def test_decodes_apostrophe_entity(self):
        assert decode_api_name("E. Eto&apos;o Pineda") == "E. Eto'o Pineda"

    def test_decodes_amp_entity(self):
        assert decode_api_name("Marcelo &amp; Silva") == "Marcelo & Silva"

    def test_passthrough_clean_name(self):
        assert decode_api_name("Robert Lewandowski") == "Robert Lewandowski"

    def test_decodes_numeric_entities(self):
        assert decode_api_name("Cami&#243;n") == "Camión"

    def test_noop_on_plain_name(self):
        assert decode_api_name("Robert Lewandowski") == "Robert Lewandowski"


# ─────────────────────────────────────────────────────────────
# Test: build_name_variants
# ─────────────────────────────────────────────────────────────


class TestBuildNameVariants:
    def test_full_name_only(self):
        variants = build_name_variants("Jude Bellingham")
        assert "jude bellingham" in variants

    def test_with_firstname_lastname(self):
        variants = build_name_variants(
            "Pedro González López", firstname="Pedro", lastname="González López"
        )
        assert "pedro gonzalez lopez" in variants
        assert "pedro" in variants
        assert "gonzalez lopez" in variants

    def test_no_duplicates(self):
        variants = build_name_variants(
            "Pedro González López", firstname="Pedro", lastname="González López"
        )
        assert len(variants) == len(set(variants))

    def test_none_fields_handled(self):
        variants = build_name_variants("Test Player", firstname=None, lastname=None)
        assert variants == ["test player"]


# ─────────────────────────────────────────────────────────────
# Test: best_match_score
# ─────────────────────────────────────────────────────────────


class TestBestMatchScore:
    def test_exact_match(self):
        score = best_match_score("Jude Bellingham", ["jude bellingham"])
        assert score == 1.0

    def test_partial_ratio_catches_nickname(self):
        # "Pedri" vs "Pedro" — partial_ratio should give a high score
        score = best_match_score("Pedri", ["pedro gonzalez lopez", "pedro", "gonzalez lopez"])
        assert score >= 0.80

    def test_accent_stripping_gives_high_score(self):
        score = best_match_score("Vinicius Junior", ["vinicius junior"])
        assert score == 1.0

    def test_empty_inputs(self):
        assert best_match_score("", ["test"]) == 0.0
        assert best_match_score("test", []) == 0.0


# ─────────────────────────────────────────────────────────────
# Test: resolve_teams
# ─────────────────────────────────────────────────────────────


class TestResolveTeams:
    def test_exact_match(self):
        from pipeline.models.raw import RawAPIFootballTeam

        api_teams = [
            RawAPIFootballTeam(team_id=529, name="Barcelona"),
            RawAPIFootballTeam(team_id=541, name="Real Madrid"),
        ]
        understat_teams = ["Barcelona", "Real Madrid"]
        result = resolve_teams(api_teams, understat_teams)
        resolved_names = {r.canonical_name for r in result if r.understat_name}
        assert "Barcelona" in resolved_names
        assert "Real Madrid" in resolved_names

    def test_fuzzy_match(self):
        from pipeline.models.raw import RawAPIFootballTeam

        api_teams = [RawAPIFootballTeam(team_id=530, name="Atletico Madrid")]
        understat_teams = ["Atlético de Madrid"]
        result = resolve_teams(api_teams, understat_teams)
        matched = [r for r in result if r.understat_name]
        assert len(matched) == 1
        assert matched[0].resolution_method == "fuzzy"

    def test_unmatched_api_team_kept(self):
        from pipeline.models.raw import RawAPIFootballTeam

        api_teams = [
            RawAPIFootballTeam(team_id=529, name="Barcelona"),
            RawAPIFootballTeam(team_id=999, name="Promoted Team"),
        ]
        understat_teams = ["Barcelona"]
        result = resolve_teams(api_teams, understat_teams)
        assert len(result) == 2

    def test_resolve_teams_propagates_metadata(self):
        from pipeline.models.raw import RawAPIFootballTeam

        api_teams = [
            RawAPIFootballTeam(
                team_id=529,
                name="Barcelona",
                code="BAR",
                country="Spain",
                founded=1899,
                logo_url="https://example.com/logo.png",
                venue_name="Camp Nou",
                venue_city="Barcelona",
                venue_capacity=55926,
                venue_surface="grass",
            )
        ]
        result = resolve_teams(api_teams, understat_teams=["Barcelona"])
        resolved = next(r for r in result if r.api_football_id == 529)
        assert resolved.country == "Spain"
        assert resolved.code == "BAR"
        assert resolved.founded == 1899
        assert resolved.venue_name == "Camp Nou"
        assert resolved.venue_capacity == 55926


# ─────────────────────────────────────────────────────────────
# Test: 20 known La Liga players (integration)
# ─────────────────────────────────────────────────────────────

# Team IDs
BARCA = 529
REAL_MADRID = 541
ATLETICO = 530
BETIS = 543
VILLARREAL = 533
ATHLETIC = 531
VALENCIA = 532
GETAFE = 546
GIRONA = 547

# Expected match pairs: (understat_id, api_football_id)
_EXPECTED_MATCHES = {
    (1001, 101),  # Bellingham
    (1002, 102),  # Lewandowski
    (1003, 103),  # Vinicius
    (1004, 104),  # Pedri
    (1005, 105),  # Rodrygo
    (1006, 106),  # Lamine Yamal
    (1007, 107),  # Griezmann
    (1008, 108),  # Koke (via stats)
    (1009, 109),  # Oblak
    (1010, 110),  # Isco (via stats)
    (1011, 111),  # Joselu
    (1012, 112),  # Dani Carvajal
    (1013, 113),  # Transfer Player
    (1014, 114),  # Sørloth
    (1015, 115),  # Morata
    (1018, 118),  # Ferran Torres
    (1019, 119),  # Iñaki Williams
    (1020, 120),  # Hugo Duro
}


@pytest.fixture()
def twenty_players_fixture():
    """Build the full 20-player test scenario."""
    # fmt: off
    api_players = [
        _make_api_player(101, "Jude Bellingham", "Jude", "Bellingham"),
        _make_api_player(102, "Robert Lewandowski", "Robert", "Lewandowski"),
        _make_api_player(
            103, "Vinícius José Paixão de Oliveira",
            "Vinícius", "José Paixão de Oliveira",
        ),
        _make_api_player(104, "Pedro González López", "Pedro", "González López"),
        _make_api_player(105, "Rodrygo Silva de Goes", "Rodrygo", "Silva de Goes"),
        _make_api_player(
            106, "Lamine Yamal Nasraoui Ebana", "Lamine", "Yamal Nasraoui Ebana",
        ),
        _make_api_player(107, "Antoine Griezmann", "Antoine", "Griezmann"),
        _make_api_player(
            108, "Jorge Resurrección Merodio", "Jorge", "Resurrección Merodio",
        ),
        _make_api_player(109, "Jan Oblak", "Jan", "Oblak"),
        _make_api_player(
            110, "Francisco Román Alarcón Suárez",
            "Francisco", "Román Alarcón Suárez",
        ),
        _make_api_player(
            111, "José Luis Mato Sanmartín", "José Luis", "Mato Sanmartín",
        ),
        _make_api_player(112, "Daniel Carvajal Ramos", "Daniel", "Carvajal Ramos"),
        _make_api_player(113, "Transfer Player", "Transfer", "Player"),
        _make_api_player(114, "Alexander Sorloth", "Alexander", "Sorloth"),
        _make_api_player(115, "Álvaro Morata", "Álvaro", "Morata"),
        # #16 — Only in Understat (no API-Football entry)
        _make_api_player(117, "Ronaldo Mysterio", "Ronaldo", "Mysterio"),
        _make_api_player(118, "Ferran Torres García", "Ferran", "Torres García"),
        _make_api_player(
            119, "Inaki Williams Arthuer", "Inaki", "Williams Arthuer",
        ),
        _make_api_player(120, "Hugo Duro Perales", "Hugo", "Duro Perales"),
        # Extra players for stat-pass testing — different stats
        _make_api_player(150, "Extra Atletico Player", "Extra", "Atletico Player"),
        _make_api_player(151, "Extra Betis Player", "Extra", "Betis Player"),
    ]
    # fmt: on

    api_stats = [
        _make_api_stats(101, REAL_MADRID, "Real Madrid", appearances=30, minutes=2500),
        _make_api_stats(102, BARCA, "Barcelona", appearances=32, minutes=2700),
        _make_api_stats(103, REAL_MADRID, "Real Madrid", appearances=28, minutes=2300),
        _make_api_stats(104, BARCA, "Barcelona", appearances=25, minutes=2100),
        _make_api_stats(105, REAL_MADRID, "Real Madrid", appearances=26, minutes=1800),
        _make_api_stats(106, BARCA, "Barcelona", appearances=30, minutes=2400),
        _make_api_stats(107, ATLETICO, "Atletico Madrid", appearances=29, minutes=2450),
        # Koke: unique stats in Atletico (30 games, 2600 min)
        _make_api_stats(108, ATLETICO, "Atletico Madrid", appearances=30, minutes=2600),
        _make_api_stats(109, ATLETICO, "Atletico Madrid", appearances=33, minutes=2970),
        # Isco: unique stats in Betis (22 games, 1500 min)
        _make_api_stats(110, BETIS, "Real Betis", appearances=22, minutes=1500),
        _make_api_stats(111, REAL_MADRID, "Real Madrid", appearances=20, minutes=1200),
        _make_api_stats(112, REAL_MADRID, "Real Madrid", appearances=15, minutes=1350),
        # Transfer Player: stats in Villarreal (was transferred from Getafe)
        _make_api_stats(113, VILLARREAL, "Villarreal", appearances=15, minutes=1200),
        _make_api_stats(114, ATLETICO, "Atletico Madrid", appearances=27, minutes=2200),
        _make_api_stats(115, ATLETICO, "Atletico Madrid", appearances=24, minutes=1800),
        _make_api_stats(117, GIRONA, "Girona", appearances=10, minutes=600),
        _make_api_stats(118, BARCA, "Barcelona", appearances=22, minutes=1600),
        _make_api_stats(119, ATHLETIC, "Athletic Club", appearances=31, minutes=2700),
        _make_api_stats(120, VALENCIA, "Valencia", appearances=28, minutes=2300),
        # Extra Atletico player: very different stats from Koke
        _make_api_stats(150, ATLETICO, "Atletico Madrid", appearances=5, minutes=200),
        # Extra Betis player: very different stats from Isco
        _make_api_stats(151, BETIS, "Real Betis", appearances=8, minutes=400),
    ]

    # fmt: off
    understat_players = [
        _make_understat_player(1001, "Jude Bellingham", "Real Madrid", 30, 2500),
        _make_understat_player(1002, "Robert Lewandowski", "Barcelona", 32, 2700),
        _make_understat_player(1003, "Vinicius Junior", "Real Madrid", 28, 2300),
        _make_understat_player(1004, "Pedri", "Barcelona", 25, 2100),
        _make_understat_player(1005, "Rodrygo", "Real Madrid", 26, 1800),
        _make_understat_player(1006, "Lamine Yamal", "Barcelona", 30, 2400),
        _make_understat_player(1007, "Antoine Griezmann", "Atletico Madrid", 29, 2450),
        # Koke: same team + unique stats (31 games, 2550 min ≈ 30/2600)
        _make_understat_player(1008, "Koke", "Atletico Madrid", 31, 2550),
        _make_understat_player(1009, "Jan Oblak", "Atletico Madrid", 33, 2970),
        # Isco: same team + unique stats (23 games, 1450 min ≈ 22/1500)
        _make_understat_player(1010, "Isco", "Real Betis", 23, 1450),
        _make_understat_player(1011, "Joselu", "Real Madrid", 20, 1200),
        _make_understat_player(1012, "Dani Carvajal", "Real Madrid", 15, 1350),
        # Transfer Player: Understat shows at Getafe (transferred from there)
        _make_understat_player(1013, "Transfer Player", "Getafe", 12, 900),
        _make_understat_player(1014, "Alexander Sørloth", "Atletico Madrid", 27, 2200),
        _make_understat_player(1015, "Álvaro Morata", "Atletico Madrid", 24, 1800),
        # #16 — Only in Understat
        _make_understat_player(1016, "Zinedine Phantom", "Girona", 5, 200),
        # #18-20
        _make_understat_player(1018, "Ferran Torres", "Barcelona", 22, 1600),
        _make_understat_player(1019, "Iñaki Williams", "Athletic Club", 31, 2700),
        _make_understat_player(1020, "Hugo Duro", "Valencia", 28, 2300),
    ]
    # fmt: on

    api_teams = [
        RawAPIFootballTeam(team_id=BARCA, name="Barcelona"),
        RawAPIFootballTeam(team_id=REAL_MADRID, name="Real Madrid"),
        RawAPIFootballTeam(team_id=ATLETICO, name="Atletico Madrid"),
        RawAPIFootballTeam(team_id=BETIS, name="Real Betis"),
        RawAPIFootballTeam(team_id=VILLARREAL, name="Villarreal"),
        RawAPIFootballTeam(team_id=ATHLETIC, name="Athletic Club"),
        RawAPIFootballTeam(team_id=VALENCIA, name="Valencia"),
        RawAPIFootballTeam(team_id=GETAFE, name="Getafe"),
        RawAPIFootballTeam(team_id=GIRONA, name="Girona"),
    ]
    understat_teams = [
        "Barcelona",
        "Real Madrid",
        "Atletico Madrid",
        "Real Betis",
        "Villarreal",
        "Athletic Club",
        "Valencia",
        "Getafe",
        "Girona",
    ]

    # Transfer: Player 113 transferred FROM Getafe TO Villarreal
    transfers = [
        RawAPIFootballTransfer(
            player_id=113,
            player_name="Transfer Player",
            date="2025-01-15",
            team_in_id=VILLARREAL,
            team_in_name="Villarreal",
            team_out_id=GETAFE,
            team_out_name="Getafe",
            type="Transfer",
        ),
    ]

    resolved_teams = resolve_teams(api_teams, understat_teams)

    return {
        "api_players": api_players,
        "api_stats": api_stats,
        "understat_players": understat_players,
        "resolved_teams": resolved_teams,
        "transfers": transfers,
    }


class TestTwentyPlayers:
    """Integration test with 20 known La Liga 2024/25 players."""

    def test_minimum_resolution_rate(self, twenty_players_fixture):
        """At least 14 of 16 matcheable players must be correctly resolved."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        # Count correctly matched pairs
        actual_matches = set()
        for p in result.resolved_players:
            if p.understat_id is not None and p.api_football_id is not None:
                actual_matches.add((p.understat_id, p.api_football_id))

        correct = actual_matches & _EXPECTED_MATCHES
        assert len(correct) >= 14, (
            f"Only {len(correct)}/16 correctly resolved (need ≥14). "
            f"Missing: {_EXPECTED_MATCHES - correct}"
        )

    def test_no_false_positives(self, twenty_players_fixture):
        """Unresolved single-source players must NOT be incorrectly matched."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        # Player 1016 (Zinedine Phantom — only in Understat) should not be cross-matched
        for p in result.resolved_players:
            if p.understat_id == 1016 and p.api_football_id is not None:
                pytest.fail(
                    f"Single-source Understat player was incorrectly matched to "
                    f"api_football_id={p.api_football_id} ({p.canonical_name})"
                )

        # Player 117 (Ronaldo Mysterio — only in API-Football) should not be cross-matched
        for p in result.resolved_players:
            if p.api_football_id == 117 and p.understat_id is not None:
                pytest.fail(
                    f"Single-source API-Football player was incorrectly matched to "
                    f"understat_id={p.understat_id}"
                )

    def test_no_wrong_matches(self, twenty_players_fixture):
        """No player should be matched to the wrong counterpart."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        for p in result.resolved_players:
            if p.understat_id is not None and p.api_football_id is not None:
                pair = (p.understat_id, p.api_football_id)
                assert pair in _EXPECTED_MATCHES, (
                    f"Wrong match: understat_id={p.understat_id} ↔ "
                    f"api_football_id={p.api_football_id} ({p.canonical_name})"
                )

    def test_pass4_resolves_statistical_cases(self, twenty_players_fixture):
        """Pass 4 (statistical fingerprint) should resolve at least 1 of Koke/Isco."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        statistical_matches = set()
        for p in result.resolved_players:
            if p.resolution_method == "statistical" and p.understat_id and p.api_football_id:
                statistical_matches.add((p.understat_id, p.api_football_id))

        koke_resolved = (1008, 108) in statistical_matches
        isco_resolved = (1010, 110) in statistical_matches
        assert koke_resolved or isco_resolved, (
            "Neither Koke nor Isco resolved via statistical fingerprint. "
            f"Statistical matches: {statistical_matches}"
        )

    def test_exact_match_players(self, twenty_players_fixture):
        """Players with identical names should resolve via Pass 1 (exact)."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        exact_ids = {
            p.understat_id for p in result.resolved_players if p.resolution_method == "exact"
        }
        # Bellingham, Lewandowski, Griezmann, Oblak should be exact
        for uid in [1001, 1002, 1007, 1009]:
            assert uid in exact_ids, f"Understat player {uid} should be exact match"

    def test_transfer_player_resolved(self, twenty_players_fixture):
        """Transfer Player should resolve via Pass 3 (contextual)."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        for p in result.resolved_players:
            if p.understat_id == 1013:
                assert p.api_football_id == 113
                assert p.resolution_method == "contextual"
                assert p.resolution_confidence == 0.70
                return
        pytest.fail("Transfer Player (understat_id=1013) was not resolved")

    def test_unresolved_in_report(self, twenty_players_fixture):
        """Unresolved Understat players should appear with top candidates."""
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        unresolved_ids = {u.player_id for u in result.unresolved}
        # "Only In Understat" (1016) should be unresolved
        assert 1016 in unresolved_ids, (
            "'Zinedine Phantom' (single-source) should be in unresolved list"
        )


class TestUnresolvedReport:
    def test_write_csv(self, twenty_players_fixture, tmp_path):
        result = resolve_players(
            twenty_players_fixture["api_players"],
            twenty_players_fixture["api_stats"],
            twenty_players_fixture["understat_players"],
            twenty_players_fixture["resolved_teams"],
            twenty_players_fixture["transfers"],
        )
        report_path = tmp_path / "unresolved.csv"
        write_unresolved_report(result.unresolved, report_path)

        assert report_path.exists()
        with report_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Should have at least the "Only In Understat" player
        assert len(rows) >= 1
        # Check CSV header
        assert "source" in reader.fieldnames
        assert "fuzzy_score" in reader.fieldnames
