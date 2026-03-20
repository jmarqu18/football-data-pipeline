"""Tests unitarios para modelos Pydantic de capa RAW.

Cada test usa datos realistas modelados a partir de las fuentes reales
(StatsBomb, Understat, FBref) para validar esquema y tipos básicos.
"""

import pytest
from pydantic import ValidationError

from pipeline.models.raw import (
    RawFBrefPlayerSeason,
    RawStatsBombEvent,
    RawStatsBombMatch,
    RawUnderstatShot,
)

# ─────────────────────────────────────────────────────────────
# RawStatsBombEvent
# ─────────────────────────────────────────────────────────────


class TestRawStatsBombEvent:
    """Tests para eventos crudos provenientes de StatsBomb."""

    def test_valid_event_with_all_fields(self):
        """Un evento completo con todos los campos requeridos se crea correctamente."""
        event = RawStatsBombEvent(
            id="8fac8b14-0d02-4036-a8e4-5e4f8f21cb09",
            type="Pass",
            player="Lionel Andrés Messi Cuccittini",
            player_id=5503,
            team="Argentina",
            location=[60.0, 40.0],
            period=1,
            minute=23,
            second=14,
        )
        assert event.id == "8fac8b14-0d02-4036-a8e4-5e4f8f21cb09"
        assert event.type == "Pass"
        assert event.player == "Lionel Andrés Messi Cuccittini"
        assert event.player_id == 5503
        assert event.team == "Argentina"
        assert event.location == [60.0, 40.0]
        assert event.period == 1
        assert event.minute == 23
        assert event.second == 14

    def test_event_location_optional(self):
        """location puede ser None (ej. eventos de tipo Starting XI)."""
        event = RawStatsBombEvent(
            id="abc-123",
            type="Starting XI",
            player="Emiliano Martínez",
            team="Argentina",
            location=None,
            period=1,
            minute=0,
            second=0,
        )
        assert event.location is None

    def test_event_player_optional(self):
        """player y player_id pueden ser None (ej. eventos de tipo Half Start)."""
        event = RawStatsBombEvent(
            id="def-456",
            type="Half Start",
            player=None,
            player_id=None,
            team="Argentina",
            location=None,
            period=2,
            minute=45,
            second=0,
        )
        assert event.player is None
        assert event.player_id is None

    def test_event_period_extra_time(self):
        """period 3 y 4 representan la prórroga."""
        event = RawStatsBombEvent(
            id="et-001",
            type="Pass",
            player="Pedri",
            player_id=6855,
            team="Spain",
            location=[50.0, 40.0],
            period=3,
            minute=91,
            second=10,
        )
        assert event.period == 3

    def test_event_period_penalties(self):
        """period 5 representa la tanda de penaltis."""
        event = RawStatsBombEvent(
            id="pk-001",
            type="Shot",
            player="Alvaro Morata",
            player_id=6624,
            team="Spain",
            location=[120.0, 40.0],
            period=5,
            minute=120,
            second=0,
        )
        assert event.period == 5

    def test_event_rejects_period_zero(self):
        """period 0 no es válido."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="p0-001",
                type="Pass",
                player="Pedri",
                team="Spain",
                location=[50.0, 30.0],
                period=0,
                minute=0,
                second=0,
            )

    def test_event_rejects_period_six(self):
        """period 6 no es válido (máximo es 5)."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="p6-001",
                type="Pass",
                player="Pedri",
                team="Spain",
                location=[50.0, 30.0],
                period=6,
                minute=0,
                second=0,
            )

    def test_event_rejects_missing_id(self):
        """id es obligatorio, no puede faltar."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                type="Shot",
                player="Kylian Mbappé",
                team="France",
                location=[100.0, 50.0],
                period=1,
                minute=45,
                second=0,
            )

    def test_event_rejects_missing_type(self):
        """type es obligatorio."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="xyz-789",
                player="Kylian Mbappé",
                team="France",
                location=[100.0, 50.0],
                period=1,
                minute=45,
                second=0,
            )

    def test_event_rejects_negative_minute(self):
        """minute no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="neg-01",
                type="Pass",
                player="Pedri",
                team="Spain",
                location=[50.0, 30.0],
                period=1,
                minute=-1,
                second=0,
            )

    def test_event_rejects_negative_second(self):
        """second no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="neg-02",
                type="Pass",
                player="Pedri",
                team="Spain",
                location=[50.0, 30.0],
                period=1,
                minute=0,
                second=-5,
            )

    def test_event_rejects_second_over_59(self):
        """second no puede superar 59."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="sec-60",
                type="Pass",
                player="Pedri",
                team="Spain",
                location=[50.0, 30.0],
                period=1,
                minute=10,
                second=60,
            )

    def test_event_rejects_extra_fields(self):
        """Campos extra no declarados son rechazados (extra='forbid')."""
        with pytest.raises(ValidationError):
            RawStatsBombEvent(
                id="extra-01",
                type="Pass",
                player="Pedri",
                team="Spain",
                period=1,
                minute=10,
                second=0,
                unknown_field="value",
            )

    def test_event_is_immutable(self):
        """El modelo es inmutable (frozen=True)."""
        event = RawStatsBombEvent(
            id="frozen-01",
            type="Pass",
            player="Pedri",
            team="Spain",
            period=1,
            minute=10,
            second=0,
        )
        with pytest.raises(ValidationError):
            event.minute = 99  # type: ignore[misc]

    def test_event_json_roundtrip(self):
        """Serialización JSON ida y vuelta mantiene los datos intactos."""
        event = RawStatsBombEvent(
            id="rt-001",
            type="Shot",
            player="Lamine Yamal",
            player_id=41218,
            team="Spain",
            location=[105.0, 34.0],
            period=2,
            minute=71,
            second=33,
        )
        json_str = event.model_dump_json()
        restored = RawStatsBombEvent.model_validate_json(json_str)
        assert restored == event

    def test_event_json_schema_generation(self):
        """El modelo genera un JSON Schema válido (Pydantic v2)."""
        schema = RawStatsBombEvent.model_json_schema()
        assert schema["type"] == "object"
        assert "id" in schema["properties"]
        assert "type" in schema["properties"]
        assert "period" in schema["properties"]
        assert "player_id" in schema["properties"]


# ─────────────────────────────────────────────────────────────
# RawStatsBombMatch
# ─────────────────────────────────────────────────────────────


class TestRawStatsBombMatch:
    """Tests para partidos crudos provenientes de StatsBomb."""

    def test_valid_match(self):
        """Un partido completo se crea correctamente."""
        match = RawStatsBombMatch(
            match_id=3869685,
            home_team="Argentina",
            away_team="France",
            home_score=3,
            away_score=3,
            competition="FIFA World Cup",
            season="2022",
        )
        assert match.match_id == 3869685
        assert match.home_team == "Argentina"
        assert match.away_team == "France"
        assert match.home_score == 3
        assert match.away_score == 3
        assert match.competition == "FIFA World Cup"
        assert match.season == "2022"

    def test_match_rejects_negative_score(self):
        """Scores no pueden ser negativos."""
        with pytest.raises(ValidationError):
            RawStatsBombMatch(
                match_id=1,
                home_team="A",
                away_team="B",
                home_score=-1,
                away_score=0,
                competition="Test",
                season="2024",
            )

    def test_match_rejects_missing_match_id(self):
        """match_id es obligatorio."""
        with pytest.raises(ValidationError):
            RawStatsBombMatch(
                home_team="A",
                away_team="B",
                home_score=0,
                away_score=0,
                competition="Test",
                season="2024",
            )

    def test_match_rejects_extra_fields(self):
        """Campos extra no declarados son rechazados."""
        with pytest.raises(ValidationError):
            RawStatsBombMatch(
                match_id=1,
                home_team="A",
                away_team="B",
                home_score=0,
                away_score=0,
                competition="Test",
                season="2024",
                extra_field="value",
            )

    def test_match_json_roundtrip(self):
        """Serialización JSON ida y vuelta."""
        match = RawStatsBombMatch(
            match_id=12345,
            home_team="Barcelona",
            away_team="Real Madrid",
            home_score=2,
            away_score=1,
            competition="La Liga",
            season="2023/2024",
        )
        json_str = match.model_dump_json()
        restored = RawStatsBombMatch.model_validate_json(json_str)
        assert restored == match

    def test_match_json_schema_generation(self):
        """El modelo genera un JSON Schema válido."""
        schema = RawStatsBombMatch.model_json_schema()
        assert schema["type"] == "object"
        assert "match_id" in schema["properties"]


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
# RawFBrefPlayerSeason
# ─────────────────────────────────────────────────────────────


class TestRawFBrefPlayerSeason:
    """Tests para estadísticas de jugador por temporada crudas de FBref."""

    def test_valid_player_season(self):
        """Un registro completo de jugador-temporada se crea correctamente."""
        ps = RawFBrefPlayerSeason(
            player="Jude Bellingham",
            competition="La Liga",
            season="2023-2024",
            nation="ENG",
            pos="MF",
            squad="Real Madrid",
            born=2003,
            matches_played=28,
            minutes=2340,
            goals=14,
            assists=7,
            cards_yellow=4,
            cards_red=0,
        )
        assert ps.player == "Jude Bellingham"
        assert ps.competition == "La Liga"
        assert ps.season == "2023-2024"
        assert ps.nation == "ENG"
        assert ps.pos == "MF"
        assert ps.squad == "Real Madrid"
        assert ps.born == 2003
        assert ps.matches_played == 28
        assert ps.minutes == 2340
        assert ps.goals == 14
        assert ps.assists == 7
        assert ps.cards_yellow == 4
        assert ps.cards_red == 0

    def test_player_season_rejects_missing_competition(self):
        """competition es obligatorio."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test Player",
                season="2020-2021",
                pos="FW",
                squad="Club",
                matches_played=10,
                minutes=900,
                goals=5,
                assists=2,
                cards_yellow=1,
                cards_red=0,
            )

    def test_player_season_rejects_missing_season(self):
        """season es obligatorio."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test Player",
                competition="La Liga",
                pos="FW",
                squad="Club",
                matches_played=10,
                minutes=900,
                goals=5,
                assists=2,
                cards_yellow=1,
                cards_red=0,
            )

    def test_player_season_optional_born(self):
        """born puede ser None (dato faltante en algunas filas de FBref)."""
        ps = RawFBrefPlayerSeason(
            player="Unknown Player",
            competition="La Liga",
            season="2020-2021",
            nation="ESP",
            pos="FW",
            squad="Atlético Madrid",
            born=None,
            matches_played=5,
            minutes=200,
            goals=1,
            assists=0,
            cards_yellow=0,
            cards_red=0,
        )
        assert ps.born is None

    def test_player_season_optional_nation(self):
        """nation puede ser None."""
        ps = RawFBrefPlayerSeason(
            player="Test Player",
            competition="La Liga",
            season="2020-2021",
            nation=None,
            pos="DF",
            squad="Test FC",
            born=1990,
            matches_played=10,
            minutes=900,
            goals=0,
            assists=2,
            cards_yellow=1,
            cards_red=0,
        )
        assert ps.nation is None

    def test_player_season_rejects_negative_goals(self):
        """goals no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test",
                competition="La Liga",
                season="2020-2021",
                nation="ESP",
                pos="FW",
                squad="Club",
                born=1995,
                matches_played=10,
                minutes=900,
                goals=-1,
                assists=0,
                cards_yellow=0,
                cards_red=0,
            )

    def test_player_season_rejects_negative_minutes(self):
        """minutes no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test",
                competition="La Liga",
                season="2020-2021",
                nation="ESP",
                pos="FW",
                squad="Club",
                born=1995,
                matches_played=10,
                minutes=-100,
                goals=0,
                assists=0,
                cards_yellow=0,
                cards_red=0,
            )

    def test_player_season_rejects_negative_matches_played(self):
        """matches_played no puede ser negativo."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test",
                competition="La Liga",
                season="2020-2021",
                nation="ESP",
                pos="FW",
                squad="Club",
                born=1995,
                matches_played=-1,
                minutes=0,
                goals=0,
                assists=0,
                cards_yellow=0,
                cards_red=0,
            )

    def test_player_season_rejects_extra_fields(self):
        """Campos extra no declarados son rechazados."""
        with pytest.raises(ValidationError):
            RawFBrefPlayerSeason(
                player="Test",
                competition="La Liga",
                season="2020-2021",
                pos="FW",
                squad="Club",
                matches_played=10,
                minutes=900,
                goals=0,
                assists=0,
                cards_yellow=0,
                cards_red=0,
                unknown_field="value",
            )

    def test_player_season_json_roundtrip(self):
        """Serialización JSON ida y vuelta."""
        ps = RawFBrefPlayerSeason(
            player="Vinícius Júnior",
            competition="La Liga",
            season="2020-2021",
            nation="BRA",
            pos="FW",
            squad="Real Madrid",
            born=2000,
            matches_played=32,
            minutes=2700,
            goals=15,
            assists=5,
            cards_yellow=6,
            cards_red=1,
        )
        json_str = ps.model_dump_json()
        restored = RawFBrefPlayerSeason.model_validate_json(json_str)
        assert restored == ps

    def test_player_season_json_schema_generation(self):
        """El modelo genera un JSON Schema válido."""
        schema = RawFBrefPlayerSeason.model_json_schema()
        assert schema["type"] == "object"
        assert "player" in schema["properties"]
        assert "goals" in schema["properties"]
        assert "competition" in schema["properties"]
        assert "season" in schema["properties"]

    def test_player_season_cards_separate_fields(self):
        """Las tarjetas se desglosan en cards_yellow y cards_red."""
        ps = RawFBrefPlayerSeason(
            player="Sergio Ramos",
            competition="La Liga",
            season="2020-2021",
            nation="ESP",
            pos="DF",
            squad="Real Madrid",
            born=1986,
            matches_played=15,
            minutes=1350,
            goals=2,
            assists=0,
            cards_yellow=8,
            cards_red=2,
        )
        assert ps.cards_yellow == 8
        assert ps.cards_red == 2
