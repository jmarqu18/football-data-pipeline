"""Loader for Understat via soccerdata scraping.

Implements the RAW layer ingestion for Understat shot events and
player season statistics.  The soccerdata ``Understat`` client handles
the actual web scraping; this loader handles extraction, validation,
and Parquet output.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from pipeline.config import UnderstatConfig
from pipeline.models.raw import RawUnderstatPlayerSeason, RawUnderstatShot

logger = logging.getLogger(__name__)


class UnderstatLoader:
    """Loader for Understat with injectable soccerdata client.

    Args:
        config: Understat configuration from ``ingestion.yaml``.
        client: Optional injectable soccerdata ``Understat`` instance
            for testing.  When *None*, a real client is created from
            the provided config.
    """

    def __init__(
        self,
        config: UnderstatConfig,
        client: object | None = None,
    ) -> None:
        self._config = config
        if client is not None:
            self._client = client
        else:
            import soccerdata as sd

            self._client = sd.Understat(
                leagues=config.league,
                seasons=config.season,
            )

    # ─────────────────────────────────────────────────────────
    # Extraction
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _extract_shot(row: dict) -> dict:
        """Map a soccerdata shot-event row to RawUnderstatShot fields."""
        return {
            "id": row["shot_id"],
            "minute": row["minute"],
            "result": row["result"],
            "x": row["location_x"],
            "y": row["location_y"],
            "xg": row["xg"],
            "player": row["player"],
            "player_id": row["player_id"],
            "situation": row["situation"],
        }

    @staticmethod
    def _extract_player_season(row: dict) -> dict:
        """Map a soccerdata player-season row to RawUnderstatPlayerSeason fields."""
        return {
            "player_id": row["player_id"],
            "player_name": row["player"],
            "team": row["team"],
            "season": row["season"],
            "games": row["matches"],
            "minutes": row["minutes"],
            "goals": row["goals"],
            "assists": row["assists"],
            "xg": row["xg"],
            "xa": row["xa"],
            "npxg": row["np_xg"],
            "xg_chain": row["xg_chain"],
            "xg_buildup": row["xg_buildup"],
            "shots": row["shots"],
            "key_passes": row["key_passes"],
            "yellow_cards": row["yellow_cards"],
            "red_cards": row["red_cards"],
        }

    # ─────────────────────────────────────────────────────────
    # Public ingestion methods
    # ─────────────────────────────────────────────────────────

    def ingest_shots(self) -> list[RawUnderstatShot]:
        """Scrape shot-level data and validate with Pydantic.

        Returns:
            List of validated shot models.
        """
        try:
            df = self._client.read_shot_events()
        except Exception as exc:
            logger.error("Failed to fetch shot events from Understat: %s", exc)
            return []

        if df.empty:
            logger.info("Shots ingested: 0 valid, 0 rejected (empty DataFrame)")
            return []

        df = df.reset_index()
        records = df.to_dict("records")

        shots: list[RawUnderstatShot] = []
        rejected = 0

        for row in records:
            try:
                extracted = self._extract_shot(row)
                shots.append(RawUnderstatShot.model_validate(extracted))
            except (ValidationError, KeyError) as exc:
                shot_id = row.get("shot_id", "unknown")
                logger.warning("Rejected shot %s: %s", shot_id, exc)
                rejected += 1

        logger.info(
            "Shots ingested: %d valid, %d rejected",
            len(shots),
            rejected,
        )
        return shots

    def ingest_player_season_stats(self) -> list[RawUnderstatPlayerSeason]:
        """Scrape player season aggregates and validate with Pydantic.

        Returns:
            List of validated player season models.
        """
        try:
            df = self._client.read_player_season_stats()
        except Exception as exc:
            logger.error(
                "Failed to fetch player season stats from Understat: %s", exc
            )
            return []

        if df.empty:
            logger.info(
                "Player season stats ingested: 0 valid, 0 rejected "
                "(empty DataFrame)"
            )
            return []

        df = df.reset_index()
        records = df.to_dict("records")

        stats: list[RawUnderstatPlayerSeason] = []
        rejected = 0

        for row in records:
            try:
                extracted = self._extract_player_season(row)
                stats.append(
                    RawUnderstatPlayerSeason.model_validate(extracted)
                )
            except (ValidationError, KeyError) as exc:
                player_id = row.get("player_id", "unknown")
                logger.warning(
                    "Rejected player season %s: %s", player_id, exc
                )
                rejected += 1

        logger.info(
            "Player season stats ingested: %d valid, %d rejected",
            len(stats),
            rejected,
        )
        return stats

    # ─────────────────────────────────────────────────────────
    # Parquet output
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def save_parquet(models: list[BaseModel], path: Path) -> None:
        """Serialise a list of Pydantic models to a Parquet file.

        Args:
            models: Validated Pydantic model instances.
            path: Destination ``.parquet`` file path.
        """
        if not models:
            logger.warning("No records to save to %s", path)
            return
        rows = [m.model_dump() for m in models]
        table = pa.Table.from_pylist(rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        logger.info("Saved %d records to %s", len(rows), path)

    # ─────────────────────────────────────────────────────────
    # Orchestration
    # ─────────────────────────────────────────────────────────

    def ingest_all(
        self,
        output_dir: Path | None = None,
    ) -> dict[str, int]:
        """Run all Understat ingestion and save Parquet files.

        Args:
            output_dir: Base directory for Parquet output.
                Defaults to ``data/raw/understat``.

        Returns:
            Dict mapping record type names to validated record counts.
        """
        out = output_dir or Path("data/raw/understat")
        counts: dict[str, int] = {}

        shots = self.ingest_shots()
        self.save_parquet(shots, out / "shots.parquet")
        counts["shots"] = len(shots)

        stats = self.ingest_player_season_stats()
        self.save_parquet(stats, out / "player_season.parquet")
        counts["player_season"] = len(stats)

        logger.info("Understat ingest complete: %s", counts)
        return counts
