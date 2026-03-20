"""Loaders: data ingestion modules for each pipeline source."""

from pipeline.loaders.api_football_loader import APIFootballLoader
from pipeline.loaders.understat_loader import UnderstatLoader

__all__ = ["APIFootballLoader", "UnderstatLoader"]
