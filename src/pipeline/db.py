"""Shared database utilities for the pipeline."""

from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_DEFAULT_DATABASE_URL = "postgresql://localhost:5432/football"


def get_engine(database_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine from a database URL.

    Falls back to the ``DATABASE_URL`` environment variable, then to a
    local default.

    Args:
        database_url: Explicit connection string. If None, reads
            ``DATABASE_URL`` from the environment.

    Returns:
        A SQLAlchemy ``Engine`` instance.
    """
    url = database_url or os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL)
    return create_engine(url)
