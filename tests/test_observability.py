"""Tests for the observability logging helpers."""

from __future__ import annotations

import logging

from pipeline import observability


def test_get_logger_namespaces_under_pipeline() -> None:
    logger = observability.get_logger("entity_resolution")
    assert logger.name == "pipeline.entity_resolution"


def test_get_logger_root_returns_package_logger() -> None:
    logger = observability.get_logger()
    assert logger.name == "pipeline"


def test_get_logger_attaches_null_handler_when_unconfigured() -> None:
    root = logging.getLogger()
    saved = list(root.handlers)
    for h in saved:
        root.removeHandler(h)
    logger = logging.getLogger("pipeline.test_null")
    try:
        for h in list(logger.handlers):
            logger.removeHandler(h)
        observability.get_logger("test_null")
        assert any(isinstance(h, logging.NullHandler) for h in logger.handlers)
    finally:
        for h in list(logger.handlers):
            logger.removeHandler(h)
        for h in saved:
            root.addHandler(h)


def test_configure_logging_is_idempotent() -> None:
    # Simulate Airflow/configured root: do not override existing handlers.
    root = logging.getLogger()
    fake = logging.Handler()
    root.addHandler(fake)
    try:
        before = list(root.handlers)
        observability.configure_logging()
        assert list(root.handlers) == before
    finally:
        root.removeHandler(fake)


def test_configure_logging_sets_handler_when_unconfigured() -> None:
    # Isolate a fresh root without handlers for this assertion.
    root = logging.getLogger()
    saved = list(root.handlers)
    for h in saved:
        root.removeHandler(h)
    try:
        observability.configure_logging()
        assert root.handlers
    finally:
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in saved:
            root.addHandler(h)
