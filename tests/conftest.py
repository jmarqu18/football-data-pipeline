"""Configuración compartida de pytest para el pipeline.

Este módulo centraliza fixtures reutilizables entre los distintos
módulos de tests.  A medida que el pipeline crezca (loaders, entity
resolution, feature engineering) se irán añadiendo fixtures que
gestionen recursos compartidos como conexiones de BD, respuestas de
API mockeadas, o DataFrames de ejemplo.
"""

from __future__ import annotations

import pytest

import pipeline.config as _config_module


@pytest.fixture
def reset_config_singleton() -> None:
    """Reset the get_config() singleton before and after each test.

    Use this fixture explicitly in tests that exercise ``get_config()`` to
    prevent state leaking between test cases.  The singleton is a module-level
    variable (``pipeline.config._config``); resetting it forces a fresh load
    on the next ``get_config()`` call.

    Usage::

        def test_something(self, tmp_path, reset_config_singleton):
            cfg = get_config(tmp_path / "ingestion.yaml")
            ...
    """
    _config_module._config = None
    yield  # type: ignore[misc]
    _config_module._config = None
