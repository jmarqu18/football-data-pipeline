"""Observabilidad: logging estructurado y métricas del pipeline.

El pipeline se ejecuta dentro de Airflow 3.x, que es dueño de la configuración
de logging en las tareas. Este módulo cubre los contextos standalone donde los
módulos de `src/pipeline` corren fuera de Airflow: `pytest`, scripts en
`data/reports/`, o `uv run` manual. En esos casos no hay handlers configurados
y el logging queda sin salida o emite advertencias "No handler found".
"""

from __future__ import annotations

import logging

_PACKAGE_ROOT = "pipeline"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a pipeline logger namespaced under ``pipeline``.

    Adjunta un ``NullHandler`` cuando la raíz no tiene handlers configurados,
    de modo que los módulos de librería no emiten warnings de "No handler found"
    fuera de Airflow (pytest, scripts sueltos). Es idempotente.
    """
    full_name = f"{_PACKAGE_ROOT}.{name}" if name else _PACKAGE_ROOT
    logger = logging.getLogger(full_name)
    if not logging.getLogger().handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def configure_logging(level: int = logging.INFO) -> None:
    """Configura logging básico para contextos fuera de Airflow (una sola vez).

    Idempotente: si la raíz ya tiene handlers (p.ej. Airflow o una configuración
    previa), no los sobrescribe. Formato legible con timestamp ISO.
    """
    root = logging.getLogger()
    if root.handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
