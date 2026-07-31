"""Tests for the API-Football ingestion DAG.

Covers the configured-scope gating: which tasks run is driven by the
``endpoints`` list in ingestion.yaml, and disabling one must not cascade into
unrelated tasks through the rate-limit sequencing edges.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from airflow.exceptions import AirflowSkipException

from pipeline.config import get_config

_DAG_PATH = Path(__file__).parents[1] / "dags" / "dag_ingest_api_football.py"


@pytest.fixture(scope="module")
def dag_module() -> ModuleType:
    """Import the DAG module once — importing Airflow is expensive."""
    spec = importlib.util.spec_from_file_location("dag_ingest_api_football", _DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_config(tmp_path: Path, endpoints: list[str]) -> Path:
    """Write a minimal ingestion.yaml with the given endpoints enabled."""
    path = tmp_path / "ingestion.yaml"
    rendered = "\n".join(f"      - {name}" for name in endpoints)
    path.write_text(
        "sources:\n"
        "  api_football:\n"
        "    league_id: 140\n"
        "    season: 2024\n"
        "    endpoints:\n"
        f"{rendered}\n"
        "    cache_dir: data/cache/api_football\n"
        "    cache_ttl_hours: 168\n"
        "    rate_limit:\n"
        "      max_calls_per_day: 100\n"
        "      delay_between_calls: 1.0\n"
        "  understat:\n"
        '    league: "ESP-La Liga"\n'
        '    season: "2024/2025"\n',
        encoding="utf-8",
    )
    return path


class TestRequireEndpoint:
    """ingestion.yaml is the single source of truth for what gets ingested."""

    def test_skips_when_endpoint_not_configured(
        self, dag_module: ModuleType, tmp_path: Path, reset_config_singleton: None
    ) -> None:
        get_config(_write_config(tmp_path, ["injuries"]))

        with pytest.raises(AirflowSkipException) as excinfo:
            dag_module._require_endpoint("transfers")

        assert "transfers" in str(excinfo.value)

    def test_passes_when_endpoint_configured(
        self, dag_module: ModuleType, tmp_path: Path, reset_config_singleton: None
    ) -> None:
        get_config(_write_config(tmp_path, ["injuries", "transfers"]))

        dag_module._require_endpoint("transfers")  # must not raise

    @pytest.mark.parametrize(
        "endpoint",
        ["players_stats", "injuries", "transfers", "standings"],
    )
    def test_every_gated_endpoint_can_be_disabled(
        self, dag_module: ModuleType, tmp_path: Path, reset_config_singleton: None, endpoint: str
    ) -> None:
        """Each of the four gated tasks honours its own key."""
        get_config(_write_config(tmp_path, ["some_other_endpoint"]))

        with pytest.raises(AirflowSkipException):
            dag_module._require_endpoint(endpoint)


class TestTaskGraph:
    """The graph shape is fixed; disabled endpoints surface as skipped tasks."""

    def test_all_expected_tasks_present(self, dag_module: ModuleType) -> None:
        dag = dag_module.ingest_api_football()

        assert {t.task_id for t in dag.tasks} == {
            "fetch_teams_task",
            "ingest_players_task",
            "ingest_injuries_task",
            "ingest_transfers_task",
            "ingest_standings_task",
        }

    @pytest.mark.parametrize("task_id", ["ingest_injuries_task", "ingest_transfers_task"])
    def test_sequencing_edges_do_not_cascade_skips(self, dag_module: ModuleType, task_id: str) -> None:
        """A task skipped by config must not skip the ones merely sequenced after it.

        ingest_players >> ingest_injuries >> ingest_transfers exists to spread
        API calls, not because of a data dependency. Under the default
        all_success rule, disabling players_stats would silently skip injuries
        and transfers too.
        """
        dag = dag_module.ingest_api_football()
        task = dag.get_task(task_id)

        assert task.trigger_rule.value == "none_failed", f"{task_id} would cascade a config-driven skip from upstream"

    def test_fetch_teams_is_not_gated(self, dag_module: ModuleType) -> None:
        """teams is not an endpoint key: one call, and teams.parquet feeds
        team entity resolution regardless of which player endpoints are on."""
        source = _DAG_PATH.read_text(encoding="utf-8")
        fetch_teams_body = source.split("def fetch_teams_task")[1].split("@task")[0]

        assert "_require_endpoint" not in fetch_teams_body
