"""Tests for scripts/generate_airflow_secrets.py (upsert + lengths)."""

from __future__ import annotations

from pathlib import Path

from scripts.generate_airflow_secrets import (
    generate_secrets,
    upsert_secret,
    write_secrets,
)


def test_upsert_secret_replaces_existing_line() -> None:
    lines = ["API_FOOTBALL_KEY=abc\n", "FERNET_KEY=old\n"]
    out = upsert_secret(lines, "FERNET_KEY", "new")
    assert out == ["API_FOOTBALL_KEY=abc\n", "FERNET_KEY=new\n"]


def test_upsert_secret_appends_when_missing() -> None:
    lines = ["API_FOOTBALL_KEY=abc\n"]
    out = upsert_secret(lines, "FERNET_KEY", "new")
    assert out == ["API_FOOTBALL_KEY=abc\n", "FERNET_KEY=new\n"]


def test_upsert_secret_preserves_comments_and_other_keys() -> None:
    lines = ["# comment\n", "API_FOOTBALL_KEY=abc\n", "# FERNET_KEY=placeholder\n"]
    out = upsert_secret(lines, "FERNET_KEY", "new")
    # commented placeholder must remain; active line appended
    assert "# FERNET_KEY=placeholder" in "".join(out)
    assert "FERNET_KEY=new\n" in out
    assert "API_FOOTBALL_KEY=abc\n" in out


def test_generate_secrets_lengths() -> None:
    secs = generate_secrets()
    assert len(secs["AIRFLOW__API_AUTH__JWT_SECRET"]) >= 64
    assert len(secs["AIRFLOW__API__SECRET_KEY"]) == 64
    assert len(secs["FERNET_KEY"]) == 44


def test_write_secrets_idempotent_on_tmp_file(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("API_FOOTBALL_KEY=keepme\n", encoding="utf-8")
    secs = generate_secrets()
    write_secrets(env, secs)
    write_secrets(env, secs)  # second run must not duplicate
    content = env.read_text(encoding="utf-8")
    for key in secs:
        assert content.count(f"{key}=") == 1
    assert "API_FOOTBALL_KEY=keepme" in content
