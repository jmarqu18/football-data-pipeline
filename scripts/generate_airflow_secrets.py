"""Genera y escribe los secretos de Airflow en `.env` (upsert idempotente).

Se invoca desde el Makefile (`make init` / `make rotate-secrets`) vía
`$(PYTHON) scripts/generate_airflow_secrets.py`. Usar un script en lugar de
`python -c "..."` evita los problemas de quoting de git-bash en Windows, donde
el `-c` con `;` y paréntesis se malinterpreta dentro de `$(...)`.

La función `write_secrets` hace upsert: reemplaza la línea `KEY=...` si existe,
o la añade al final si no. Nunca toca `API_FOOTBALL_KEY` ni otras líneas.
"""

from __future__ import annotations

import os
import secrets
import sys
from pathlib import Path

from cryptography.fernet import Fernet

DEFAULT_ENV_PATH = ".env"


def generate_secrets() -> dict[str, str]:
    """Devuelve los tres secretos de Airflow con longitudes seguras."""
    return {
        "FERNET_KEY": Fernet.generate_key().decode(),
        "AIRFLOW__API__SECRET_KEY": secrets.token_hex(32),
        "AIRFLOW__API_AUTH__JWT_SECRET": secrets.token_urlsafe(64),
    }


def upsert_secret(lines: list[str], key: str, value: str) -> list[str]:
    """Reemplaza `key=` si existe, si no lo añade al final.

    `lines` son las líneas originales (con su terminador si lo tenían).
    No modifica líneas comentadas (`# key=`) ni otras claves.
    """
    out: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}\n")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        if out and not out[-1].endswith("\n"):
            out[-1] = out[-1] + "\n"
        out.append(f"{key}={value}\n")
    return out


def write_secrets(env_path: Path, secrets_map: dict[str, str]) -> None:
    """Escribe los secretos en `env_path` preservando el resto del contenido."""
    lines = (
        env_path.read_text(encoding="utf-8").splitlines(keepends=True)
        if env_path.exists()
        else []
    )
    for key, value in secrets_map.items():
        lines = upsert_secret(lines, key, value)
    env_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    env_path = Path(os.environ.get("ENV_PATH", DEFAULT_ENV_PATH))
    secrets_map = generate_secrets()
    write_secrets(env_path, secrets_map)
    jwt_len = len(secrets_map["AIRFLOW__API_AUTH__JWT_SECRET"])
    print(
        f"Secrets de Airflow actualizados en {env_path}: "
        f"FERNET_KEY, AIRFLOW__API__SECRET_KEY, AIRFLOW__API_AUTH__JWT_SECRET "
        f"(JWT={jwt_len} bytes, >= 64)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
