#!/bin/bash
# postgres-init.sh — Crea la base de datos 'football' y aplica el DDL.
# Ejecutado automáticamente por postgres:18-alpine en el primer arranque
# (cuando postgres_data está vacío). Los ficheros *.sh en initdb.d/ no
# requieren bit de ejecución; el entrypoint los invoca con bash explícitamente.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE football;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname football \
    -f /opt/init.sql
