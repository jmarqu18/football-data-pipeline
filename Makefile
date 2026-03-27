# =============================================================================
# Makefile — Football Data Pipeline
# =============================================================================
#
# Gestión del ciclo de vida completo: entorno, contenedores, DAGs, caché y backups.
# Compatible con Podman (preferido) y Docker.
#
# Uso rápido:
#   make              → Muestra esta ayuda
#   make init         → Configura el entorno por primera vez
#   make up           → Levanta el stack completo
#   make pipeline-full → Lanza la guía del pipeline de extremo a extremo
#
# =============================================================================


# ── §1 Configuración ──────────────────────────────────────────────────────────

# Auto-detección del motor de contenedores: Podman tiene prioridad sobre Docker.
CONTAINER_ENGINE := $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)
COMPOSE          := $(shell command -v podman-compose >/dev/null 2>&1 && echo podman-compose || echo "docker compose")

# Servicio donde se ejecutan los comandos Airflow CLI.
AIRFLOW_SVC := airflow-scheduler

# Directorio de caché de API-Football.
CACHE_DIR := data/cache/api_football

# Directorio de backup con timestamp evaluado una sola vez al parsear el Makefile.
BACKUP_DIR = backups/$(shell date +%Y%m%d_%H%M%S)

# Variables con override posible vía línea de comandos.
DAG      ?= ingest_api_football
SERVICE  ?=
ENDPOINT ?=

# Colores ANSI.
BOLD   := \033[1m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
CYAN   := \033[0;36m
RED    := \033[0;31m
RESET  := \033[0m

.DEFAULT_GOAL := help


# ── §2 Ayuda ──────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Muestra esta ayuda con todos los targets disponibles
	@printf "\n$(BOLD)Football Data Pipeline — Comandos disponibles$(RESET)\n"
	@printf "Motor detectado: $(CYAN)$(CONTAINER_ENGINE)$(RESET) / $(CYAN)$(COMPOSE)$(RESET)\n\n"
	@awk 'BEGIN {FS = ":.*?## "; section=""} \
	    /^# ── §[0-9]/ { \
	        split($$0, a, "──"); \
	        gsub(/^[ \t]+|[ \t]+$$/, "", a[2]); \
	        printf "\n$(BOLD)%s$(RESET)\n", a[2]; next \
	    } \
	    /^[a-zA-Z_-]+:.*?## / { \
	        printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2 \
	    }' $(MAKEFILE_LIST)
	@printf "\n$(YELLOW)Variables de override:$(RESET)\n"
	@printf "  $(BOLD)DAG$(RESET)=<nombre>       Nombre del DAG        (default: $(DAG))\n"
	@printf "  $(BOLD)SERVICE$(RESET)=<nombre>   Servicio de logs      (default: todos)\n"
	@printf "  $(BOLD)ENDPOINT$(RESET)=<nombre>  Endpoint de caché     (ej: players, injuries, transfers)\n"
	@printf "\n"


# ── §3 Inicialización ─────────────────────────────────────────────────────────

.PHONY: init
init: ## Genera .env con claves de seguridad a partir de .env.example
	@if [ -f .env ]; then \
		printf "$(YELLOW)⚠  .env ya existe. ¿Sobreescribir? [s/N] $(RESET)"; \
		read ans; [ "$$ans" = "s" ] || { echo "Cancelado."; exit 0; }; \
	fi
	@cp .env.example .env
	@printf "$(CYAN)→  Generando claves de seguridad...$(RESET)\n"
	@FERNET=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") && \
	 JWT=$$(python3 -c "import secrets; print(secrets.token_urlsafe(64))") && \
	 API_SECRET=$$(python3 -c "import secrets; print(secrets.token_hex(32))") && \
	 sed -i "s|^# FERNET_KEY=.*|FERNET_KEY=$$FERNET|" .env && \
	 sed -i "s|^# AIRFLOW__API__SECRET_KEY=.*|AIRFLOW__API__SECRET_KEY=$$API_SECRET|" .env && \
	 sed -i "s|^# AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=$$JWT|" .env && \
	 printf "  $(GREEN)✓  Fernet key, API secret y JWT secret generados$(RESET)\n"
	@printf "$(CYAN)→  Introduce tu API_FOOTBALL_KEY: $(RESET)"; \
	 read key; sed -i "s|^API_FOOTBALL_KEY=.*|API_FOOTBALL_KEY=$$key|" .env
	@printf "$(GREEN)✓  .env configurado correctamente$(RESET)\n"
	@printf "   Siguiente paso: $(BOLD)make up$(RESET)\n"

.PHONY: env-check
env-check: ## Verifica que .env existe y API_FOOTBALL_KEY está configurada
	@[ -f .env ] || { \
		printf "$(RED)✗  Falta .env — ejecuta: make init$(RESET)\n"; exit 1; }
	@grep -qE "^API_FOOTBALL_KEY=.+" .env || { \
		printf "$(YELLOW)⚠  API_FOOTBALL_KEY no configurada en .env$(RESET)\n"; exit 1; }
	@printf "$(GREEN)✓  Entorno OK$(RESET)\n"


# ── §4 Desarrollo local ───────────────────────────────────────────────────────

.PHONY: install
install: ## Instala todas las dependencias con uv (incluye extras de dev)
	uv sync --all-extras

.PHONY: test
test: ## Ejecuta la suite de tests con pytest
	uv run pytest tests/ -v

.PHONY: test-fast
test-fast: ## Ejecuta tests sin output verbose
	uv run pytest tests/ -q

.PHONY: lint
lint: ## Comprueba estilo con ruff (sin modificar ficheros)
	uv run ruff check src/ tests/ dags/

.PHONY: lint-fix
lint-fix: ## Corrige automáticamente los errores de estilo con ruff
	uv run ruff check --fix src/ tests/ dags/

.PHONY: format
format: ## Formatea el código con ruff format
	uv run ruff format src/ tests/ dags/

.PHONY: format-check
format-check: ## Verifica el formateo sin modificar (útil para CI)
	uv run ruff format --check src/ tests/ dags/

.PHONY: typecheck
typecheck: ## Comprueba tipos estrictamente con mypy
	uv run mypy src/pipeline/

.PHONY: check
check: lint format-check typecheck ## Ejecuta lint + format-check + typecheck (todo a la vez)


# ── §5 Stack de contenedores ──────────────────────────────────────────────────

.PHONY: build
build: ## Construye la imagen del pipeline
	$(COMPOSE) build

.PHONY: rebuild
rebuild: ## Fuerza la reconstrucción completa sin caché de capas
	$(COMPOSE) build --no-cache

.PHONY: up
up: env-check ## Levanta el stack completo en segundo plano
	$(COMPOSE) up -d
	@printf "\n$(GREEN)✓  Stack levantado$(RESET)\n"
	@printf "   Airflow UI  →  $(BOLD)http://localhost:8080$(RESET)  (admin / admin)\n"
	@printf "   Datasette   →  $(BOLD)http://localhost:8001$(RESET)\n"
	@printf "   PostgreSQL  →  $(BOLD)localhost:5432$(RESET)  (airflow / airflow)\n"

.PHONY: down
down: ## Para y elimina los contenedores (conserva volúmenes y datos)
	$(COMPOSE) down

.PHONY: restart
restart: ## Reinicia todos los servicios del stack
	$(COMPOSE) restart

.PHONY: ps
ps: ## Muestra el estado actual de los servicios
	$(COMPOSE) ps

.PHONY: logs
logs: ## Sigue los logs en tiempo real (SERVICE=<nombre> para filtrar un servicio)
	$(COMPOSE) logs -f $(SERVICE)

.PHONY: shell
shell: ## Abre bash en el contenedor airflow-scheduler
	$(COMPOSE) exec $(AIRFLOW_SVC) bash


# ── §6 DAGs ───────────────────────────────────────────────────────────────────

.PHONY: dag-list
dag-list: ## Lista todos los DAGs registrados en Airflow
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags list

.PHONY: dag-run
dag-run: ## Dispara un DAG manualmente (DAG=<nombre>)
	@printf "$(CYAN)→  Disparando DAG: $(BOLD)$(DAG)$(RESET)\n"
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger $(DAG)
	@printf "$(GREEN)✓  DAG '$(DAG)' disparado$(RESET) — monitorea en http://localhost:8080\n"

.PHONY: dag-status
dag-status: ## Muestra las últimas 5 ejecuciones de un DAG (DAG=<nombre>)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags list-runs --dag-id $(DAG) --limit 5

.PHONY: dag-pause
dag-pause: ## Pausa la planificación de un DAG (DAG=<nombre>)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags pause $(DAG)

.PHONY: dag-unpause
dag-unpause: ## Activa la planificación de un DAG pausado (DAG=<nombre>)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags unpause $(DAG)


# ── §7 Pipeline ───────────────────────────────────────────────────────────────

.PHONY: pipeline-ingest
pipeline-ingest: ## Dispara las dos ingestas en paralelo (API-Football + Understat)
	@printf "$(CYAN)→  Ingesta API-Football...$(RESET)\n"
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger ingest_api_football
	@printf "$(CYAN)→  Ingesta Understat...$(RESET)\n"
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger ingest_understat
	@printf "$(GREEN)✓  Ambas ingestas disparadas$(RESET)\n"
	@printf "   Monitorea en $(BOLD)http://localhost:8080$(RESET)\n"
	@printf "   Cuando ambas finalicen, ejecuta: $(BOLD)make pipeline-clean$(RESET)\n"

.PHONY: pipeline-clean
pipeline-clean: ## Dispara la transformación RAW → CLEAN (entity resolution + PostgreSQL)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger transform_clean
	@printf "$(GREEN)✓  DAG transform_clean disparado$(RESET)\n"
	@printf "   Cuando finalice, ejecuta: $(BOLD)make pipeline-features$(RESET)\n"

.PHONY: pipeline-features
pipeline-features: ## Dispara la construcción de features (CLEAN → FEATURES Parquet)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger build_features
	@printf "$(GREEN)✓  DAG build_features disparado$(RESET)\n"
	@printf "   Cuando finalice, ejecuta: $(BOLD)make pipeline-enrich$(RESET)\n"

.PHONY: pipeline-enrich
pipeline-enrich: ## Dispara la exportación a SQLite/Datasette (FEATURES → ENRICHED)
	$(COMPOSE) exec $(AIRFLOW_SVC) airflow dags trigger export_enriched
	@printf "$(GREEN)✓  DAG export_enriched disparado$(RESET)\n"
	@printf "   Resultados en: $(BOLD)http://localhost:8001$(RESET)\n"

.PHONY: pipeline-full
pipeline-full: ## Dispara la ingesta inicial y muestra los pasos siguientes hasta enrich
	@printf "$(BOLD)══════════════════════════════════════════════$(RESET)\n"
	@printf "$(BOLD) Pipeline completo — Paso 1/4: Ingesta$(RESET)\n"
	@printf "$(BOLD)══════════════════════════════════════════════$(RESET)\n"
	@$(MAKE) -s pipeline-ingest
	@printf "\n$(YELLOW)Pasos siguientes (en orden, cuando cada DAG finalice):$(RESET)\n"
	@printf "  2.  $(BOLD)make pipeline-clean$(RESET)     → RAW → CLEAN\n"
	@printf "  3.  $(BOLD)make pipeline-features$(RESET)  → CLEAN → FEATURES\n"
	@printf "  4.  $(BOLD)make pipeline-enrich$(RESET)    → FEATURES → ENRICHED\n"
	@printf "\nMonitorea el progreso en $(BOLD)http://localhost:8080$(RESET)\n"


# ── §8 Caché de API-Football ──────────────────────────────────────────────────

.PHONY: cache-stats
cache-stats: ## Muestra tamaño y número de archivos por endpoint de caché
	@printf "\n$(BOLD)Caché API-Football: $(CACHE_DIR)$(RESET)\n\n"
	@if [ ! -d "$(CACHE_DIR)" ]; then \
		printf "$(YELLOW)⚠  Directorio no encontrado: $(CACHE_DIR)$(RESET)\n"; exit 0; \
	fi
	@printf "$(CYAN)%-20s %10s %10s$(RESET)\n" "Endpoint" "Archivos" "Tamaño"
	@printf "%-20s %10s %10s\n"  "────────────────────" "────────" "────────"
	@found=0; \
	 for dir in $(CACHE_DIR)/*/; do \
	     [ -d "$$dir" ] || continue; \
	     found=1; \
	     name=$$(basename "$$dir"); \
	     count=$$(find "$$dir" -name "*.json" 2>/dev/null | wc -l | tr -d ' '); \
	     size=$$(du -sh "$$dir" 2>/dev/null | cut -f1); \
	     printf "%-20s %10s %10s\n" "$$name" "$$count" "$$size"; \
	 done; \
	 [ "$$found" = "1" ] || printf "  (sin endpoints cacheados)\n"
	@printf "\n$(BOLD)Total: $$(du -sh $(CACHE_DIR) 2>/dev/null | cut -f1)$(RESET)\n\n"

.PHONY: cache-clear
cache-clear: ## Borra toda la caché de API-Football (pide confirmación)
	@printf "$(YELLOW)⚠  Se borrará todo el contenido de $(CACHE_DIR)/\n"
	@printf "   ¿Confirmar? [s/N] $(RESET)"; \
	 read ans; [ "$$ans" = "s" ] || { echo "Cancelado."; exit 0; }
	@rm -rf $(CACHE_DIR)/*/
	@printf "$(GREEN)✓  Caché eliminada$(RESET)\n"

.PHONY: cache-clear-endpoint
cache-clear-endpoint: ## Borra la caché de un endpoint concreto (ENDPOINT=<nombre>)
	@[ -n "$(ENDPOINT)" ] || { \
		printf "$(RED)✗  Especifica ENDPOINT=<nombre>  (ej: players, injuries, transfers, teams)$(RESET)\n"; \
		exit 1; }
	@[ -d "$(CACHE_DIR)/$(ENDPOINT)" ] || { \
		printf "$(YELLOW)⚠  No existe: $(CACHE_DIR)/$(ENDPOINT)$(RESET)\n"; exit 1; }
	@printf "$(YELLOW)⚠  Se borrará: $(CACHE_DIR)/$(ENDPOINT)/\n"
	@printf "   ¿Confirmar? [s/N] $(RESET)"; \
	 read ans; [ "$$ans" = "s" ] || { echo "Cancelado."; exit 0; }
	@rm -rf $(CACHE_DIR)/$(ENDPOINT)/
	@printf "$(GREEN)✓  Caché de '$(ENDPOINT)' eliminada$(RESET)\n"


# ── §9 Backups ────────────────────────────────────────────────────────────────

.PHONY: backup-db
backup-db: ## Dump de las bases de datos PostgreSQL en backups/YYYYMMDD_HHMMSS/
	@mkdir -p $(BACKUP_DIR)
	@printf "$(CYAN)→  Volcando base de datos 'football'...$(RESET)\n"
	$(COMPOSE) exec -T postgres \
		pg_dump -U airflow -d football --no-password \
		> $(BACKUP_DIR)/football.sql
	@printf "$(CYAN)→  Volcando base de datos 'airflow'...$(RESET)\n"
	$(COMPOSE) exec -T postgres \
		pg_dump -U airflow -d airflow --no-password \
		> $(BACKUP_DIR)/airflow.sql
	@printf "$(GREEN)✓  Dumps guardados en $(BACKUP_DIR)/$(RESET)\n"
	@ls -lh $(BACKUP_DIR)/*.sql

.PHONY: backup-data
backup-data: ## Comprime data/raw, data/features y data/enriched en backups/YYYYMMDD_HHMMSS/
	@mkdir -p $(BACKUP_DIR)
	@printf "$(CYAN)→  Comprimiendo datos locales...$(RESET)\n"
	@for layer in raw features enriched; do \
	     if [ -d "data/$$layer" ] && [ "$$(ls -A data/$$layer 2>/dev/null)" ]; then \
	         tar -czf $(BACKUP_DIR)/$$layer.tar.gz data/$$layer/ && \
	         printf "  $(GREEN)✓  data/$$layer$(RESET) → $(BACKUP_DIR)/$$layer.tar.gz\n"; \
	     else \
	         printf "  $(YELLOW)⚠  data/$$layer vacío o inexistente, omitido$(RESET)\n"; \
	     fi; \
	 done

.PHONY: backup
backup: backup-db backup-data ## Backup completo: PostgreSQL + datos locales
	@printf "\n$(GREEN)✓  Backup completo en: $(BOLD)$(BACKUP_DIR)/$(RESET)\n"
	@du -sh $(BACKUP_DIR)/


# ── §10 Base de datos ─────────────────────────────────────────────────────────

.PHONY: db-shell
db-shell: ## Abre una sesión psql en la base de datos football
	$(COMPOSE) exec postgres psql -U airflow -d football

.PHONY: db-reset
db-reset: ## ⚠ Destruye y recrea PostgreSQL desde cero (¡borra todos los datos!)
	@printf "$(RED)$(BOLD)⚠  PELIGRO: Se borrarán TODOS los datos de PostgreSQL.$(RESET)\n"
	@printf "   Haz un backup primero con: $(BOLD)make backup-db$(RESET)\n"
	@printf "$(YELLOW)   ¿Confirmar reset completo? [s/N] $(RESET)"; \
	 read ans; [ "$$ans" = "s" ] || { echo "Cancelado."; exit 0; }
	$(COMPOSE) down -v
	$(COMPOSE) up airflow-init
	$(COMPOSE) up -d
	@printf "$(GREEN)✓  PostgreSQL reiniciado con schema limpio$(RESET)\n"
