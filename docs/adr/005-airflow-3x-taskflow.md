# ADR-005: Airflow 3.x con TaskFlow API para Orquestación

**Status:** Accepted
**Date:** 2026-04-01
**Decision makers:** Juanje Márquez

## Context

El pipeline necesita un orquestador que ejecute 5 DAGs en secuencia (con 2 ingestas paralelas), maneje dependencias entre capas, y proporcione visibilidad del estado de cada ejecución. El pipeline se ejecuta en un entorno containerizado local (desarrollo/portfolio).

### Requisitos

- Ejecución secuencial de DAGs con disparo manual entre capas.
- Paralelismo controlado (2 ingestas simultáneas, sin competir por recursos).
- Visibilidad: logs, estado de ejecución, triggers manuales desde UI.
- Containerización: debe funcionar dentro del stack Podman existente.
- Free tier: sin coste de licencia ni infraestructura cloud.
- Python nativo: los DAGs deben ser código Python que llame directamente a funciones del pipeline.

### Alternativas consideradas

| Alternativa          | Pros                                                             | Contras                                                  |
| -------------------- | ---------------------------------------------------------------- | -------------------------------------------------------- |
| Airflow 3.x          | Maduro, UI rica, TaskFlow API, containerizable, Community activa | Arquitectura más compleja que alternativas más ligeras   |
| Prefect              | Python nativo, serverless option, buena DX                       | Menos madurez en containerización, comunidad más pequeña |
| Dagster              | Type-safe, asset-oriented, gran DX                               | Curva de aprendizaje alta, ecosistema menos maduro       |
| Scripts shell + cron | Simple, sin dependencias                                         | Sin visibilidad, sin gestión de dependencias, sin retry  |

## Decision

Usar **Apache Airflow 3.x** con **TaskFlow API** para orquestación.

Airflow 3.x se elige sobre versiones anteriores porque introduce `airflow dag-processor` como servicio separado, mejora la API REST nativa, y simplifica el modelo de ejecución con respecto a Airflow 2.x.

La TaskFlow API (`@dag` + `@task` decorators) permite escribir DAGs como funciones Python que llaman directamente a las funciones del pipeline — sin operadores SQL/Postgres/Bash, sin XComs explícitos, sin contextos de tirada.

### Arquitectura de containers

5 servicios en `compose.yml`:

| Servicio                | Rol                                                         |
| ----------------------- | ----------------------------------------------------------- |
| `postgres`              | Base de datos de Airflow (metadata) + capa CLEAN            |
| `airflow-webserver`     | UI + API REST nativa (`airflow api-server`)                 |
| `airflow-scheduler`     | Ejecuta tareas programadas                                  |
| `airflow-dag-processor` | **Nuevo en 3.x**: parsea DAGs y los registra en metadata DB |
| `datasette`             | Sirve datos enriquecidos (capa ENRICHED)                    |

El DAG processor es un requisito no obvio de Airflow 3.x: sin este servicio, los DAGs nunca se registran en la base de datos, causando "DAG not found" en el scheduler.

## Alternatives Considered

### Prefect

- Pros: Serverless mediante Prefect Cloud, API más limpia, Python nativo
- Cons: Prefect Cloud tiene limitaciones en free tier; self-hosted requiere más infra que Airflow. Comunidad y ecosistema de plugins más pequeño.
- Rejected: Airflow tiene mejor soporte containerizado y más recursos de aprendizaje.

### Dagster

- Pros: Modelo asset-oriented que mapea naturalmente a las 4 capas del pipeline, type-safe
- Cons: Curva de aprendizaje más pronunciada. La orquestación paralela simple de 5 DAGs no justifica la sobrecarga conceptual.
- Rejected: Para el alcance actual, Airflow es suficiente y más conocido.

### Scripts shell + cron

- Pros: Máxima simplicidad, sin dependencias
- Cons: Sin UI, sin retry automático, sin visibilidad de estado, sin gestión de dependencias entre DAGs
- Rejected: La visibilidad y el retry son requisitos para un pipeline que tarda ~63 calls de API en completarse.

## Consequences

**Positivas:**

- UI rica en `localhost:8080` con visibilidad de logs, estado, y triggers manuales.
- DAGs escritos como Python puro (TaskFlow API) que llaman directamente a `pipeline.transform_clean.run()` etc.
- Containerización estándar OCI, compatible con Podman y Docker.
- Retry automático de tareas fallidas.
- 5 DAGs independientes que se pueden ejecutar individualmente para debug.

**Negativas:**

- Complejidad operativa: 5 containers (vs 0 con scripts), configuración de networking, variables de entorno.
- Airflow 3.x es reciente (2025): documentación y recursos limitados comparado con 2.x.
- El DAG processor es un punto de fricción: sin él los DAGs no se registran y el error no es obvio.
- La inyección de DAGs requiere que el código esté disponible en la imagen del container (no hot-reload).

**Quirks documentados:**

- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` debe apuntar a `http://airflow-webserver:8080/execution/`.
- `AIRFLOW__API_AUTH__JWT_SECRET` debe ser idéntico en todos los containers Airflow.
- Datasette overrides el entrypoint de Airflow para no pasar por el wrapper `/entrypoint`.

## Referencias

- `compose.yml` — definición de servicios.
- `dags/` — 5 DAGs implementados con TaskFlow API.
- `Containerfile` — imagen OCI con dependencias pre-instaladas.
