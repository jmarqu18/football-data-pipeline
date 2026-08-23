# CONTEXT.md — Vocabulario de dominio

Este fichero fija el lenguaje del proyecto: qué significa cada término y qué módulo lo encarna. Si un concepto tiene nombre aquí, el código usa ese nombre y no un sinónimo.

Para *cómo* trabajar en el repo (comandos, convenciones, estructura), ver [CLAUDE.md](CLAUDE.md). Para las decisiones y su porqué, ver [docs/adr/](docs/adr/).

## Capas

El pipeline mueve datos por cuatro capas. El nombre de la capa identifica el estado de los datos, no el almacenamiento.

| Término      | Significado                                                                        |
| ------------ | ---------------------------------------------------------------------------------- |
| **RAW**      | Los datos tal cual llegaron de la fuente, validados con Pydantic pero sin cruzar.   |
| **CLEAN**    | Datos con identidad resuelta entre fuentes. Es donde vive el modelo relacional.     |
| **FEATURES** | Métricas derivadas (per-90, xG overperformance, percentiles). No añade identidades. |
| **ENRICHED** | Vista desnormalizada para exploración humana.                                       |

## Entity resolution

El problema central del proyecto: API-Football y Understat identifican al mismo jugador con IDs distintos y nombres distintos, y no hay campo de cruce. Ver [ADR-004](docs/adr/004-entity-resolution-strategy.md).

| Término                   | Significado                                                                                                                                                |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pasada** (_pass_)       | Un intento de emparejar, con un criterio y una confidence fijos. Hay cuatro, en orden descendente de confianza: exact, fuzzy, contextual, statistical.      |
| **Candidato**             | Un jugador de API-Football que podría ser el jugador de Understat que se está resolviendo. Las pasadas puntúan candidatos.                                  |
| **Confidence**            | Cuánto se fía el pipeline de un emparejamiento, en 0.0–1.0. La fija la pasada que lo produjo, no se calcula.                                                |
| **Resolution method**     | Qué pasada produjo el emparejamiento: `exact`, `fuzzy`, `contextual`, `statistical` o `unresolved`. Se persiste en CLEAN.                                   |
| **Single-source player**  | Jugador que existe en API-Football y que ninguna pasada emparejó. Llega igualmente a la tabla `players` con `resolution_method='unresolved'` y sin confidence. |
| **Unresolved player**     | Jugador de **Understat** que ninguna pasada emparejó. No llega a `players`: sale en el informe CSV con sus mejores candidatos para revisión manual.          |
| **Canonical name**        | El nombre oficial, el de API-Football ya decodificado. Es el que manda.                                                                                     |
| **Known name**            | El nombre corto o apodo, el de Understat, cuando difiere del canonical ("Pedri" frente a "Pedro González López").                                            |

> Ojo con la asimetría: **single-source** y **unresolved** describen los dos lados de un fallo de emparejamiento, pero acaban en sitios distintos. Un jugador solo en API-Football se guarda; uno solo en Understat se reporta.

## Módulos del dominio

| Módulo                                                            | De qué es dueño                                                                                                                                              |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`name_normalization`](src/pipeline/name_normalization.py)         | **Preparar** nombres para comparar: decodificar entidades HTML, quitar diacríticos, generar variantes. Produce texto preparado; no lo compara.               |
| [`match_scoring`](src/pipeline/match_scoring.py)                   | **Comparar** un candidato con un jugador de Understat: score de nombre, compatibilidad de posición, huella estadística, detección de ambigüedad.              |
| [`resolution_ledger`](src/pipeline/resolution_ledger.py)           | **Quién ya está emparejado** y qué se ha resuelto. Único escritor de los registros resueltos; convierte "nadie se empareja dos veces" en invariante.          |
| [`entity_resolution`](src/pipeline/entity_resolution.py)           | **La estrategia**: el orden de las pasadas, sus criterios de elegibilidad y sus confidences. Es lo único que decide *si* un emparejamiento cuenta.            |

La separación entre los tres primeros y el cuarto es deliberada: preparar, comparar y contabilizar no son decisiones de dominio; qué cuenta como emparejamiento válido sí lo es, y ADR-004 exige que viva en un solo sitio.

### Resolution Ledger

Las cuatro pasadas recorren la misma población y tienen que saber quién sigue disponible. Antes, cada una manipulaba a mano tres estructuras mutables; olvidar una actualización producía un doble emparejamiento que no se manifestaba hasta el `INSERT` en PostgreSQL, donde `players.api_football_id` es `UNIQUE`.

El ledger es dueño de ese estado. Las pasadas **preguntan** (`unmatched_among`, `has_understat`) y **registran** (`record_match`, `record_single_source`); no mutan conjuntos. Registrar un duplicado lanza `DoubleMatchError` en la pasada que lo causó, no tres capas más abajo.

El `resolved_at` se inyecta al construirlo, de modo que toda una ejecución lleva el mismo timestamp y la salida es determinista en tests.
