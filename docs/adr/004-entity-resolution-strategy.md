# ADR-004: Entity Resolution Strategy for Player Identity

**Status:** Accepted
**Date:** 2026-03-25
**Decision makers:** Juanje Márquez

## Context

El pipeline integra 2 fuentes de datos de fútbol (API-Football y Understat) que asignan IDs propios a los mismos jugadores sin ningún identificador compartido. Por ejemplo, un jugador puede tener `player_id: 1100` en API-Football y `player_id: 8872` en Understat, con nombres diferentes ("Pedro González López" vs "Pedri"). Sin entity resolution, no es posible cruzar stats observables (API-Football) con métricas avanzadas xG (Understat) por jugador.

### Restricciones clave

- Understat no expone `birth_date` ni `nationality`, eliminando las señales de deduplicación más habituales.
- El límite de calls diarios de API-Football (100/día) impide re-consultas masivas para depuración.
- Cada fuente usa su propio sistema de IDs opacos sin campo de cruce estándar.

### Datos disponibles para matching

| Campo            | API-Football                                         | Understat                                |
| ---------------- | ---------------------------------------------------- | ---------------------------------------- |
| Nombre           | `name`, `firstname`, `lastname`                      | `player_name`                            |
| Posición         | `position` (Goalkeeper/Defender/Midfielder/Attacker) | `position` (códigos: "M S", "D M", etc.) |
| Equipo           | `team_name` + `team_id`                              | `team`                                   |
| Fecha nacimiento | `birth_date` (ISO)                                   | No disponible                            |
| Nacionalidad     | `nationality`                                        | No disponible                            |
| Apariciones      | `appearances`                                        | `games`                                  |
| Minutos          | `minutes`                                            | `minutes`                                |

## Decision

Implementar entity resolution en 2 fases secuenciales: primero equipos, luego jugadores. La resolución de jugadores usa 4 pasadas en orden descendente de confianza:

### Fase 1: Team Resolution

Los equipos (~20 en La Liga) se resuelven primero porque sus nombres son más estables y cortos que los de jugadores, permitiendo fuzzy matching más preciso. El equipo resuelto se usa como contexto reductor en la resolución de jugadores (~500 candidatos → ~25 por equipo).

**Pasada 1** — Exact match (confidence 1.0): nombres normalizados con `unidecode` + lowercase + strip.
**Pasada 2** — Fuzzy match (confidence 0.85): `rapidfuzz.token_sort_ratio ≥ 80` entre nombres normalizados.

### Fase 2: Player Resolution — 4 pasadas

| Pasada          | Criterio                                                                                                     | Confidence           |
| --------------- | ------------------------------------------------------------------------------------------------------------ | -------------------- |
| 1 — Exact       | Nombre normalizado coincide exactamente con alguna variante del jugador API-Football + mismo equipo resuelto | 1.0                  |
| 2 — Fuzzy       | `max(token_sort_ratio, partial_ratio)` ≥ 0.85 + mismo equipo. Con tiebreak por posición si hay conflicto     | 0.90 (0.88 tiebreak) |
| 3 — Contextual  | Fuzzy ≥ 0.75 cross-team + historial de transferencias confirma                                               | 0.70                 |
| 4 — Statistical | Games ±3 AND minutes ±20% + mismo equipo + candidato único + posición compatible                             | 0.60                 |

### Normalización de nombres

Se generan múltiples variantes desde los 3 campos de API-Football (`name`, `firstname`, `lastname`) y se comparan contra el nombre único de Understat. Las variantes las construye `CandidatePool` al indexar; el método `MatchScorer.fuzzy_score` usa `token_sort_ratio` y `partial_ratio` con un length guard (`ScoringThresholds.partial_ratio_min_length_ratio = 0.6`) para evitar que variantes cortas inflen scores por substring match.

### Position mapping

Las posiciones Understat (códigos: "M S", "D M S", "G") se mapean a los buckets canónicos API-Football (Goalkeeper, Defender, Midfielder, Attacker). La compatibilidad de posición se usa como tiebreaker en Pass 2 y como filtro en Pass 4. La regla vive en un único sitio, `MatchScorer.positions_compatible`; la posición del candidato la resuelve `CandidatePool.position_of`.

## Alternatives Considered

### Matching solo por nombre sin jerarquía de equipos

- Pros: Simple de implementar
- Cons: Sin el reductor de equipo, ~500 jugadores crean demasiados falsos positivos
- Rejected: La señal de equipo es la más fiable disponible

### Usar solo fuzzy matching sin pasadas estadísticas

- Pros: Más simple, menos código
- Cons: Apodos sin overlap fonético ("Koke" ↔ "Jorge Resurrección Merodio") quedarían sin resolver
- Rejected: Pass 4 es necesaria para estos casos límite

### Servicio externo de entity resolution (Dedupe, Splink)

- Pros: Probablemente más preciso
- Cons: Dependencia externa, complejidad operativa, curvas de aprendizaje
- Rejected: El problema es lo suficientemente acotado (1 liga, ~500 jugadores) para una solución interna

### Resolver todos los jugadores en un solo paso con ML

- Pros: Podría capturar patrones complejos
- Cons: Requiere datos etiquetados (no disponibles), sobreingeniería para el alcance actual
- Rejected: Las 4 pasadas son deterministas y auditables

## Consequences

**Positivas:**

- Resolución determinista y auditable: cada match tiene un método y confidence documentados.
- Sin dependencias externas más allá de `rapidfuzz` y `unidecode`.
- El informe CSV de no resueltos permite intervención manual en casos límite.
- Arquitectura extensible: nuevas fuentes solo necesitan implementar normalización de nombres para integrarse.

**Negativas:**

- La precisión depende de la calidad de los datos de minutos/games en API-Football (Pass 4).
- Jugadores transferidos mid-season requieren datos de transferencias RAW (Pass 3) lo que añade dependencia entre capas.
- ~25% de jugadores pueden quedar no resueltos en la primera ejecución, requiriendo revisión manual o ajuste de thresholds.

**Métrica de éxito:** ≥ 18 de 20 jugadores conocidos resueltos correctamente en tests.

## Referencias

- Spec detallado: `docs/entity-resolution-spec.md`
- Vocabulario de dominio: `CONTEXT.md`
- Implementación (estrategia de 4 pasadas): `src/pipeline/entity_resolution.py`
- Implementación (scoring de candidatos): `src/pipeline/match_scoring.py`
- Implementación (índice de candidatos): `src/pipeline/candidate_pool.py`
- Implementación (estado de matcheo): `src/pipeline/resolution_ledger.py`
- Tests: `tests/test_entity_resolution.py`, `tests/test_match_scoring.py`, `tests/test_candidate_pool.py`, `tests/test_resolution_ledger.py`
