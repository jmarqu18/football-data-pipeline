# Entity Resolution — Spec de Diseño

> **Fecha:** 2026-03-23
> **Estado:** Aprobado para implementación
> **Scope:** La Liga 2024/25, 2 fuentes (API-Football + Understat)

## Contexto

El pipeline integra 2 fuentes con IDs incompatibles. Un mismo jugador tiene `player_id: 1100` en API-Football y `player_id: 8872` en Understat, con nombres que pueden diferir significativamente ("Pedro González López" vs "Pedri"). Entity resolution fusiona estas identidades para que las capas CLEAN, FEATURES y ENRICHED operen sobre una tabla unificada de jugadores.

### Datos disponibles para matching

| Campo            | API-Football                    | Understat              |
| ---------------- | ------------------------------- | ---------------------- |
| Nombre           | `name`, `firstname`, `lastname` | `player_name` (string) |
| Fecha nacimiento | `birth_date` (ISO)              | **No disponible**      |
| Nacionalidad     | `nationality`                   | **No disponible**      |
| Equipo           | `team_name` + `team_id`         | `team`                 |
| ID propio        | `player_id`                     | `player_id`            |

**Implicación clave:** Understat no expone `birth_date` ni `nationality`. No se pueden usar como criterio de matching bidireccional. El **equipo** actúa como reductor principal de candidatos (~500 jugadores → ~25 por equipo).

**Fuente de identidad de jugadores Understat:** La resolución usa exclusivamente registros de `RawUnderstatPlayerSeason` (que tiene `player_name` y `team`). Los registros de `RawUnderstatShot` no tienen campo `team` y se vinculan a un jugador resuelto después, via `player_id`.

> **Nota:** Esta spec supersede los valores de confidence de las 3 pasadas definidos en CLAUDE.md (1.0/0.85/0.70), que asumían `birth_date` disponible en ambas fuentes. Los valores actualizados reflejan la realidad de datos asimétricos. CLAUDE.md se actualizará tras implementación.

---

## Arquitectura: 2 Fases Secuenciales

```text
Phase 1: TEAM RESOLUTION
  Input:  team_names de API-Football + team_names de Understat
  Output: tabla `teams` con canonical_name + IDs cruzados

Phase 2: PLAYER RESOLUTION (usa equipos resueltos)
  Input:  jugadores de ambas fuentes + mapping de equipos
  Output: tabla `players` con IDs cruzados + metadata de resolución
          + reporte unresolved_candidates.csv
```

---

## Phase 1: Team Resolution

~20 equipos en La Liga. Nombres relativamente estables entre fuentes.

### Pasada 1 — Exact match (confidence 1.0)

Normalizar ambos nombres (`unidecode` + lowercase + strip) → comparar.

```text
"Barcelona" (API-Football) ↔ "Barcelona" (Understat) → MATCH
```

### Pasada 2 — Fuzzy match (confidence 0.85)

`rapidfuzz.fuzz.token_sort_ratio ≥ 80` entre nombres normalizados.

```text
"Atletico Madrid" ↔ "Atlético de Madrid" → fuzzy ~88% → MATCH
```

### Resultado

Cada equipo resuelto registra:

- `canonical_name`: nombre normalizado elegido (de API-Football por tener ID estructurado)
- `api_football_id`: team_id de API-Football
- `understat_name`: nombre tal cual aparece en Understat
- `resolution_confidence` y `resolution_method`

**Si un equipo no se resuelve → ERROR log.** Con 20 equipos conocidos, cualquier fallo aquí requiere intervención.

---

## Phase 2: Player Resolution — 4 Pasadas

### Normalización de nombres

```python
def normalize_name(name: str) -> str:
    """unidecode + lowercase + strip + collapse whitespace."""
    # "Vinícius Júnior" → "vinicius junior"
    # "Pedro González López" → "pedro gonzalez lopez"
```

### Generación de variantes (API-Football)

Para cada jugador API-Football, generar variantes desde los 3 campos:

```python
def build_name_variants(name: str, firstname: str | None, lastname: str | None) -> list[str]:
    variants = {normalize(name)}          # "pedro gonzalez lopez"
    if firstname:
        variants.add(normalize(firstname))      # "pedro"
    if lastname:
        variants.add(normalize(lastname))       # "gonzalez lopez"
    if firstname and lastname:
        variants.add(normalize(f"{firstname} {lastname}"))
    return list(variants)
```

### Función de scoring

```python
def best_match_score(understat_name: str, api_variants: list[str]) -> float:
    """Max score entre token_sort_ratio y partial_ratio sobre todas las variantes."""
    norm = normalize(understat_name)
    scores = []
    for variant in api_variants:
        scores.append(fuzz.token_sort_ratio(norm, variant))
        scores.append(fuzz.partial_ratio(norm, variant))
    return max(scores) / 100.0
```

**Justificación de `partial_ratio`:** Captura apodos que son contracciones del nombre real. "Pedri" vs "Pedro" → `partial_ratio` ~90% porque "pedr" es casi substring de "pedro".

### Pass 1 — Exact name + same team → confidence 1.0

- Nombre normalizado de Understat coincide exactamente con **alguna variante** del jugador API-Football.
- Ambos pertenecen al mismo equipo (resuelto en Phase 1).
- Cubre: "Jude Bellingham" ↔ "Jude Bellingham", "Robert Lewandowski" ↔ "Robert Lewandowski".

### Pass 2 — Fuzzy name + same team → confidence 0.90

- `best_match_score ≥ 0.85` dentro del mismo equipo resuelto.
- Cubre: "Vinícius Júnior" ↔ "Vinicius Junior" (acentos), "Pedri" ↔ "Pedro" (firstname variant), "Rodrygo" ↔ "Rodrygo Goes".

### Pass 3 — Cross-team fuzzy + transfer history → confidence 0.70

- Para jugadores sin match en passes 1-2 (posible transfer mid-season).
- `best_match_score ≥ 0.75` contra TODOS los jugadores API-Football no resueltos.
- **Confirmación obligatoria:** verificar en datos RAW de transfers (`data/raw/api_football/transfers.parquet`) que el jugador estuvo en el equipo que reporta Understat durante la temporada. Se usa la capa RAW directamente (no CLEAN) para evitar dependencia circular — los datos de transfers en CLEAN requieren que `players` esté poblada, pero entity resolution es prerequisito de esa población.
- Sin confirmación de transfer → no resolver, va a unresolved.

### Pass 4 — Statistical fingerprint + same team → confidence 0.60

Última pasada para jugadores cuyo apodo no tiene overlap fonético con el nombre legal (e.g., "Koke" ↔ "Jorge Resurrección Merodio").

- Solo aplica a jugadores **unresolved tras passes 1-3** que pertenecen al **mismo equipo resuelto**.
- **Condiciones (TODAS requeridas):**
  - Diferencia de partidos (games/appearances) ≤ 3
  - Diferencia de minutos ≤ 20%
  - **Candidato único:** si >1 jugador API-Football cumple las condiciones estadísticas → no resolver (conflicto)
- Method: `'statistical'`

**Datos usados:**

- API-Football: `appearances` y `minutes` de `RawAPIFootballPlayerStats`
- Understat: `games` y `minutes` de `RawUnderstatPlayerSeason`

**Justificación:** Dentro de un equipo de ~25 jugadores, la combinación de partidos + minutos es un "fingerprint" bastante único. Tras eliminar los jugadores ya resueltos en passes 1-3, el pool restante es pequeño. La restricción de candidato único evita false positives.

**Ejemplo:**

```text
Unresolved: Understat "Koke" (Atlético, 30 games, 2400 min)
Candidates: API-Football unresolved in Atlético:
  - "Jorge Resurrección Merodio" (30 appearances, 2380 min) → ✓ unique match
  → Resolved with confidence 0.60, method 'statistical'
```

### Reglas de integridad

- **1:1 estricto:** Cada jugador de cada fuente solo puede matchear una vez.
- **Conflictos:** Si los dos mejores candidatos tienen scores con diferencia absoluta < 0.05 (e.g., 0.90 vs 0.86), el jugador va a unresolved para revisión manual.
- **Orden de resolución:** Las pasadas se ejecutan secuencialmente. Un jugador resuelto en Pass 1 no participa en Pass 2.

---

## Jugadores no resueltos

Los jugadores que no matchean en ninguna pasada:

1. **Se insertan** en `players` con solo un ID (el de su fuente). El otro ID queda NULL.
2. `resolution_confidence = NULL`, `resolution_method = 'unresolved'`.

### Valores válidos de `resolution_method`

| Valor           | Significado                                     |
| --------------- | ----------------------------------------------- |
| `'exact'`       | Pass 1: exact name + same team                  |
| `'fuzzy'`       | Pass 2: fuzzy name + same team                  |
| `'contextual'`  | Pass 3: cross-team fuzzy + transfer confirmed   |
| `'statistical'` | Pass 4: statistical fingerprint (games+minutes) |
| `'unresolved'`  | No matcheó en ninguna pasada                    |

3. Sus stats se cargan normalmente (no se pierde dato).
4. **Reporte:** Se genera `data/reports/unresolved_candidates.csv`:

```csv
source,player_id,player_name,team,candidate_name,candidate_source_id,fuzzy_score
understat,8872,Pedri,Barcelona,Pedro González López,1100,72
understat,8872,Pedri,Barcelona,Pedro Porro,2345,65
```

Top-3 candidatos más cercanos por jugador no resuelto.

---

## Observabilidad

| Nivel   | Qué se loguea                                                                     |
| ------- | --------------------------------------------------------------------------------- |
| INFO    | Total resueltos por método (exact/fuzzy/contextual/statistical), confidence media |
| INFO    | Teams: X/Y resueltos                                                              |
| WARNING | Cada jugador no resuelto + mejor candidato + score                                |
| DEBUG   | Cada comparación individual en cada pasada                                        |

---

## Casos de test (20 jugadores)

Criterio: **≥18 de 20 correctos (90%)**.

| #   | Understat name       | API-Football name                | firstname | Tipo de caso               | Pasada esperada |
| --- | -------------------- | -------------------------------- | --------- | -------------------------- | --------------- |
| 1   | Jude Bellingham      | Jude Bellingham                  | Jude      | Control positivo (exact)   | 1               |
| 2   | Robert Lewandowski   | Robert Lewandowski               | Robert    | Control positivo (exact)   | 1               |
| 3   | Vinicius Junior      | Vinícius José Paixão de Oliveira | Vinícius  | Apodo vs nombre completo   | 2               |
| 4   | Pedri                | Pedro González López             | Pedro     | Apodo ≈ firstname          | 2               |
| 5   | Rodrygo              | Rodrygo Silva de Goes            | Rodrygo   | Nombre corto vs completo   | 2               |
| 6   | Lamine Yamal         | Lamine Yamal Nasraoui Ebana      | Lamine    | Nombre parcial             | 2               |
| 7   | Antoine Griezmann    | Antoine Griezmann                | Antoine   | Control positivo           | 1               |
| 8   | Koke                 | Jorge Resurrección Merodio       | Jorge     | Apodo sin overlap → stats  | **4**           |
| 9   | Jan Oblak            | Jan Oblak                        | Jan       | Control positivo           | 1               |
| 10  | Isco                 | Francisco Román Alarcón Suárez   | Francisco | Apodo sin overlap → stats  | **4**           |
| 11  | Joselu               | José Luis Mato Sanmartín         | José Luis | Apodo parcial de firstname | 2               |
| 12  | Dani Carvajal        | Daniel Carvajal Ramos            | Daniel    | Diminutivo + apellido      | 2               |
| 13  | Transfer Player      | Transfer Player                  | Transfer  | Transferido mid-season     | 3               |
| 14  | Alexander Sørloth    | Alexander Sorloth                | Alexander | Carácter nórdico           | 1               |
| 15  | Álvaro Morata        | Álvaro Morata                    | Álvaro    | Acentos españoles          | 1               |
| 16  | Only In Understat    | (no existe)                      | —         | Solo una fuente            | unresolved      |
| 17  | Only In API-Football | Only In API-Football             | Only      | Solo una fuente            | unresolved      |
| 18  | Ferran Torres        | Ferran Torres García             | Ferran    | Nombre parcial             | 2               |
| 19  | Iñaki Williams       | Inaki Williams Arthuer           | Inaki     | Ñ + apellido extra         | 2               |
| 20  | Hugo Duro            | Hugo Duro Perales                | Hugo      | Nombre parcial             | 2               |

**Análisis de resolubilidad:**

- **14 resueltos por nombre** (passes 1-3): #1-7, #9, #11-15, #18-20
- **2 resueltos por stats** (pass 4): #8 (Koke), #10 (Isco) — apodos sin overlap fonético, pero con fingerprint estadístico único en su equipo
- **2 unresolved esperados**: #16 (solo Understat), #17 (solo API-Football) — no tienen par en la otra fuente
- **Resultado esperado: 16/16 matcheables resueltos = 100%**

**Nota sobre Koke (#8) e Isco (#10):** Estos apodos no tienen relación fonética con el nombre legal (`koke` vs `jorge`, `isco` vs `francisco`). Pass 4 los resuelve por fingerprint estadístico (games + minutes) dentro de su equipo, con confidence 0.60. El test fixture debe asegurar que sus stats sean únicas dentro del equipo para que el candidato sea único.

**Criterio de test:** De los 20 casos, 16 tienen par en ambas fuentes. El test valida:

- ≥14 de 16 matcheables resueltos correctamente (≥87.5%)
- 0 false positives (los 2 unresolved no se matchean erróneamente)
- Pass 4 resuelve al menos 1 de los 2 casos de apodo extremo (#8, #10)

---

## Archivos a crear/modificar

| Archivo                             | Acción  | Descripción                                           |
| ----------------------------------- | ------- | ----------------------------------------------------- |
| `src/pipeline/entity_resolution.py` | Rewrite | Lógica completa: team + player resolution             |
| `src/pipeline/models/clean.py`      | Create  | Pydantic models: `ResolvedTeam`, `ResolvedPlayer`     |
| `tests/test_entity_resolution.py`   | Create  | Test 20 jugadores + tests unitarios de normalización  |
| `tests/fixtures/entity_resolution/` | Create  | Fixtures con datos de ambas fuentes para 20 jugadores |
| `docs/entity-resolution-spec.md`    | Create  | Este documento                                        |

## Dependencias

- `rapidfuzz` — ya en `pyproject.toml`
- `unidecode` — **añadir** a `pyproject.toml` (no está actualmente)

---

## Validación end-to-end

1. **Unit tests:** `pytest tests/test_entity_resolution.py -v`
   - Test de normalización de nombres
   - Test de generación de variantes
   - Test de scoring
   - Test de 20 jugadores conocidos (≥18 correctos)
   - Test de manejo de unresolved
   - Test de generación de reporte CSV

2. **Integration:** Ejecutar entity resolution contra datos RAW reales (Parquet) y verificar output en consola/logs.
