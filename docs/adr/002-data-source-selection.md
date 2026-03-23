# ADR-002: Selección de fuentes de datos

**Status:** Accepted
**Date:** 2026-03-23
**Supersedes:** versión inicial del 2026-03-20
**Decision makers:** Juanje Márquez

## Context

Este pipeline necesita integrar múltiples fuentes de datos de fútbol para reconstruir una visión completa post-FBref (enero 2026, Opta retiró métricas avanzadas). El proyecto es un portfolio público, lo que impone restricciones adicionales: las fuentes deben ser gratuitas, éticamente utilizables y respetuosas con los Terms of Service de cada proveedor.

Se evaluaron las siguientes fuentes:

| Fuente            | Datos disponibles                                           | Viabilidad                                                                  |
| ----------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------- |
| **FBref**         | Stats básicas agregadas (goles, minutos, tarjetas, tiros)   | ⚠️ Scraping personal/educativo aceptable, pero datos duplican API-Football. |
| **Understat**     | xG, xA, npxG, xGChain, xGBuildup (season) + shot-level data | ✅ Acceso público, scraping educativo aceptable. Métricas avanzadas únicas. |
| **API-Football**  | Stats agregadas, lesiones, transferencias, imágenes         | ✅ API REST con free tier (100 calls/día). Uso legítimo vía API key.        |
| **Transfermarkt** | Valores de mercado, historial de traspasos, lesiones        | ❌ TOS prohíben explícitamente scraping automatizado.                       |
| **FotMob**        | Stats avanzadas, heatmaps, momentum                         | ❌ Sin API pública. Scraping viola TOS.                                     |
| **WhoScored**     | Ratings, stats detalladas (datos Opta)                      | ❌ Datos Opta bajo licencia. Scraping viola TOS y derechos de Opta.         |

### Por qué se descartó FBref

FBref perdió sus métricas avanzadas Opta en enero 2026, quedando reducida a stats básicas agregadas (goles, minutos, tarjetas, tiros) — exactamente las variables que ya cubre el endpoint `/players` de API-Football. Mantener FBref suponía añadir una dependencia de scraping frágil para datos redundantes. Frente a API-Football:

- **TOS:** API-Football es un acceso legítimo vía API key; FBref requiere scraping sujeto a rate limits y cambios HTML.
- **Estabilidad:** Una API versionada es más robusta que scraping de HTML.
- **Solapamiento:** Appearances, minutes, goals, assists, shots y cards están disponibles en API-Football sin coste adicional de integración.

## Decision

Las **2 fuentes** seleccionadas para el pipeline son:

1. **Understat** — Métricas avanzadas de rendimiento (xG family) y datos shot-level. Fuente única para estas métricas.
2. **API-Football** — Stats agregadas de temporada, scouting (lesiones, transferencias, imágenes de jugador).

Se descartan FBref (solapamiento con API-Football, menor fiabilidad de acceso), Transfermarkt, FotMob y WhoScored por incompatibilidad ética o legal con un proyecto público.

## Consequences

**Positivas:**

- El pipeline es 100% reproducible por cualquier usuario con una API key gratuita de API-Football.
- No hay riesgo legal ni reputacional en un portfolio público.
- Arquitectura simplificada: 2 fuentes en lugar de 3, entity resolution entre menos IDs.
- Cada fuente aporta datos genuinamente distintos: stats observables (API-Football) vs métricas de valor esperado (Understat).

**Negativas:**

- No se dispone de valores de mercado (Transfermarkt) ni ratings compuestos (WhoScored).
- API-Football free tier limita a 100 calls/día, requiriendo cache agresivo y config de scope.

**Mitigación:**

- El sistema de config (`config/ingestion.yaml`) permite añadir fuentes futuras si cambian sus condiciones de acceso.
- El README documenta explícitamente el cumplimiento de TOS de cada proveedor.
