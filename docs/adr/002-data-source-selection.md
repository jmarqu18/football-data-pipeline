# ADR-002: Selección de fuentes de datos

**Status:** Accepted
**Date:** 2026-03-20
**Decision makers:** Juanje Márquez

## Context

Este pipeline necesita integrar múltiples fuentes de datos de fútbol para reconstruir una visión completa post-FBref (enero 2026, Opta retiró métricas avanzadas). El proyecto es un portfolio público, lo que impone restricciones adicionales: las fuentes deben ser gratuitas, éticamente utilizables y respetuosas con los Terms of Service de cada proveedor.

Se evaluaron las siguientes fuentes:

| Fuente | Datos disponibles | Viabilidad |
|--------|------------------|------------|
| **FBref** | Stats básicas agregadas (goles, minutos, tarjetas, tiros) | ✅ Permite scraping personal/educativo. `soccerdata` respeta sus rate limits. |
| **Understat** | xG, xA, npxG, xGChain, xGBuildup (season) + shot-level data | ✅ Acceso público, scraping educativo aceptable. Métricas avanzadas únicas. |
| **API-Football** | Stats de jugador, lesiones, transferencias, imágenes | ✅ API REST con free tier (100 calls/día). Uso legítimo vía API key. |
| **Transfermarkt** | Valores de mercado, historial de traspasos, lesiones | ❌ TOS prohíben explícitamente scraping automatizado. |
| **FotMob** | Stats avanzadas, heatmaps, momentum | ❌ Sin API pública. Scraping viola TOS. |
| **WhoScored** | Ratings, stats detalladas (datos Opta) | ❌ Datos Opta bajo licencia. Scraping viola TOS y derechos de Opta. |

## Decision

Las 3 fuentes seleccionadas para el pipeline son:

1. **FBref (Sports Reference)** — Stats base y estructura de competiciones/temporadas.
2. **Understat** — Métricas avanzadas de rendimiento (xG family) y datos shot-level.
3. **API-Football** — Datos de scouting (lesiones, transferencias, imágenes) y stats complementarias.

Se descartan Transfermarkt, FotMob y WhoScored por incompatibilidad ética y legal con un proyecto público.

## Consequences

**Positivas:**
- El pipeline es 100% reproducible por cualquier usuario con una API key gratuita de API-Football.
- No hay riesgo legal ni reputacional en un portfolio público.
- Las 3 fuentes cubren La Liga actual con overlap suficiente para entity resolution.
- Cada fuente aporta datos distintos y complementarios (stats base + xG avanzado + scouting).

**Negativas:**
- No se dispone de valores de mercado (Transfermarkt) ni ratings compuestos (WhoScored).
- API-Football free tier limita a 100 calls/día, requiriendo cache agresivo y config de scope.

**Mitigación:**
- El sistema de config (`config/ingestion.yaml`) permite añadir fuentes futuras si cambian sus condiciones de acceso.
- El README documenta explícitamente el cumplimiento de TOS de cada proveedor.
