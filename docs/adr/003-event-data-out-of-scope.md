# ADR-003: Event data fuera de scope

**Status:** Accepted
**Date:** 2026-03-20
**Decision makers:** Juanje Márquez

## Context

El plan inicial del pipeline incluía event data (cada pase, tiro, presión, carry) como una de las capas de ingesta, usando StatsBomb Open Data como fuente. Durante la fase de diseño se identificaron dos problemas que hacen inviable esta línea en el sprint actual.

**Problema 1: El event data es privativo.** Opta, StatsBomb, Stats Perform y Wyscout comercializan event data bajo licencia. No existe ningún proveedor que ofrezca event data actual de forma gratuita y abierta. StatsBomb Open Data es la excepción parcial: publica datasets abiertos, pero limitados a torneos específicos y temporadas antiguas.

**Problema 2: Cobertura temporal insuficiente.** StatsBomb Open Data cubre la temporada 2015/16 como única temporada completa de las 5 grandes ligas europeas, junto con algunos torneos sueltos (Euro 2020, Mundial 2022). Esto hace inviable cruzar event data con Understat y FBref en temporadas actuales, que es el objetivo del pipeline.

## Decision

El event data queda fuera del scope de este sprint. El pipeline se centra en stats agregadas (season/player level) y shot-level data, que son accesibles de forma gratuita y actual a través de las fuentes seleccionadas (ADR-002).

## Consequences

**Positivas:**
- El pipeline trabaja con datos de la temporada actual, más relevante para portfolio y scouting real.
- Entity resolution tiene overlap completo entre las 3 fuentes en La Liga 2024/25.
- Se simplifica la arquitectura: no hay que modelar eventos individuales ni sus relaciones.

**Negativas:**
- No se pueden calcular métricas event-level como expected Threat (xT), VAEP o passing networks en este sprint.

**Futuro:**
- StatsBomb Open Data es un candidato natural como fuente adicional en sprints posteriores, cuando el foco sea modelado ML con event data histórico (xG propio, clustering de acciones, VAEP).
- Si en el futuro aparece un proveedor de event data actual con acceso gratuito, la arquitectura de 4 capas y el sistema de config permiten integrarlo sin rediseño.
