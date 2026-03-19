# ADR-001 — Podman vs Docker

## Status
Aceptado

## Contexto
Necesitamos una estrategia de contenerización para desplegar el stack completo de nuestro pipeline de datos, que incluye Airflow, PostgreSQL y Datasette. Históricamente, Docker ha sido el estándar de la industria para esta tarea. Sin embargo, en el ecosistema moderno de contenedores, otras alternativas de entorno de ejecución de Open Container Initiative (OCI) han madurado significativamente. Debemos decidir si mantener la configuración tradicional con Docker o adoptar Podman.

## Rationale (Fundamento)
Elegimos Podman en lugar de Docker por las siguientes razones clave:
* **Rootless by default:** Podman permite ejecutar contenedores como usuario no root por defecto, ofreciendo una mejor postura de seguridad.
* **Daemonless:** A diferencia de Docker, Podman no requiere un demonio (daemon) en segundo plano persistente, reduciendo el sobrecosto y los posibles puntos únicos de fallo.
* **OCI-Compliant:** Podman cumple estrictamente con los estándares OCI, lo que significa que cualquier imagen construida con Podman funcionará en Docker y viceversa.
* **CLI-Compatible:** Los comandos de la CLI son casi idénticos a los de Docker (`alias docker=podman`), lo que hace que la transición sea sin fricciones para desarrolladores familiarizados con Docker.
* **GCP-Aligned:** Se alinea muy bien con las prácticas y despliegues modernos nativos de la nube al no requerir el demonio monolítico de Docker.

## Trade-offs (Compromisos)
* **Madurez de podman-compose:** `podman-compose` es algo menos maduro que `docker-compose`, lo que puede generar cierta fricción en orquestaciones complejas.
* **Comunidad más pequeña:** La comunidad alrededor de Podman es más pequeña que la de Docker, lo que puede suponer que haya menos respuestas en foros o tutoriales para casos extremos.
* **Mercado Laboral/Reclutamiento:** Los reclutadores y el mercado laboral suelen buscar la palabra clave "Docker" en los perfiles.
  * *Mitigación:* Lo mitigamos documentando claramente esta elección mediante este ADR y manteniendo nuestros Containerfiles completamente compatibles con OCI.

## Consecuencias
* Utilizaremos definiciones estándar de contenedores compatibles con OCI.
* Seguiremos usando el nombre `compose.yml` para nuestro archivo (en vez de `podman-compose.yml`) para mantener la compatibilidad con las herramientas del ecosistema existente.
* El archivo `README.md` documentará explícitamente los comandos tanto para `podman-compose` como para `docker-compose`.
