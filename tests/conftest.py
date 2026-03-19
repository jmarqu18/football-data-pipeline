"""Configuración compartida de pytest para el pipeline.

Este módulo centraliza fixtures reutilizables entre los distintos
módulos de tests.  A medida que el pipeline crezca (loaders, entity
resolution, feature engineering) se irán añadiendo fixtures que
gestionen recursos compartidos como conexiones de BD, respuestas de
API mockeadas, o DataFrames de ejemplo.
"""
