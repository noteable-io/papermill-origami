"""This module combines Dagstermill with Noteable.

To accomplish this a new `noteable-dagstermill` engine is created.
This engine combines the DagstermillEngine with the NoteableEngine.
To use this engine and module, ensure you install the `dagster` extra.
"""
import papermill.translators

from .translator import DagsterTranslator

papermill.translators.papermill_translators.register("python", DagsterTranslator)
