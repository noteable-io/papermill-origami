"""This module combines Dagstermill with Noteable.

To accomplish this a new `noteable-dagstermill` engine is created.
This engine combines the DagstermillEngine with the NoteableEngine.
To use this engine and module, ensure you install the `dagster` extra.
"""
from papermill.engines import papermill_engines
from papermill.translators import papermill_translators

from .engine import NoteableDagstermillEngine
from .translator import DagsterTranslator

papermill_engines.register("noteable-dagstermill", NoteableDagstermillEngine)
papermill_translators.register("python", DagsterTranslator)
