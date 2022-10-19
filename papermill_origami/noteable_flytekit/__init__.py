"""This module combines Flyte with Noteable.

To execute a noteable notebook in Flyte, use the `NoteableNotebookTask` class rather than the builtin NotebookTask from Flyte.
"""
from .tasks import NoteableNotebookTask
