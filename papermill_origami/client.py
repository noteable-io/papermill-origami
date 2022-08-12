import origami.client
from papermill.engines import papermill_engines
from papermill.iorw import papermill_io

from .engine import NoteableEngine
from .iorw import NoteableHandler


class NoteableClient(origami.client.NoteableClient):
    """Extended NoteableClient that registers NoteableEngine and NoteableHandler into papermill"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        papermill_io.register('noteable://', NoteableHandler(self))
        papermill_engines.register('noteable', NoteableEngine)
