import origami.client
from papermill.iorw import papermill_io

from .iorw import NoteableHandler


class NoteableClient(origami.client.NoteableClient):
    """Extended NoteableClient that registers NoteableEngine and NoteableHandler into papermill"""

    async def __aenter__(self):
        self._noteable_handler = NoteableHandler(self)
        papermill_io.register("noteable://", self._noteable_handler)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        # hack around removing the handler since papermill doesn't have a way to unregister
        papermill_io._handlers.remove(("noteable://", self._noteable_handler))
        return await super().__aexit__(exc_type, exc, tb)
