from typing import Optional, Union

import origami.client
from origami.client import ClientConfig, Token
from papermill.engines import papermill_engines
from papermill.iorw import papermill_io

from .engine import NoteableEngine
from .iorw import NoteableHandler


class NoteableClient(origami.client.NoteableClient):
    """Extended NoteableClient that registers NoteableEngine and NoteableHandler into papermill"""

    def __init__(
        self,
        api_token: Optional[Union[str, Token]] = None,
        config: Optional[ClientConfig] = None,
        follow_redirects=True,
        **kwargs,
    ):
        super().__init__(
            api_token=api_token, config=config, follow_redirects=follow_redirects, **kwargs
        )
        papermill_io.register('noteable://', NoteableHandler(self))
        papermill_engines.register('noteable', NoteableEngine)
