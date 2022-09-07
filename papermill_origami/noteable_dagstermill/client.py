from typing import Optional, Union

import origami.client
from origami.client import ClientConfig, Token
from papermill.engines import papermill_engines
from papermill.iorw import papermill_io

from papermill_origami import NoteableHandler

from .engine import NoteableDagstermillEngine


class NoteableDagstermillClient(origami.client.NoteableClient):
    """Extended NoteableClient that registers NoteableDagstermillEngine and NoteableHandler into papermill"""

    def __init__(
        self,
        api_token: Optional[Union[str, Token]] = None,
        config: Optional[ClientConfig] = None,
        follow_redirects=True,
        **kwargs
    ):
        super().__init__(api_token, config, follow_redirects, **kwargs)
        papermill_io.register("noteable://", NoteableHandler(self))
        papermill_engines.register("noteable-dagstermill", NoteableDagstermillEngine)
