"""The iorw module provides the handlers for registration with papermill to read/write Notebooks"""
import functools
import json
from urllib.parse import urlparse

import httpx
import structlog
from jupyter_client.utils import run_sync
from origami.client import NoteableClient
from origami.defs.files import FileVersion

from papermill_origami.util import parse_noteable_file_id

logger = structlog.get_logger(__name__)


def _ensure_client(func):
    @functools.wraps(func)
    def wrapper(obj, *args, **kwargs):
        if isinstance(obj, NoteableHandler):
            # If we're a handler, we can be sure that we have a client
            return func(obj, *args, **kwargs)
        else:
            # If we're not a handler, we need to create a handler instance
            # and then bind the function to it
            with NoteableClient() as client:
                if (url := urlparse(obj)).scheme == "https" and url.netloc != client.config.domain:
                    logger.warning(
                        "The domain from the file URL does not match the domain from the default client config"
                    )
                instance = NoteableHandler(client)
                bound_method = func.__get__(instance, instance.__class__)
                return bound_method(obj, *args, **kwargs)

    return wrapper


class NoteableHandler:
    """Defines a class which implements the interface papermill needs to pull and push content
    from Noteable using notebook ids, version ids or noteable file URLs.
    """

    def __init__(self, client: NoteableClient):
        """Tracks the noteable client to be used with the handler"""
        self.client = client

    @_ensure_client
    def read(self, path):
        """Reads a file from the noteable client by either version id, file id or file url"""
        id = parse_noteable_file_id(path)
        # Wrap the async call since we're in a blocking method
        file_version: FileVersion = run_sync(self.client.get_version_or_none)(id)
        if file_version is not None:
            resp = httpx.get(file_version.content_presigned_url)
            resp.raise_for_status()
            file_contents = resp.json()
        else:
            # Contents of this file is the last saved version (in-flight deltas are not squashed)
            file = run_sync(self.client.get_notebook)(id)
            file_contents = file.content
        # Should be a string but the type check also accept JSON
        return file_contents if isinstance(file_contents, str) else json.dumps(file_contents)

    def listdir(self, path):
        """Lists available files in a given path relative to the file's project"""
        from papermill.exceptions import (  # avoid circular imports due to papermill handler registration
            PapermillException,
        )

        raise PapermillException('listdir is not supported by NoteableHandler yet')

    def write(self, buf, path):
        """Writes a notebook file back to Noteable"""
        from papermill.exceptions import (  # avoid circular imports due to papermill handler registration
            PapermillException,
        )

        raise PapermillException('write is not supported by NoteableHandler yet')

    @classmethod
    def pretty_path(cls, path):
        """Used for logging"""
        return path
