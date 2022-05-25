"""The iorw module provides the handlers for registration with papermill to read/write Notebooks"""

import json

from origami.client import NoteableClient
from orgiami.loop import run_sync
from papermill.exceptions import PapermillException


class NoteableHandler(object):
    """Defines a class which implements the interface papermill needs to pull and push content
    from Noteable.
    """

    def __init__(self, client: NoteableClient):
        """Tracks the noteable client to be used with the handler"""
        self.client = client

    def read(self, path):
        """Reads a file from the noteable client by id"""
        id = path.split('://')[-1]
        # Wrap the async call since we're in a blocking method
        file = run_sync(self.client.get_notebook)(id)
        # Should be a string but the type check also accept JSON
        return file.content if isinstance(file.content, str) else json.dumps(file.content)

    def listdir(self, path):
        """Lists available files in a given path relative to the file's project"""
        raise PapermillException('listdir is not supported by NoteableHandler yet')

    def write(self, buf, path):
        """Writes a notebook file back to Noteable"""
        raise PapermillException('write is not supported by NoteableHandler yet')

    def pretty_path(self, path):
        """Used for logging"""
        return path

    def register(self, pm_io_registry):
        """A helper which registers the handler with papermill's default io registry"""
        pm_io_registry.register("noteable://", self)
