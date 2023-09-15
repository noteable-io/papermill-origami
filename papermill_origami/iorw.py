"""The iorw module provides the handlers for registration with papermill to read/write Notebooks"""
import orjson
from jupyter_client.utils import run_sync

from papermill_origami.dependencies import get_api_client
from papermill_origami.path_util import parse_noteable_file_id_and_version_number


class NoteableHandler:
    """Defines a class which implements the interface papermill needs to pull and push content
    from Noteable using notebook ids, version ids or noteable file URLs.
    """

    @staticmethod
    async def _read(path) -> str:
        client = get_api_client()
        file_id, version_number = parse_noteable_file_id_and_version_number(path)

        if version_number is not None:
            resp = await client.client.get(f"/files/{file_id}/versions/{version_number}")
            resp.raise_for_status()
            js = resp.json()
            file_contents = js["content"]
            return orjson.dumps(file_contents)
        else:
            rtu_client = await client.connect_realtime(file_id)
            await rtu_client.shutdown()
            return rtu_client.builder.nb.json()

    @staticmethod
    def read(path) -> str:
        """Reads a file from the noteable client by either version id, file id or file url"""
        return run_sync(NoteableHandler._read)(path)

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
