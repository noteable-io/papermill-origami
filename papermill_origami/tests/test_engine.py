import uuid
import copy
from datetime import datetime, timezone
from unittest.mock import ANY

import nbformat
import pytest
from origami.types.access_levels import Visibility
from origami.types.files import FileType, NotebookFile

from papermill_origami import NoteableEngine


@pytest.fixture
def file():
    project_id = uuid.uuid4()
    return NotebookFile(
        id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        project_id=project_id,
        filename="hello world.ipynb",
        path="folder/hello world.ipynb",
        type=FileType.notebook,
        created_by_id=uuid.uuid4(),
        visibility=Visibility.private,
        is_playground_mode_file=False,
        space_id=uuid.uuid4(),
        file_store_path=f"{project_id}/folder/hello world.ipynb",
        content={"metadata": {}},
    )


@pytest.fixture
def noteable_engine(mocker):
    return NoteableEngine(nb_man=mocker.Mock(), km=mocker.Mock())


@pytest.mark.asyncio
async def test_sync_noteable_nb_with_papermill(file, mocker, noteable_engine):
    mock_noteable_client = mocker.AsyncMock()
    noteable_engine.km.client = mock_noteable_client
    noteable_nb = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("1 + 1") for _ in range(10)])

    papermill_nb = copy.deepcopy(noteable_nb)
    # Remove a cell
    deleted_cell = papermill_nb.cells.pop(1)

    # Add a cell
    added_cell = nbformat.v4.new_code_cell("2 + 2")
    after_id = papermill_nb.cells[0]['id']
    papermill_nb.cells.insert(1, added_cell)

    await noteable_engine.sync_noteable_nb_with_papermill(file=mocker.Mock(), noteable_nb=noteable_nb, papermill_nb=papermill_nb)

    mock_noteable_client.delete_cell.assert_called_with(ANY, deleted_cell['id'])
    mock_noteable_client.add_cell.assert_called_with(ANY, cell=added_cell, after_id=after_id)
