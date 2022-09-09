import uuid
from datetime import datetime, timezone

import nbformat.v4
import pytest
from origami.types.access_levels import Visibility
from origami.types.files import FileType, NotebookFile

from papermill_origami import NoteableEngine


@pytest.fixture
def file_content():
    return nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("1 + 1") for _ in range(10)])


@pytest.fixture
def file(file_content):
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
        content=file_content,
    )


@pytest.fixture
def noteable_engine(mocker, file):
    mock_noteable_client = mocker.AsyncMock()
    mock_noteable_client.get_notebook.return_value = file
    return NoteableEngine(nb_man=mocker.Mock(), km=mocker.Mock(), client=mock_noteable_client)
