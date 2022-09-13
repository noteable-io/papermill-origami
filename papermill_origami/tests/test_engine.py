import copy
import logging
from unittest.mock import ANY

import nbformat


async def test_sync_noteable_nb_with_papermill(file, file_content, mocker, noteable_engine):
    mock_noteable_client = mocker.AsyncMock()
    noteable_engine.km.client = mock_noteable_client
    noteable_nb = file_content

    papermill_nb = copy.deepcopy(noteable_nb)
    # Remove a cell
    deleted_cell = papermill_nb.cells.pop(1)

    # Add a cell
    added_cell = nbformat.v4.new_code_cell("2 + 2")
    after_id = papermill_nb.cells[0]['id']
    papermill_nb.cells.insert(1, added_cell)

    await noteable_engine.sync_noteable_nb_with_papermill(
        file=mocker.Mock(),
        noteable_nb=noteable_nb,
        papermill_nb=papermill_nb,
        dagster_logger=logging.getLogger(__name__),
    )

    mock_noteable_client.delete_cell.assert_called_with(ANY, deleted_cell['id'])
    mock_noteable_client.add_cell.assert_called_with(ANY, cell=added_cell, after_id=after_id)
