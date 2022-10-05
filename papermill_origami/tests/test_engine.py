import copy
import logging
from unittest.mock import ANY

import nbformat
import pytest


@pytest.fixture
def mock_noteable_client(mocker, file):
    mock_noteable_client = mocker.patch(
        'papermill_origami.engine.NoteableClient', return_value=mocker.AsyncMock()
    )
    mock_noteable_client.return_value.__aenter__.return_value.create_parameterized_notebook.return_value = (
        file
    )


@pytest.fixture
def noteable_engine(mocker, file, file_content, mock_noteable_client):
    from papermill_origami.engine import (  # avoid circular import due to papermill engine registration
        NoteableEngine,
    )

    mock_nb_man = mocker.MagicMock()
    mock_nb_man.nb = copy.deepcopy(file_content)
    execute_result = mocker.Mock()
    execute_result.state.is_error_state = False

    noteable_engine = NoteableEngine(nb_man=mock_nb_man, km=mocker.AsyncMock(), client=None)

    # Set the execution result to successful
    noteable_engine.km.client.execute.return_value = execute_result

    return noteable_engine


async def test_sync_noteable_nb_with_papermill(file, file_content, mocker, noteable_engine):
    papermill_nb = copy.deepcopy(file_content)
    # Remove a cell
    deleted_cell = papermill_nb.cells.pop(1)

    # Add a cell
    added_cell = nbformat.v4.new_code_cell("2 + 2")
    after_id = papermill_nb.cells[0]['id']
    papermill_nb.cells.insert(1, added_cell)

    await noteable_engine.sync_noteable_nb_with_papermill(
        file=mocker.Mock(),
        noteable_nb=file_content,
        papermill_nb=papermill_nb,
        dagster_logger=logging.getLogger(__name__),
    )

    noteable_engine.km.client.delete_cell.assert_called_with(ANY, deleted_cell['id'])
    noteable_engine.km.client.add_cell.assert_called_with(ANY, cell=added_cell, after_id=after_id)


async def test_default_client(mocker, file, file_content, noteable_engine):
    # Ensure this doesn't explode with no client
    await noteable_engine.execute(
        file_id='fake_id',
        noteable_nb=file_content,
        logger=logging.getLogger(__name__),
    )
    # Check that we sent an execute request to the client
    noteable_engine.km.client.execute.assert_has_calls(
        [mocker.call(ANY, cell.id) for cell in file_content.cells], any_order=True
    )


async def test_ignore_empty_code_cells(mocker, file, file_content, noteable_engine):
    # Add empty code cells
    empty_cells = [nbformat.v4.new_code_cell() for _ in range(3)]
    non_empty_cells = copy.deepcopy(file_content.cells)
    file_content.cells.extend(empty_cells)

    await noteable_engine.execute(
        file_id='fake_id',
        noteable_nb=file_content,
        logger=logging.getLogger(__name__),
    )

    # Check that we did not try to execute the empty cells
    assert noteable_engine.km.client.execute.call_count == len(file_content.cells) - len(
        empty_cells
    )
    noteable_engine.km.client.execute.assert_has_calls(
        [mocker.call(ANY, cell.id) for cell in non_empty_cells],
        any_order=True,
    )


async def test_propagate_cell_execution_error(mocker, file, file_content, noteable_engine):
    execute_results = [mocker.Mock() for _ in range(len(file_content.cells))]

    # Set all cell outputs to be successful
    for execute_result in execute_results:
        execute_result.state.is_error_state = False

    # Set the last cell to be an error
    execute_results[-1].state.is_error_state = True

    noteable_engine.km.client.execute.side_effect = execute_results

    await noteable_engine.execute(
        file_id='fake_id',
        noteable_nb=file_content,
        logger=logging.getLogger(__name__),
    )

    noteable_engine.nb_man.cell_exception.assert_called_once_with(
        file_content.cells[-1], cell_index=len(file_content.cells) - 1, exception=ANY
    )
