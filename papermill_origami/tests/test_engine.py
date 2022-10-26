import copy
import logging
import uuid
from datetime import datetime
from unittest.mock import ANY

import nbformat
import pytest
from origami.types.rtu import KernelOutput, KernelOutputContent, KernelOutputType
from orjson import orjson


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
        file_content.cells[-1], len(file_content.cells) - 1, exception=ANY
    )


@pytest.mark.parametrize(
    "d, expected",
    [
        ({"a": {"b": 1, "c": 2}}, {("a", "b"): 1, ("a", "c"): 2}),
        ({"a": {"b": 1, "c": {"d": 4}}}, {("a", "b"): 1, ("a", "c", "d"): 4}),
        (
            {"tags": ["parameters"], "jupyter": {"source_hidden": True}},
            {("tags",): ["parameters"], ("jupyter", "source_hidden"): True},
        ),
        ({"default_parameters": {}}, {("default_parameters",): {}}),
        ({}, {}),
    ],
)
def test_flatten_dict(d, expected):
    # avoid circular import due to papermill engine registration
    from papermill_origami.util import flatten_dict

    assert flatten_dict(d) == expected


@pytest.mark.parametrize(
    "d, parent_key_tuple, expected",
    [
        ({"a": {"b": 1, "c": 2}}, ("parent",), {("parent", "a", "b"): 1, ("parent", "a", "c"): 2}),
        (
            {"tags": ["parameters"], "jupyter": {"source_hidden": True}},
            ("metadata",),
            {
                (
                    "metadata",
                    "tags",
                ): ["parameters"],
                ("metadata", "jupyter", "source_hidden"): True,
            },
        ),
        ({}, (), {}),
    ],
)
def test_flatten_dict_with_parent_key_tuple(d, parent_key_tuple, expected):
    # avoid circular import due to papermill engine registration
    from papermill_origami.util import flatten_dict

    assert flatten_dict(d, parent_key_tuple) == expected


@pytest.fixture
def create_noteable_output():
    def _wrapper(type, content):
        return KernelOutput(
            type=type,
            available_mimetypes=["text/plain"],
            content=content,
            content_metadata=KernelOutputContent(raw="", mimetype="text/plain"),
            id=uuid.uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            parent_collection_id=uuid.uuid4(),
        )

    return _wrapper


@pytest.mark.asyncio
class TestUpdateOutputsCallback:
    async def test_update_outputs_callback_clears_outputs(
        self, mocker, file, file_content, noteable_engine
    ):
        resp = mocker.Mock()
        resp.data.outputs = []
        resp.data.cell_id = file_content.cells[0].id

        # Add a mock output to the cell
        noteable_engine.nb.cells[0].outputs = [mocker.Mock()]
        await noteable_engine._update_outputs_callback(resp)

        # Assert that the output was cleared
        assert noteable_engine.nb.cells[0].outputs == []

    @pytest.mark.parametrize(
        "type, content",
        [
            (KernelOutputType.stream, KernelOutputContent(raw="test", mimetype="text/plain")),
            (
                KernelOutputType.error,
                KernelOutputContent(
                    raw=orjson.dumps(
                        {"ename": "fake ename", "evalue": "fake evalue", "traceback": []}
                    ),
                    mimetype="text/plain",
                ),
            ),
            (
                KernelOutputType.execute_result,
                KernelOutputContent(raw="test", mimetype="text/plain"),
            ),
            (KernelOutputType.display_data, KernelOutputContent(raw="test", mimetype="text/plain")),
        ],
    )
    async def test_update_outputs_callback_updates_outputs(
        self, mocker, file, file_content, noteable_engine, create_noteable_output, type, content
    ):
        resp = mocker.Mock()
        resp.data.cell_id = file_content.cells[0].id

        noteable_output = create_noteable_output(type, content)
        resp.data.outputs = [noteable_output]

        await noteable_engine._update_outputs_callback(resp)

        jupyter_output = noteable_engine.nb.cells[0].outputs[0]
        if type == KernelOutputType.stream:
            assert jupyter_output.name == "stdout"
            assert jupyter_output.text == "test"
        elif type in (KernelOutputType.execute_result, KernelOutputType.display_data):
            assert jupyter_output.data == {"text/plain": "test"}
        elif type == KernelOutputType.error:
            assert jupyter_output.ename == "fake ename"
            assert jupyter_output.evalue == "fake evalue"
            assert jupyter_output.traceback == []

    async def test_update_outputs_callback_updates_cache(
        self, mocker, file, file_content, create_noteable_output, noteable_engine
    ):
        resp = mocker.Mock()
        resp.data.cell_id = file_content.cells[0].id

        noteable_output = create_noteable_output(
            type="display_data", content=KernelOutputContent(raw="test", mimetype="text/plain")
        )
        resp.data.outputs = [noteable_output]

        await noteable_engine._update_outputs_callback(resp)

        # Trigger a _display_handler_update_callback to check that the cache was updated
        display_handler_resp = mocker.Mock()
        display_handler_resp.data.output_ids = [str(noteable_output.id)]
        display_handler_resp.data.content.mimetype = "text/plain"
        display_handler_resp.data.content.raw = "updated text"
        await noteable_engine._display_handler_update_callback(display_handler_resp)

        # Assert that the cache was updated, and hence the output was updated
        assert noteable_engine.nb.cells[0].outputs[0].data == {"text/plain": "updated text"}


@pytest.mark.asyncio
class TestAppendOutputsCallback:
    async def test_append_outputs_callback_appends_outputs(
        self, mocker, file, file_content, noteable_engine, create_noteable_output
    ):
        resp = mocker.Mock()
        resp.data.cell_id = file_content.cells[0].id

        noteable_output = create_noteable_output(
            type="display_data", content=KernelOutputContent(raw="test", mimetype="text/plain")
        )
        parent_collection_id = noteable_output.parent_collection_id

        resp.data.outputs = [noteable_output]

        # Add an output so we have something to append to
        await noteable_engine._update_outputs_callback(resp)

        append_outputs_resp = mocker.Mock()
        append_outputs_resp.data.parent_collection_id = parent_collection_id
        append_output = create_noteable_output(
            type="display_data",
            content=KernelOutputContent(raw="appended text", mimetype="text/plain"),
        )
        append_outputs_resp.data = append_output

        # Override the parent_collection_id to be the same as the original output
        append_outputs_resp.data.parent_collection_id = parent_collection_id

        await noteable_engine._append_outputs_callback(append_outputs_resp)

        # Assert that the output was appended
        assert len(noteable_engine.nb.cells[0].outputs) == 2
        assert noteable_engine.nb.cells[0].outputs[1].data == {"text/plain": "appended text"}

        # Trigger a _display_handler_update_callback to check that the cache was updated
        display_handler_resp = mocker.Mock()
        display_handler_resp.data.output_ids = [str(noteable_output.id), str(append_output.id)]
        display_handler_resp.data.content.mimetype = "text/plain"
        display_handler_resp.data.content.raw = "updated text"
        await noteable_engine._display_handler_update_callback(display_handler_resp)

        # Assert that the cache was updated, and hence the output was updated
        assert noteable_engine.nb.cells[0].outputs[0].data == {"text/plain": "updated text"}
        assert noteable_engine.nb.cells[0].outputs[1].data == {"text/plain": "updated text"}
