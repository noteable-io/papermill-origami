from unittest.mock import ANY

import pytest

from papermill_origami.noteable_dagstermill.engine import NoteableDagstermillEngine


@pytest.fixture
def noteable_dagstermill_engine(mocker, file, file_content):
    kernel_manager = mocker.Mock()
    kernel_manager.file = file
    client = mocker.AsyncMock()
    client.get_notebook.return_value = file
    kernel_manager.client = client
    nb_manager = mocker.Mock()
    nb_manager.nb = file_content
    engine = NoteableDagstermillEngine(nb_man=nb_manager, client=client, km=kernel_manager)
    engine.file = file
    return engine


class TestPapermillExecuteCells:
    async def test_dagstermill_teardown_is_added_and_executed(
        self, noteable_dagstermill_engine, file
    ):
        await noteable_dagstermill_engine.papermill_execute_cells()

        # a few things should have happened.
        # 1) a new cell was added
        previous_cell = noteable_dagstermill_engine.nb_man.nb.cells[-2]
        cell = noteable_dagstermill_engine.nb_man.nb.cells[-1]
        assert (
            cell.source == "import dagstermill as __dm_dagstermill\n"
            "__dm_dagstermill._teardown()\n"
        )
        assert "injected-teardown" in cell.metadata.tags
        assert "papermill" in cell.metadata

        # 1.1) The cell was added over RTU too
        noteable_client = noteable_dagstermill_engine.km.client
        noteable_client.add_cell.assert_called_with(
            file,
            cell=ANY,
            after_id=previous_cell.id,
        )

        # 2) The cell was executed
        noteable_dagstermill_engine.nb_man.cell_start.assert_called_with(
            ANY, len(noteable_dagstermill_engine.nb_man.nb.cells) - 1
        )
        noteable_client.execute.assert_called_with(file, cell.id)
