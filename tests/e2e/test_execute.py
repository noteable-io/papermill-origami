import pytest
import nbformat
import papermill
from origami.clients.api import APIClient
from origami.models.notebook import Notebook


class TestPapermillExecute:
    @pytest.mark.parametrize("cells", (
    [
        nbformat.v4.new_code_cell(source="a = 1"),
        nbformat.v4.new_code_cell(source="b = 2"),
        nbformat.v4.new_code_cell(source="c = a + b"),
        nbformat.v4.new_code_cell(source="print(c)"),
    ],
    [
        nbformat.v4.new_code_cell(source="# Parameters\na = 2\nb = 3\n", metadata={"tags": ["injected-parameters"]}),
        nbformat.v4.new_code_cell(source="a = 1"),
        nbformat.v4.new_code_cell(source="b = 2"),
        nbformat.v4.new_code_cell(source="c = a + b"),
        nbformat.v4.new_code_cell(source="print(c)"),
    ],
    ))
    async def test_papermill_execute_with_noteable(self, cells, notebook_maker, api_client: APIClient):
        nb = nbformat.v4.new_notebook(cells=cells, metadata={
            "kernel_info": {
              "name": "python3"
            },
            "kernelspec": {
              "display_name": "Python 3",
              "language": "python",
              "name": "python3"
            },
            "language_info": {
              "name": "python",
            },
        })
        notebook = await notebook_maker(notebook=Notebook.parse_obj(nb))
        executed_nb = papermill.execute_notebook(
            f"http://localhost:8002/f/{notebook.id}",
            "-",
            kernel_name="python3",
            # Should get added as the first cell and overwrite values of a and b
            parameters={"a": 2, "b": 3},
            engine_name="noteable",
            log_output=False,
            progress_bar=False,
        )

        parameterized_notebook_id = executed_nb.metadata['parameterized_notebook_id']
        rtu_client = await api_client.connect_realtime(parameterized_notebook_id)
        # Check that the first cell is the parameters cell
        assert rtu_client.builder.nb.cells[0].source == "# Parameters\na = 2\nb = 3\n"
        assert rtu_client.kernel_state not in ("busy", "idle")

    async def test_papermill_execution_raises_exception(self, notebook_maker, api_client: APIClient):
        nb = nbformat.v4.new_notebook(cells=[
            nbformat.v4.new_code_cell(source="a = 10\nb=20", metadata={"tags": ["parameters"]}),
            nbformat.v4.new_code_cell(source="a = 1"),
            nbformat.v4.new_code_cell(source="b = 2"),
            nbformat.v4.new_code_cell(source="c = a + b"),
            nbformat.v4.new_code_cell(source="print(c)"),
            nbformat.v4.new_code_cell(source="raise Exception('test')"),
        ])
        notebook = await notebook_maker(notebook=nb)
        executed_nb = papermill.execute_notebook(
            f"http://localhost:8002/f/{notebook.id}",
            "-",
            # Should get added as the first cell and overwrite values of a and b
            parameters={"a": 2, "b": 3},
            engine_name="noteable",
            log_output=False,
            progress_bar=False,
        )

        parameterized_notebook_id = executed_nb.metadata['parameterized_notebook_id']

        rtu_client = await api_client.connect_realtime(parameterized_notebook_id)

        # Check that the first cell is the parameters cell
        assert rtu_client.builder.nb.cells[0].source == "# Parameters\na = 2\nb = 3\n"

        # Check that there is an active kernel session
        assert rtu_client.kernel_state in ("busy", "idle")

        # TODO: figure out how to get the currently active kernel_session_id to shut
        #       it down. Until then, we'll just let it time out.

