"""A Papermill engine that combines Dagstermill and Noteable."""
import nbformat
from nbclient.exceptions import CellExecutionError
from papermill import PapermillExecutionError

from ..engine import NoteableEngine


class NoteableDagstermillEngine(NoteableEngine):
    async def execute(self, **kwargs):
        job_metadata = kwargs.setdefault("job_metadata", {})
        job_metadata["orchestrator_id"] = "dagster"
        job_metadata["orchestrator_name"] = "Dagster"
        return await super().execute(**kwargs)

    async def papermill_execute_cells(self):
        try:
            # Run the Noteable execute cells
            await super().papermill_execute_cells()
        finally:
            # Look for existing cells with tag `injected-teardown` and delete them
            injected_teardown_cell_ids = [
                (idx, cell['id'])
                for idx, cell in enumerate(self.nb_man.nb.cells)
                if "injected-teardown" in cell['metadata'].get('tags', [])
            ]

            for idx, cell_id in injected_teardown_cell_ids:
                self.nb_man.nb.cells.pop(idx)
                await self.km.client.delete_cell(self.file, cell_id=cell_id)

            # After execution or on error, run the Dagstermill teardown
            new_cell = nbformat.v4.new_code_cell(
                source="import dagstermill as __dm_dagstermill\n__dm_dagstermill._teardown()\n"
            )
            new_cell.metadata["tags"] = ["injected-teardown"]
            new_cell.metadata["papermill"] = {
                "exception": None,
                "start_time": None,
                "end_time": None,
                "duration": None,
                "status": self.nb_man.PENDING,
            }
            index = len(self.nb_man.nb.cells)
            after_id = self.nb_man.nb.cells[-1]["id"]

            self.nb_man.nb.cells = self.nb_man.nb.cells + [new_cell]
            await self.km.client.add_cell(self.file, cell=new_cell, after_id=after_id)

            try:
                self.nb_man.cell_start(new_cell, index)
                await self.async_execute_cell(new_cell, index)
            except (PapermillExecutionError, CellExecutionError) as ex:
                self.nb_man.cell_exception(self.nb.cells[index], cell_index=index, exception=ex)
            finally:
                self.nb_man.cell_complete(self.nb.cells[index], cell_index=index)
