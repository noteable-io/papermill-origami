import asyncio

import httpx
import nbformat
import orjson
from jupyter_client.utils import run_sync
from origami.clients.rtu import RTUClient
from origami.models.notebook import CodeCell, Notebook
from papermill.engines import Engine, NotebookExecutionManager
from papermill_origami.dependencies import get_api_client

from papermill_origami.path_util import parse_noteable_file_id_and_version_number


class NoteableEngine(Engine):
    def __init__(self):
        self.api_client = get_api_client()

    async def create_parameterized_notebook(
        self, file_id: str, file_version_id: str = None, job_instance_attempt: dict = None
    ) -> tuple[dict, dict]:
        body = {
            "notebook_version_id": file_version_id,
            "job_instance_attempt": job_instance_attempt,
        }
        resp = await self.api_client.client.post(
            f"/v1/files/{file_id}/parameterized_notebooks", json=body
        )
        resp.raise_for_status()
        js = resp.json()
        return js["parameterized_notebook"], js["job_instance_attempt"]

    async def version_id_from_number(self, file_id, version_number) -> str:
        # TODO: create a new v1 version of this endpoint
        #       that returns a presigned url instead of the full file contents
        resp = await self.api_client.client.get(f"/files/{file_id}/versions/{version_number}")
        resp.raise_for_status()
        return resp.json()["file_version"]["id"]

    async def _execute_managed_notebook(
        self, notebook_execution_manager: NotebookExecutionManager, kernel_name: str, **kwargs
    ):
        # Get file id and file version number from input_path
        file_id, version_number = parse_noteable_file_id_and_version_number(kwargs["input_path"])

        # Get the version_id from the version_number if it exists
        # This would be the second time we call the API to get the version_id
        # First is in NoteableHandler where the file contents are fetched
        version_id = None
        if version_number is not None:
            version_id = await self.version_id_from_number(file_id, version_number)
        print(f"version_id: {version_id}")
        # Create a parameterized notebook with the file_id as the source notebook
        parameterized_notebook, job_instance_attempt = await self.create_parameterized_notebook(
            file_id, version_id, job_instance_attempt=kwargs.get("job_instance_attempt")
        )

        async with httpx.AsyncClient() as plain_client:
            resp = await plain_client.get(parameterized_notebook["presigned_download_url"])
            resp.raise_for_status()
        noteable_nb = nbformat.from_dict(resp.json())

        errored = False
        kernel_session_id = None
        try:
            rtu_client = await self.api_client.connect_realtime(parameterized_notebook["id"])

            # Updates the noteable notebook with changes papermill made to the notebook
            # This is necessary because papermill could inject or replace the
            # parameters cell with a new cell tagged `injected-parameters`.
            await self.sync_noteable_nb_with_papermill(
                rtu_client=rtu_client,
                noteable_nb=noteable_nb,
                papermill_nb=notebook_execution_manager.nb,
            )

            # Launch kernel
            kernel_session = await self.api_client.launch_kernel(
                file_id=parameterized_notebook["id"],
            )
            kernel_session_id = kernel_session.id
            await rtu_client.wait_for_kernel_idle()

            # Update job instance attempt status to RUNNING
            if job_instance_attempt:
                await self.api_client.client.patch(
                    f"/v1/job-instance-attempts/{job_instance_attempt['id']}",
                    json={"status": "RUNNING"},
                )

            # Execute all cells
            queued_execution = await rtu_client.queue_execution(run_all=True)
            cells: list[CodeCell] = await asyncio.gather(*queued_execution)

            # Fetch error output and set it on the papermill managed notebook
            for cell in cells:
                if rtu_client.cell_states.get(cell.id) == "finished_with_error":
                    errored = True
                    koc = await self.api_client.get_output_collection(cell.output_collection_id)
                    papermill_nb_cell = next(
                        c for c in notebook_execution_manager.nb.cells if c.id == cell.id
                    )
                    papermill_nb_cell.outputs = [
                        self._convert_noteable_output_to_jupyter_output(output=output)
                        for output in koc.outputs
                    ]

            if job_instance_attempt:
                await self.api_client.client.patch(
                    f"/v1/job-instance-attempts/{job_instance_attempt['id']}",
                    json={"status": "FAILED" if errored else "SUCCEEDED"},
                )
        except Exception as e:  # noqa
            if job_instance_attempt:
                await self.api_client.client.patch(
                    f"/v1/job-instance-attempts/{job_instance_attempt['id']}",
                    json={"status": "FAILED"},
                )
            raise e
        finally:
            # if not errored:
            if kernel_session_id:
                await self.api_client.shutdown_kernel(kernel_session_id)

        return notebook_execution_manager.nb

    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name, **kwargs):
        return run_sync(cls()._execute_managed_notebook)(nb_man, kernel_name, **kwargs)

    async def sync_noteable_nb_with_papermill(
        self, rtu_client: RTUClient, noteable_nb: Notebook, papermill_nb: Notebook
    ):
        """Used to sync the cells of in-memory notebook representation that papermill manages with the Noteable notebook

        Papermill injects a new parameters cell with tag `injected-parameters` after a cell tagged `parameters`.
        """

        noteable_nb_cell_ids = [cell['id'] for cell in noteable_nb.cells]
        papermill_nb_cell_ids = [cell['id'] for cell in papermill_nb.cells]

        deleted_cell_ids = list(set(noteable_nb_cell_ids) - set(papermill_nb_cell_ids))
        added_cell_ids = list(set(papermill_nb_cell_ids) - set(noteable_nb_cell_ids))

        for cell_id in deleted_cell_ids:
            await rtu_client.delete_cell(cell_id)
        for cell_id in added_cell_ids:
            idx = papermill_nb_cell_ids.index(cell_id)
            after_id = papermill_nb_cell_ids[idx - 1] if idx > 0 else None
            await rtu_client.add_cell(cell=papermill_nb.cells[idx], after_id=after_id)

    @staticmethod
    def _convert_noteable_output_to_jupyter_output(output):
        """Converts a Noteable KernelOutput to a Jupyter NotebookNode output

        Note:
        - clear_output:
            Noteable backend will never send an explicit clear_output event,
            but will instead send an empty list of outputs to clear the cell
        - update_display_data:
            Noteable backend will never send an explicit update_display_data event,
            but will instead send an update_outputs_by_display_id_event
            with a list of outputs to update by collection_id
        """
        # TODO: Handle fetching and parsing content via output.content.url
        content = output.content.raw
        if output.type == "error":
            error_data = orjson.loads(content)
            return nbformat.v4.new_output(
                "error",
                **error_data,
            )
        elif output.type == "stream":
            return nbformat.v4.new_output(
                "stream",
                text=content,
            )
        elif output.type == "execute_result":
            return nbformat.v4.new_output(
                "execute_result",
                data={output.content.mimetype: content},
            )
        elif output.type == "display_data":
            return nbformat.v4.new_output(
                "display_data",
                data={output.content.mimetype: content},
            )
        else:
            raise ValueError(f"Unhandled output type: {output.type}")
