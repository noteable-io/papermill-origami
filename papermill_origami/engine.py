"""This module holds the NoteableEngine class used to register the noteable engine with papermill.

It enables papermill to run notebooks against Noteable as though it were executing a notebook locally.
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Generator, Optional

import nbformat
import orjson
from jupyter_client.utils import run_sync
from nbclient.exceptions import CellExecutionError
from nbformat import NotebookNode
from origami.client import NoteableClient, SkipCallback
from origami.types.files import NotebookFile
from origami.types.jobs import (
    CustomerJobDefinitionReferenceInput,
    CustomerJobInstanceReferenceInput,
    JobInstanceAttempt,
    JobInstanceAttemptStatus,
)
from origami.types.rtu import (
    AppendOutputEventSchema,
    KernelOutputType,
    UpdateOutputCollectionEventSchema,
)
from papermill.engines import Engine, NotebookExecutionManager

from .manager import NoteableKernelManager

logger = logging.getLogger(__name__)


class NoteableEngine(Engine):
    """The subclass that can be registered with papermill to handle notebook executions."""

    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name=None, **kwargs):
        """The interface method used by papermill to initiate an execution request"""
        return run_sync(cls(nb_man, client=kwargs.pop('client'), **kwargs).execute)(
            kernel_name=kernel_name, **kwargs
        )

    def __init__(
        self,
        nb_man: NotebookExecutionManager,
        client: NoteableClient,
        km: Optional[NoteableKernelManager] = None,
        timeout_func=None,
        timeout: float = None,
        log_output: bool = False,
        stdout_file=None,
        stderr_file=None,
        **kw,
    ):
        """Initializes the execution manager.

        Parameters
        ----------
        nb_man : NotebookExecutionManager
            Notebook execution manager wrapper being executed.
        km : KernelManager (optional)
            Optional kernel manager. If none is provided, a kernel manager will
            be created.
        """
        self.nb_man = nb_man
        self.client = client
        self.km = km
        self.timeout_func = timeout_func
        self.timeout = timeout
        self.log_output = log_output
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.kernel_name = kw.get('kernel_name', '__NOT_SET__')
        self.nb = nb_man.nb
        self.__noteable_output_collection_cache = {}
        self.file = None

    async def execute(self, **kwargs):
        """Executes a notebook using Noteable's APIs"""
        dagster_logger = kwargs["logger"]

        # The original notebook id can either be the notebook file id or notebook version id
        original_notebook_id = kwargs["file_id"]

        job_instance_attempt = None
        if job_metadata := kwargs.get("job_metadata", {}):
            version = await self.client.get_version_or_none(original_notebook_id)
            if version is not None:
                space_id = version.space_id
            else:
                file = await self.client.get_notebook(original_notebook_id)
                space_id = file.space_id

            # 1: Ensure the job definition&instance references exists
            job_instance = await self.client.create_job_instance(
                CustomerJobInstanceReferenceInput(
                    orchestrator_job_instance_id=job_metadata.get('job_instance_id'),
                    orchestrator_job_instance_uri=job_metadata.get('job_instance_uri'),
                    customer_job_definition_reference=CustomerJobDefinitionReferenceInput(
                        space_id=space_id,
                        orchestrator_id=job_metadata.get('orchestrator_id'),
                        orchestrator_name=job_metadata.get('orchestrator_name'),
                        orchestrator_uri=job_metadata.get('orchestrator_uri'),
                        orchestrator_job_definition_id=job_metadata.get('job_definition_id'),
                        orchestrator_job_definition_uri=job_metadata.get('job_definition_uri'),
                    ),
                )
            )

            # 2: Set up the job instance attempt
            # TODO: update the job instance attempt status while running/after completion
            job_instance_attempt = JobInstanceAttempt(
                status=JobInstanceAttemptStatus.CREATED,
                attempt_number=0,
                customer_job_instance_reference_id=job_instance.id,
            )

        # Create the parameterized_notebook
        self.file = await self.client.create_parameterized_notebook(
            original_notebook_id, job_instance_attempt=job_instance_attempt
        )
        dagster_logger.info(
            f"Parameterized notebook available at https://{self.client.config.domain}/f/{self.file.id}"
        )
        # HACK: We need this delay in order to successfully subscribe to the files channel
        #       of the newly created parameterized notebook.
        await asyncio.sleep(1)

        async with self.setup_kernel(file=self.file, client=self.client, **kwargs):
            noteable_nb = nbformat.reads(
                self.file.content
                if isinstance(self.file.content, str)
                else json.dumps(self.file.content),
                as_version=4,
            )
            await self.sync_noteable_nb_with_papermill(
                file=self.file,
                noteable_nb=noteable_nb,
                papermill_nb=self.nb,
                dagster_logger=dagster_logger,
            )
            await self.papermill_execute_cells()
            # info_msg = self.wait_for_reply(self.kc.kernel_info())
            # self.nb.metadata['language_info'] = info_msg['content']['language_info']

        return self.nb

    async def sync_noteable_nb_with_papermill(
        self, file: NotebookFile, noteable_nb, papermill_nb, dagster_logger
    ):
        """Used to sync the cells of in-memory notebook representation that papermill manages with the Noteable notebook

        Papermill injects a new parameters cell with tag `injected-parameters` after a cell tagged `parameters`.
        This method handles the cell additions/deletions that must be communicated
        with the Noteable notebook via NoteableClient.
        """

        noteable_nb_cell_ids = [cell['id'] for cell in noteable_nb.cells]
        papermill_nb_cell_ids = [cell['id'] for cell in papermill_nb.cells]
        deleted_cell_ids = list(set(noteable_nb_cell_ids) - set(papermill_nb_cell_ids))
        added_cell_ids = list(set(papermill_nb_cell_ids) - set(noteable_nb_cell_ids))
        for cell_id in deleted_cell_ids:
            await self.km.client.delete_cell(file, cell_id)
        for cell_id in added_cell_ids:
            idx = papermill_nb_cell_ids.index(cell_id)
            after_id = papermill_nb_cell_ids[idx - 1] if idx > 0 else None
            await self.km.client.add_cell(file, cell=papermill_nb.cells[idx], after_id=after_id)

        dagster_logger.info(
            "Synced notebook with Noteable, "
            f"added {len(added_cell_ids)} cells and deleted {len(deleted_cell_ids)} cells"
        )

    @staticmethod
    def create_kernel_manager(file: NotebookFile, client: NoteableClient, **kwargs):
        """Helper that generates a kernel manager object from kwargs"""
        return NoteableKernelManager(file, client, **kwargs)

    @asynccontextmanager
    async def setup_kernel(self, cleanup_kc=True, cleanup_kc_on_error=False, **kwargs) -> Generator:
        """Context manager for setting up the kernel to execute a notebook."""
        dagster_logger = kwargs["logger"]

        if self.km is None:
            # Assumes that file and client are being passed in
            self.km = self.create_kernel_manager(**kwargs)

        # Subscribe to the file or we won't see status updates
        await self.client.subscribe_file(self.km.file, from_version_id=self.file.current_version_id)
        dagster_logger.info("Subscribed to file")

        await self.km.async_start_kernel(**kwargs)
        dagster_logger.info("Started kernel")

        try:
            yield
            # if cleanup_kc:
            #     if await self.km.async_is_alive():
            #         await self.km.async_shutdown_kernel()
        finally:
            pass
            # if cleanup_kc and cleanup_kc_on_error:
            #     if await self.km.async_is_alive():
            #         await self.km.async_shutdown_kernel()

    sync_execute = run_sync(execute)

    async def papermill_execute_cells(self):
        """This function replaces cell execution with its own wrapper.

        We are doing this for the following reasons:

        1. Notebooks will stop executing when they encounter a failure but not
           raise a `CellException`. This allows us to save the notebook with the
           traceback even though a `CellExecutionError` was encountered.

        2. We want to write the notebook as cells are executed. We inject our
           logic for that here.

        3. We want to include timing and execution status information with the
           metadata of each cell.
        """
        # Execute each cell and update the output in real time.
        for index, cell in enumerate(self.nb.cells):
            try:
                self.nb_man.cell_start(cell, index)
                await self.async_execute_cell(cell, index)
            except CellExecutionError as ex:
                # TODO: Make sure we raise these
                self.nb_man.cell_exception(self.nb.cells[index], cell_index=index, exception=ex)
                break
            finally:
                self.nb_man.cell_complete(self.nb.cells[index], cell_index=index)

    def _get_timeout(self, cell: Optional[NotebookNode]) -> int:
        """Helper to fetch a timeout as a value or a function to be run against a cell"""
        if self.timeout_func is not None and cell is not None:
            timeout = self.timeout_func(cell)
        else:
            timeout = self.timeout

        if not timeout or timeout < 0:
            timeout = None

        return timeout

    async def async_execute_cell(
        self, cell: NotebookNode, cell_index: int, **kwargs
    ) -> NotebookNode:
        """
        Executes a single code cell.

        To execute all cells see :meth:`execute`.

        Parameters
        ----------
        cell : nbformat.NotebookNode
            The cell which is currently being processed.
        cell_index : int
            The position of the cell within the notebook object.

        Returns
        -------
        output : dict
            The execution output payload (or None for no output).

        Raises
        ------
        CellExecutionError
            If execution failed and should raise an exception, this will be raised
            with defaults about the failure.

        Returns
        -------
        cell : NotebookNode
            The cell which was just processed.
        """
        assert self.km.client is not None
        if cell.cell_type != 'code':
            logger.debug("Skipping non-executing cell %s", cell_index)
            return cell

        logger.debug("Executing cell:\n%s", cell.id)

        # TODO: Handle
        # if self.record_timing and 'execution' not in cell['metadata']:
        #     cell['metadata']['execution'] = {}

        # TODO: Handle
        # cell_allows_errors = (not self.force_raise_errors) and (
        #     self.allow_errors
        #     or "raises-exception" in cell.metadata.get("tags", []))

        # By default this will wait until the cell execution status is no longer active

        async def update_outputs_callback(resp: UpdateOutputCollectionEventSchema):
            """Callback to set cell outputs observed from Noteable over RTU into the
            corresponding cell outputs here in Papermill

            TODO: This callback currently only sets error outputs. We will need to extend it to set all types of outputs
                  with a translation layer from Noteable to Jupyter.
            """
            if resp.data.cell_id != cell.id:
                raise SkipCallback("Not tracked cell")
            if not resp.data.outputs:
                raise SkipCallback("Nothing to do")

            for output in resp.data.outputs:
                # We need to map parent_collection_id to cell_id in order to process any append_output_events
                # which are uniquely identified by collection_id and not cell_id
                self.__noteable_output_collection_cache[output.parent_collection_id] = cell.id
                if output.type == KernelOutputType.error:
                    if output.content.raw:
                        error_data = orjson.loads(output.content.raw)
                        error_output = nbformat.v4.new_output("error", **error_data)
                        self.nb.cells[cell_index].outputs.append(error_output)
                        return True
            return False

        async def append_outputs_callback(resp: AppendOutputEventSchema):
            """
            Callback to append cell outputs observed from Noteable over RTU into the
            corresponding cell outputs here in Papermill

            TODO: This callback currently only sets error outputs. We will need to extend it to set all types of outputs
                  with a translation layer from Noteable to Jupyter.
            """
            output = resp.data
            cell_id = self.__noteable_output_collection_cache.get(output.parent_collection_id)

            if cell_id != cell.id:
                raise SkipCallback("Not tracked cell")

            if output.type == KernelOutputType.error:
                output = nbformat.v4.new_output("error", **orjson.loads(output.content.raw))
                self.nb.cells[cell_index].outputs.append(output)
                return True
            return False

        self.km.client.register_message_callback(
            update_outputs_callback,
            self.km.client.files_channel(file_id=self.km.file.id),
            "update_output_collection_event",
            response_schema=UpdateOutputCollectionEventSchema,
            once=False,
        )

        self.km.client.register_message_callback(
            append_outputs_callback,
            self.km.client.files_channel(file_id=self.km.file.id),
            "append_output_event",
            response_schema=AppendOutputEventSchema,
            once=False,
        )

        result = await self.km.client.execute(self.km.file, cell.id)
        # TODO: This wasn't behaving correctly with the timeout?!
        # result = await asyncio.wait_for(self.km.client.execute(self.km.file, cell.id), self._get_timeout(cell))
        logger.error(result.data.state)
        logger.error(result.data.state.is_error_state)
        if result.data.state.is_error_state:
            # TODO: Add error info from stacktrace output messages
            raise CellExecutionError("", str(result.data.state), "Cell execution failed")
        return cell

    def log_output_message(self, output):
        """Process a given output. May log it in the configured logger and/or write it into
        the configured stdout/stderr files.
        """
        if output.output_type == "stream":
            content = "".join(output.text)
            if output.name == "stdout":
                if self.log_output:
                    logger.info(content)
                if self.stdout_file:
                    self.stdout_file.write(content)
                    self.stdout_file.flush()
            elif output.name == "stderr":
                if self.log_output:
                    # In case users want to redirect stderr differently, pipe to warning
                    logger.warning(content)
                if self.stderr_file:
                    self.stderr_file.write(content)
                    self.stderr_file.flush()
        elif self.log_output and ("data" in output and "text/plain" in output.data):
            logger.info("".join(output.data['text/plain']))

    def process_message(self, *arg, **kwargs):
        """Handles logging ZMQ style messages.
        TODO: Change to account for RTU outputs here?
        """
        output = super().process_message(*arg, **kwargs)
        if output and (self.log_output or self.stderr_file or self.stdout_file):
            self.log_output_message(output)
        return output

    @classmethod
    def nb_kernel_name(cls, nb, name=None):
        """
        This method is defined to override the default `Engine.nb_kernel_name` which throws an error
        when `metadata.kernelspec.name` is not present in the notebook.
        Noteable notebooks do not store `kernelspec` metadata.
        """
        return

    @classmethod
    def nb_language(cls, nb, language=None):
        try:
            return super().nb_language(nb, language)
        except ValueError:
            return "python"
