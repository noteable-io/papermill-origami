"""This module holds the NoteableEngine class used to register the noteable engine with papermill.

It enables papermill to run notebooks against Noteable as though it were executing a notebook locally.
"""
import asyncio
import functools
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
from origami.defs.files import NotebookFile
from origami.defs.jobs import (
    CustomerJobDefinitionReferenceInput,
    CustomerJobInstanceReferenceInput,
    JobInstanceAttempt,
    JobInstanceAttemptRequest,
    JobInstanceAttemptStatus,
    JobInstanceAttemptUpdate,
)
from origami.defs.rtu import (
    AppendOutputEventSchema,
    BulkCellStateMessage,
    DisplayHandlerUpdateEventSchema,
    KernelOutput,
    KernelOutputType,
    UpdateOutputCollectionEventSchema,
)
from papermill.engines import Engine, NotebookExecutionManager

from .manager import NoteableKernelManager
from .util import flatten_dict, parse_noteable_file_id

logger = logging.getLogger(__name__)


def ensure_client(func):
    @functools.wraps(func)
    async def client_context_wrapper(obj, *args, **kwargs):
        if obj.client is None:
            # Assume env variables supply config arguments
            async with NoteableClient() as client:
                try:
                    obj.client = client
                    return await func(obj, *args, **kwargs)
                finally:
                    obj.client = None
        else:
            return await func(obj, *args, **kwargs)

    return client_context_wrapper


class NoteableEngine(Engine):
    """The subclass that can be registered with papermill to handle notebook executions."""

    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name=None, **kwargs):
        """The interface method used by papermill to initiate an execution request"""
        return run_sync(cls(nb_man, client=kwargs.pop('client', None), **kwargs).execute)(
            kernel_name=kernel_name, **kwargs
        )

    def __init__(
        self,
        nb_man: NotebookExecutionManager,
        client: Optional[NoteableClient] = None,
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
        # Map parent_collection_id to cell_id in order to process any append_output_events
        # which are uniquely identified by parent_collection_id and not cell_id
        self.__noteable_output_collection_cache = {}

        # Used to store created Jupyter outputs for the corresponding Noteable cell output by its output_id.
        # This is so that we can mutate the output in the event of a display handler update (e.g. progress bar)
        # Currently, this is only used when the output is of type display_data.
        self.__noteable_output_id_cache = {}
        self.file = None
        self.job_instance_attempt: Optional[JobInstanceAttempt] = None

    def catch_cell_metadata_updates(self, func):
        """A decorator for catching cell metadata updates related to papermill
        and updating the Noteable notebook via RTU"""

        @functools.wraps(func)
        def wrapper(cell, *args, **kwargs):
            ret_val = func(cell, *args, **kwargs)
            # Update Noteable cell metadata
            if not cell.metadata.get("papermill"):
                return ret_val
            for key, value in flatten_dict(
                cell.metadata.papermill, parent_key_tuple=("papermill",)
            ).items():
                run_sync(self.km.client.update_cell_metadata)(
                    file=self.file,
                    cell_id=cell.id,
                    metadata_update_properties={"path": key, "value": value},
                )
            return ret_val

        return wrapper

    @ensure_client
    async def execute(self, **kwargs):
        """Executes a notebook using Noteable's APIs"""
        # Use papermill-origami logger if one is not provided
        if not kwargs.get("logger"):
            kwargs["logger"] = logger

        ext_logger = kwargs["logger"]

        dagster_context = kwargs.get("dagster_context")

        # The original notebook id can either be the notebook file id or notebook version id
        original_notebook_id = kwargs.get("file_id")
        if original_notebook_id is None:
            maybe_file = kwargs.get("file")
            maybe_input_path = kwargs.get("input_path")
            if maybe_file:
                original_notebook_id = maybe_file.id
            elif maybe_input_path and (file_id := parse_noteable_file_id(maybe_input_path)):
                original_notebook_id = file_id
            else:
                raise ValueError("No file_id or derivable file_id found")

        job_instance_attempt_request = (
            JobInstanceAttemptRequest.parse_obj(kwargs.get('job_instance_attempt'))
            if kwargs.get('job_instance_attempt')
            else None
        )

        # Setup job instance attempt from the provided customer job metadata
        if job_metadata := kwargs.get("job_metadata", {}):
            version = await self.client.get_version_or_none(original_notebook_id)
            if version is not None:
                space_id = version.space_id
            else:
                file = await self.client.get_notebook(original_notebook_id)
                space_id = file.space_id

            # 1: Ensure the job definition and instance references exists
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
            job_instance_attempt_request = JobInstanceAttemptRequest(
                status=JobInstanceAttemptStatus.CREATED,
                attempt_number=0,
                customer_job_instance_reference_id=job_instance.id,
            )

        # Create the parameterized_notebook
        resp = await self.client.create_parameterized_notebook(
            original_notebook_id, job_instance_attempt=job_instance_attempt_request
        )
        self.file = resp.parameterized_notebook
        self.job_instance_attempt = resp.job_instance_attempt
        parameterized_url = f"https://{self.client.config.domain}/f/{self.file.id}"

        self.nb.metadata["executed_notebook_url"] = parameterized_url
        self.nb.metadata["parameterized_notebook_id"] = str(self.file.id)
        if dagster_context:
            from dagster import AssetObservation, DagsterEvent, MetadataValue
            from dagster._core.events import AssetObservationData

            asset_obs = AssetObservation(
                asset_key=dagster_context.asset_key_for_output(),
                description="Parameterized notebook available at",
                metadata={"parameterized_notebook_url": MetadataValue.url(parameterized_url)},
            )
            event = DagsterEvent(
                event_type_value="ASSET_OBSERVATION",
                pipeline_name=dagster_context.job_name,
                solid_handle=dagster_context.op_handle,
                event_specific_data=AssetObservationData(asset_obs),
            )
            ext_logger.log_dagster_event(
                level="INFO", msg="Parameterized notebook available at", dagster_event=event
            )
        else:
            ext_logger.info(f"Parameterized notebook available at {parameterized_url}")
        # HACK: We need this delay in order to successfully subscribe to the files channel
        #       of the newly created parameterized notebook.
        await asyncio.sleep(1)

        try:
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
                    ext_logger=ext_logger,
                )

                # Sync metadata from papermill to noteable before execution
                await self.sync_noteable_nb_metadata_with_papermill()

                # We're going to start executing the notebook; Update the job instance attempt status to RUNNING
                if self.job_instance_attempt:
                    logger.debug(
                        f"Updating job instance attempt id {self.job_instance_attempt.id} to status RUNNING"
                    )
                    await self.client.update_job_instance(
                        job_instance_attempt_id=self.job_instance_attempt.id,
                        job_instance_attempt_update=JobInstanceAttemptUpdate(
                            status=JobInstanceAttemptStatus.RUNNING
                        ),
                    )

                await self.papermill_execute_cells()

                # This is a hack to ensure we have the client in session to send nb metadata
                # updates over RTU after execution.
                self.nb_man.notebook_complete()
                await self.sync_noteable_nb_metadata_with_papermill()

                # Override the notebook_complete method and set it to a no-op (since we already called it)
                self.nb_man.notebook_complete = lambda: None

                # info_msg = self.wait_for_reply(self.kc.kernel_info())
                # self.nb.metadata['language_info'] = info_msg['content']['language_info']
        except:  # noqa
            logger.exception("Error executing notebook")
            if self.job_instance_attempt:
                await self.client.update_job_instance(
                    job_instance_attempt_id=self.job_instance_attempt.id,
                    job_instance_attempt_update=JobInstanceAttemptUpdate(
                        status=JobInstanceAttemptStatus.FAILED
                    ),
                )
            raise

        return self.nb

    @ensure_client
    async def sync_noteable_nb_with_papermill(
        self, file: NotebookFile, noteable_nb, papermill_nb, ext_logger
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

        ext_logger.info(
            "Synced notebook with Noteable, "
            f"added {len(added_cell_ids)} cells and deleted {len(deleted_cell_ids)} cells"
        )

    async def sync_noteable_nb_metadata_with_papermill(self):
        """Used to sync the papermill metadata of in-memory notebook representation that papermill manages with
        the Noteable notebook"""
        if not self.nb.metadata.get("papermill"):
            return
        for key, value in flatten_dict(
            self.nb.metadata.papermill, parent_key_tuple=("papermill",)
        ).items():
            await self.km.client.update_nb_metadata(self.file, {"path": key, "value": value})

    @staticmethod
    def create_kernel_manager(file: NotebookFile, client: NoteableClient, **kwargs):
        """Helper that generates a kernel manager object from kwargs"""
        return NoteableKernelManager(file, client, **kwargs)

    @asynccontextmanager
    async def setup_kernel(self, cleanup_kc=True, cleanup_kc_on_error=False, **kwargs) -> Generator:
        """Context manager for setting up the kernel to execute a notebook."""
        ext_logger = kwargs["logger"]

        if self.km is None:
            # Assumes that file and client are being passed in
            self.km = self.create_kernel_manager(**kwargs)

        # Subscribe to the file or we won't see status updates
        await self.client.subscribe_file(self.km.file, from_version_id=self.file.current_version_id)
        ext_logger.info("Subscribed to file")

        await self.km.async_start_kernel(**kwargs)
        ext_logger.info("Started kernel")

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

    def _cell_start(self, cell, cell_index=None, **kwargs):
        self.catch_cell_metadata_updates(self.nb_man.cell_start)(cell, cell_index, **kwargs)

    def _cell_exception(self, cell, cell_index=None, **kwargs):
        self.catch_cell_metadata_updates(self.nb_man.cell_exception)(cell, cell_index, **kwargs)
        # Manually update the Noteable nb metadata
        run_sync(self.km.client.update_nb_metadata)(
            self.file, {"path": ["papermill", "exception"], "value": True}
        )

    def _cell_complete(self, cell, cell_index=None, **kwargs):
        self.catch_cell_metadata_updates(self.nb_man.cell_complete)(cell, cell_index, **kwargs)

    @ensure_client
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

        files_channel = self.km.client.files_channel(file_id=self.km.file.id)
        self.km.client.register_message_callback(
            self._update_outputs_callback,
            files_channel,
            "update_output_collection_event",
            response_schema=UpdateOutputCollectionEventSchema,
            once=False,
        )

        self.km.client.register_message_callback(
            self._append_outputs_callback,
            files_channel,
            "append_output_event",
            response_schema=AppendOutputEventSchema,
            once=False,
        )

        self.km.client.register_message_callback(
            self._display_handler_update_callback,
            files_channel,
            "update_outputs_by_display_id_event",
            response_schema=DisplayHandlerUpdateEventSchema,
            once=False,
        )

        self.km.client.register_message_callback(
            self._update_execution_count_callback,
            self.km.kernel.kernel_channel,
            "bulk_cell_state_update_event",
            response_schema=BulkCellStateMessage,
            once=False,
        )

        # Execute each cell and update the output in real time.
        errored = False
        for index, cell in enumerate(self.nb.cells):
            try:
                self._cell_start(cell, index)
                await self.async_execute_cell(cell, index)
            except CellExecutionError as ex:
                # TODO: Make sure we raise these
                self._cell_exception(self.nb.cells[index], index, exception=ex)
                errored = True
                break
            finally:
                self._cell_complete(self.nb.cells[index], cell_index=index)

        # Update the job instance attempt status
        if self.job_instance_attempt:
            status = (
                JobInstanceAttemptStatus.FAILED if errored else JobInstanceAttemptStatus.SUCCEEDED
            )
            logger.debug(
                f"Updating job instance attempt id {self.job_instance_attempt.id} to status {status}"
            )
            await self.client.update_job_instance(
                job_instance_attempt_id=self.job_instance_attempt.id,
                job_instance_attempt_update=JobInstanceAttemptUpdate(status=status),
            )

        if not errored:
            # Delete the kernel session
            logger.debug("Deleting kernel session for file id %s", self.file.id)
            await self.client.delete_kernel_session(self.file)

    def _get_timeout(self, cell: Optional[NotebookNode]) -> int:
        """Helper to fetch a timeout as a value or a function to be run against a cell"""
        if self.timeout_func is not None and cell is not None:
            timeout = self.timeout_func(cell)
        else:
            timeout = self.timeout

        if not timeout or timeout < 0:
            timeout = None

        return timeout

    def _get_cell_index(self, cell_id: str) -> int:
        """Used to get the index of a cell in the papermill notebook representation.

        We don't want to cache this because the
        cell index can change if cells are added or deleted during execution,
        which is not currently implemented, but could be in the future.
        """
        for idx, nb_cell in enumerate(self.nb.cells):
            if nb_cell.id == cell_id:
                return idx
        raise ValueError(f"Cell with id {cell_id} not found")

    async def _update_outputs_callback(self, resp: UpdateOutputCollectionEventSchema):
        """Callback to set cell outputs observed from Noteable over RTU into the
        corresponding cell outputs here in Papermill
        """
        if not resp.data.outputs:
            # Clear output
            self.nb.cells[self._get_cell_index(resp.data.cell_id)].outputs = []
            return True

        for output in resp.data.outputs:
            new_output = self._convert_noteable_output_to_jupyter_output(output)

            self.__noteable_output_collection_cache[output.parent_collection_id] = resp.data.cell_id

            # Cache the created output so that we can mutate it later if an
            # update_outputs_by_display_id_event is received against this output_id
            if output.type == KernelOutputType.display_data:
                self.__noteable_output_id_cache[str(output.id)] = new_output

            self.nb.cells[self._get_cell_index(resp.data.cell_id)].outputs.append(new_output)

        # Mark the callback as successful
        return True

    async def _append_outputs_callback(self, resp: AppendOutputEventSchema):
        """
        Callback to append cell outputs observed from Noteable over RTU into the
        corresponding cell outputs here in Papermill
        """
        cell_id = self.__noteable_output_collection_cache.get(resp.data.parent_collection_id)

        if cell_id is None:
            raise SkipCallback("Nothing found to append to")

        new_output = self._convert_noteable_output_to_jupyter_output(resp.data)

        if resp.data.type == KernelOutputType.display_data:
            self.__noteable_output_id_cache[str(resp.data.id)] = new_output

        self.nb.cells[self._get_cell_index(cell_id)].outputs.append(new_output)

        # Mark the callback as successful
        return True

    async def _display_handler_update_callback(self, resp: DisplayHandlerUpdateEventSchema):
        outputs_to_update = [
            self.__noteable_output_id_cache[output_id] for output_id in resp.data.output_ids
        ]
        if not outputs_to_update:
            # Nothing to update
            return False

        for output in outputs_to_update:
            new_output = nbformat.v4.new_output(
                "display_data",
                data={resp.data.content.mimetype: resp.data.content.raw},
            )
            output.update(**new_output)
        return True

    async def _update_execution_count_callback(self, resp: BulkCellStateMessage):
        """Callback to set cell execution count observed from Noteable over RTU into the
        corresponding cell execution count here in Papermill
        """
        for cell_state in resp.data.cell_states:
            cell_index = self._get_cell_index(cell_state.cell_id)
            self.nb.cells[cell_index].execution_count = cell_state.execution_count

        # Mark the callback as successful
        return True

    @staticmethod
    def _convert_noteable_output_to_jupyter_output(output: KernelOutput):
        """Converts a Noteable KernelOutput to a Jupyter NotebookNode output

        Note:
        - KernelOutputType.clear_output:
            Noteable backend will never send an explicit clear_output event,
            but will instead send an empty list of outputs to clear the cell
        - KernelOutputType.update_display_data:
            Noteable backend will never send an explicit update_display_data event,
            but will instead send an update_outputs_by_display_id_event
            with a list of outputs to update by collection_id
        """
        # TODO: Handle fetching and parsing content via output.content.url
        content = output.content.raw
        if output.type == KernelOutputType.error:
            error_data = orjson.loads(content)
            return nbformat.v4.new_output(
                "error",
                **error_data,
            )
        elif output.type == KernelOutputType.stream:
            return nbformat.v4.new_output(
                "stream",
                text=content,
            )
        elif output.type == KernelOutputType.execute_result:
            return nbformat.v4.new_output(
                "execute_result",
                data={output.content.mimetype: content},
            )
        elif output.type == KernelOutputType.display_data:
            return nbformat.v4.new_output(
                "display_data",
                data={output.content.mimetype: content},
            )
        else:
            raise SkipCallback(f"Unhandled output type: {output.type}")

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
        elif not cell.source.strip():
            logger.debug("Skipping empty code cell %s", cell_index)
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

        result = await self.km.client.execute(self.km.file, cell.id)
        # TODO: This wasn't behaving correctly with the timeout?!
        # result = await asyncio.wait_for(self.km.client.execute(self.km.file, cell.id), self._get_timeout(cell))
        if result.state.is_error_state:
            # TODO: Add error info from stacktrace output messages
            raise CellExecutionError("", str(result.state), "Cell execution failed")
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
