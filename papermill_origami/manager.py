"""This file implements a jupyter_client KernelManager using the origami library"""

import functools
from typing import Optional

from jupyter_client.managerabc import KernelManagerABC
from jupyter_client.utils import run_sync
from origami.client import NoteableClient
from origami.types.files import NotebookFile


class NoteableKernelManager(KernelManagerABC):
    """KernelManager for Noteable client interactions"""

    def _requires_client_context(func):
        """A helper for checking if one is in a websocket context or not"""

        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            if not self.client.in_context:
                raise ValueError("Cannot send RTU request outside of a context manager scope.")
            return await func(self, *args, **kwargs)

        return wrapper

    def __init__(
        self, file: NotebookFile, client: NoteableClient, kernel_name: Optional[str] = None, **kw
    ):
        """Sets up basic trackers for the Manager"""
        self.open_context = False
        self.client: NoteableClient = client
        self.file = file

    async def __aenter__(self):
        """Helper for context wrapping the client if so desired"""
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *excinfo):
        """Helper for context wrapping the client if so desired"""
        await self.client.__aexit__(*excinfo)

    @property
    def kernel(self):
        """Returns the session details object holding kernel session info"""
        return self.client.file_session_cache.get(self.file.id)

    # --------------------------------------------------------------------------
    # Expected functions not part of ABC
    # --------------------------------------------------------------------------

    def pre_start_kernel(self, **kw):
        """Can be overwritten to modify kw args. The first return value is always None as
        Noteable does not allow for the kernel command to be overwritten
        """
        return None, kw

    def post_start_kernel(self, **kw) -> None:
        """Can be overwritten to take actions after a kernel cleanup occurs"""
        pass

    # --------------------------------------------------------------------------
    # Kernel management
    # --------------------------------------------------------------------------

    async def async_launch_kernel(self, **kw):
        """Actually launch the kernel

        Override in a subclass to launch kernel subprocesses differently
        """
        import inspect

        func_args = inspect.signature(self.client.get_or_launch_ready_kernel_session).parameters
        filtered_kw = {k: v for k, v in kw.items() if k in func_args}
        filtered_kw.pop('file')  # We're passing it in already
        return await self.client.get_or_launch_ready_kernel_session(self.file, **filtered_kw)

    launch_kernel = run_sync(async_launch_kernel)

    async def async_start_kernel(self, **kw):
        """Launches a new kernel if not already launched"""
        _, kw = self.pre_start_kernel(**kw)
        await self.async_launch_kernel(**kw)
        self.post_start_kernel(**kw)
        return self.kernel

    start_kernel = run_sync(async_start_kernel)

    async def async_shutdown_kernel(self, now=False, restart=False):
        """Shutdown the active or pending kernel pod"""
        await self.client.delete_kernel_session(self.file)

    shutdown_kernel = run_sync(async_shutdown_kernel)

    async def async_restart_kernel(self, now=False, **kw):
        """Restarts a kernel process by forcibly generating a new k8 pod"""
        raise NotImplementedError("TODO")

    restart_kernel = run_sync(async_restart_kernel)

    async def async_has_kernel(self):
        """Causes a request to be made to check on kernel"""
        # TODO: Change to RTU update instead of polling
        session = await self.client.get_kernel_session(self.file)
        return session is not None and not session.kernel.execution_state.is_gone

    has_kernel = run_sync(async_restart_kernel)

    def async_interrupt_kernel(self):
        """Interrupts active execution on a live kernel"""
        raise NotImplementedError("TODO")

    interrupt_kernel = run_sync(async_interrupt_kernel)

    def signal_kernel(self, signum):
        """Not Implemented: Kernel managers can normally forward signals to process based kernels"""
        raise NotImplementedError("Direct process signaling is not allowed for Noteable kernels")

    async def async_is_alive(self):
        """Causes a request to be made to check on kernel"""
        # TODO: Change to RTU update instead of polling
        session = await self.client.get_kernel_session(self.file)
        return session is not None and session.kernel.execution_state.kernel_is_alive

    is_alive = run_sync(async_is_alive)
