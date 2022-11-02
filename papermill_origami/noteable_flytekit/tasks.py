import typing
from typing import Any

import papermill as pm
from flytekit import FlyteContext, PythonInstanceTask
from flytekit.extend import Interface, TaskPlugins, TypeEngine
from flytekit.loggers import logger
from flytekitplugins.papermill import NotebookTask
from flytekitplugins.papermill.task import PAPERMILL_TASK_PREFIX, T, _dummy_task_func

from papermill_origami.util import parse_noteable_file_id


class NoteableNotebookTask(NotebookTask):
    def __init__(
        self,
        name: str,
        notebook_path: str,
        render_deck: bool = False,
        task_config: T = None,
        inputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        outputs: typing.Optional[typing.Dict[str, typing.Type]] = None,
        **kwargs,
    ):
        # Each instance of NotebookTask instantiates an underlying task with a dummy function that will only be used
        # to run pre- and post- execute functions using the corresponding task plugin.
        # We rename the function name here to ensure the generated task has a unique name and avoid duplicate task name
        # errors.
        # This seem like a hack. We should use a plugin_class that doesn't require a fake-function to make work.
        plugin_class = TaskPlugins.find_pythontask_plugin(type(task_config))
        self._config_task_instance = plugin_class(
            task_config=task_config, task_function=_dummy_task_func, **kwargs
        )
        # Rename the internal task so that there are no conflicts at serialization time. Technically these internal
        # tasks should not be serialized at all, but we don't currently have a mechanism for skipping Flyte entities
        # at serialization time.
        self._config_task_instance._name = f"{PAPERMILL_TASK_PREFIX}.{name}"
        task_type = f"{self._config_task_instance.task_type}"
        task_type_version = self._config_task_instance.task_type_version
        self._notebook_path = notebook_path

        self._render_deck = render_deck

        if outputs:
            outputs.update(
                {
                    self._IMPLICIT_OP_NOTEBOOK: self._IMPLICIT_OP_NOTEBOOK_TYPE,
                    self._IMPLICIT_RENDERED_NOTEBOOK: self._IMPLICIT_RENDERED_NOTEBOOK_TYPE,
                }
            )

        # avoid call to NotebookTask.__init__ which does things we don't want to do.
        # instead call its parent class's __init__ directly.
        super(PythonInstanceTask, self).__init__(
            name,
            task_config,
            task_type=task_type,
            task_type_version=task_type_version,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    @property
    def output_notebook_path(self) -> str:
        # ensure the output path is on the local filesystem
        return parse_noteable_file_id(super().output_notebook_path)

    @property
    def rendered_output_path(self) -> str:
        # ensure the output path is on the local filesystem
        return parse_noteable_file_id(super().rendered_output_path)

    def execute(self, **kwargs) -> Any:
        """
        TODO: Figure out how to share FlyteContext ExecutionParameters with the notebook kernel (as notebook kernel
             is executed in a separate python process)
        For Spark, the notebooks today need to use the new_session or just getOrCreate session and get a handle to the
        singleton
        """
        logger.info(f"Hijacking the call for task-type {self.task_type}, to call notebook.")
        # Execute Notebook via Papermill.
        pm.execute_notebook(
            self._notebook_path,
            self.output_notebook_path,
            parameters=kwargs,
            engine_name="noteable",  # changed from upstream
        )  # type: ignore

        outputs = self.extract_outputs(self.output_notebook_path)
        self.render_nb_html(self.output_notebook_path, self.rendered_output_path)

        m = {}
        if outputs:
            m = outputs.literals
        output_list = []
        for k, type_v in self.python_interface.outputs.items():
            if k == self._IMPLICIT_OP_NOTEBOOK:
                output_list.append(self.output_notebook_path)
            elif k == self._IMPLICIT_RENDERED_NOTEBOOK:
                output_list.append(self.rendered_output_path)
            elif k in m:
                v = TypeEngine.to_python_value(
                    ctx=FlyteContext.current_context(), lv=m[k], expected_python_type=type_v
                )
                output_list.append(v)
            else:
                raise RuntimeError(
                    f"Expected output {k} of type {type_v} not found in the notebook outputs"
                )

        return tuple(output_list)
