import copy
import os
import sys
import tempfile
import uuid
from base64 import b64encode
from typing import Any, Dict, List, Mapping, Optional, Set, Union, cast

import cloudpickle as pickle
import nbformat
import papermill
from dagster import In, OpDefinition, Out
from dagster import _check as check
from dagster._core.definitions.events import AssetMaterialization, Failure, Output, RetryRequested
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.definitions.utils import validate_tags
from dagster._core.execution.context.compute import SolidExecutionContext
from dagster._core.execution.context.input import build_input_context
from dagster._core.execution.context.system import StepExecutionContext
from dagster._core.execution.plan.outputs import StepOutputHandle
from dagster._utils import safe_tempfile_path
from dagster._utils.error import serializable_error_info_from_exc_info
from dagstermill import _load_input_parameter, _reconstitute_pipeline_context
from dagstermill.compat import ExecutionError
from dagstermill.factory import _find_first_tagged_cell_index, get_papermill_parameters
from papermill.iorw import load_notebook_node, write_ipynb
from papermill.translators import PythonTranslator

from ..util import parse_noteable_file_id
from .context import SerializableExecutionContext


def _dm_compute(
    dagster_factory_name,
    name,
    notebook_path,
    output_notebook_name=None,
    asset_key_prefix=None,
    output_notebook=None,
):
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    check.opt_str_param(output_notebook_name, "output_notebook_name")
    check.opt_list_param(asset_key_prefix, "asset_key_prefix")
    check.opt_str_param(output_notebook, "output_notebook")

    def _t_fn(step_context, inputs):
        check.inst_param(step_context, "step_context", SolidExecutionContext)
        check.param_invariant(
            isinstance(step_context.run_config, dict),
            "context",
            "StepExecutionContext must have valid run_config",
        )

        step_execution_context: StepExecutionContext = step_context.get_step_execution_context()

        with tempfile.TemporaryDirectory() as output_notebook_dir:
            with safe_tempfile_path() as output_log_path:

                prefix = str(uuid.uuid4())
                parameterized_notebook_path = os.path.join(
                    output_notebook_dir, f"{prefix}-inter.ipynb"
                )

                executed_notebook_path = os.path.join(output_notebook_dir, f"{prefix}-out.ipynb")

                # Scaffold the registration here
                nb = load_notebook_node(notebook_path)
                compute_descriptor = "op"

                base_parameters = get_papermill_parameters(
                    step_execution_context,
                    inputs,
                    output_log_path,
                    compute_descriptor,
                )
                job_definition_id = step_execution_context.job_name
                job_instance_id = step_execution_context.run_id
                file_id = parse_noteable_file_id(notebook_path)
                noteable_parameters = {
                    'job_definition_id': job_definition_id,
                    'job_instance_id': job_instance_id,
                    'file_id': file_id,
                }
                context_args = base_parameters["__dm_context"]
                pipeline_context_args = dict(
                    executable_dict=base_parameters["__dm_executable_dict"],
                    pipeline_run_dict=base_parameters["__dm_pipeline_run_dict"],
                    solid_handle_kwargs=base_parameters["__dm_solid_handle_kwargs"],
                    instance_ref_dict=base_parameters["__dm_instance_ref_dict"],
                    step_key=base_parameters["__dm_step_key"],
                    **context_args,
                )
                reconstituted_pipeline_context = _reconstitute_pipeline_context(
                    **pipeline_context_args
                )
                serializable_ctx = SerializableExecutionContext(
                    pipeline_tags=reconstituted_pipeline_context._pipeline_context.log.logging_metadata.pipeline_tags,
                    op_config=reconstituted_pipeline_context.op_config,
                    resources=reconstituted_pipeline_context.resources,
                    run_id=reconstituted_pipeline_context.run_id,
                    run=reconstituted_pipeline_context.run,
                    solid_handle=reconstituted_pipeline_context.solid_handle,
                )
                serialized = serializable_ctx.dumps()
                serialized_context_b64 = b64encode(serialized).decode("utf-8")
                load_input_template = "cloudpickle.loads(b64decode({serialized_val}))"
                input_params_list = [
                    PythonTranslator().codify(noteable_parameters, "Noteable provided parameters"),
                    "\n# Dagster provided parameter inputs\n",
                    "\n".join(
                        [
                            f"{input_name} = {load_input_template.format(serialized_val=b64encode(pickle.dumps(_load_input_parameter(input_name))))}"  # noqa: E501
                            for input_name in base_parameters["__dm_input_names"]
                        ]
                    ),
                ]
                input_parameters = "".join(input_params_list)

                template = f"""# Injected parameters
import cloudpickle
from base64 import b64decode

serialized_context_b64 = "{serialized_context_b64}"
serialized_context = b64decode(serialized_context_b64)

context = cloudpickle.loads(serialized_context)
{input_parameters}

# Print out the applied parameter variable names and their types
parameters = {base_parameters["__dm_input_names"]}
parameter_types = [type(eval(parameter)).__name__ for parameter in parameters]

import pandas
from IPython.display import display, HTML
display(
    HTML(
        pandas.DataFrame.from_dict(
            {{"Dagster Applied Parameters": parameters, "Types": parameter_types}}
        ).to_html(index=False)
    )
)
"""

                nb_no_parameters = copy.deepcopy(nb)
                newcell = nbformat.v4.new_code_cell(source=template)
                newcell.metadata["tags"] = ["injected-parameters"]
                # Hide the injected parameters cell source by default
                newcell.metadata.setdefault("jupyter", {})["source_hidden"] = True

                param_cell_index = _find_first_tagged_cell_index(nb_no_parameters, "parameters")
                injected_cell_index = _find_first_tagged_cell_index(
                    nb_no_parameters, "injected-parameters"
                )
                if injected_cell_index >= 0:
                    # Replace the injected cell with a new version
                    before = nb_no_parameters.cells[:injected_cell_index]
                    after = nb_no_parameters.cells[injected_cell_index + 1 :]
                    check.int_value_param(param_cell_index, -1, "param_cell_index")
                    # We should have blown away the parameters cell if there is an injected-parameters cell
                elif param_cell_index >= 0:
                    # Replace the parameter cell with the injected-parameters cell
                    before = nb_no_parameters.cells[:param_cell_index]
                    after = nb_no_parameters.cells[param_cell_index + 1 :]
                else:
                    # Inject to the top of the notebook, presumably first cell includes dagstermill import
                    before = []
                    after = nb_no_parameters.cells
                nb_no_parameters.cells = before + [newcell] + after
                # nb_no_parameters.metadata.papermill["parameters"] = _seven.json.dumps(parameters)

                write_ipynb(nb_no_parameters, parameterized_notebook_path)

                try:
                    executed_nb = papermill.execute_notebook(
                        input_path=parameterized_notebook_path,
                        output_path=executed_notebook_path,
                        engine_name="noteable-dagstermill",  # noteable specific
                        log_output=True,
                        # noteable specific args
                        file_id=file_id,
                        job_metadata={
                            'job_definition_id': job_definition_id,
                            'job_instance_id': job_instance_id,
                        },
                        logger=step_execution_context.log,
                    )
                except Exception as ex:
                    step_execution_context.log.warn(
                        f"Error when attempting to materialize executed notebook: {serializable_error_info_from_exc_info(sys.exc_info())}"  # noqa: E501
                    )
                    # pylint: disable=no-member
                    # compat:
                    if isinstance(ex, ExecutionError) and (
                        ex.ename == "RetryRequested" or ex.ename == "Failure"
                    ):
                        step_execution_context.log.warn(
                            f"Encountered raised {ex.ename} in notebook. Use dagstermill.yield_event "
                            "with RetryRequested or Failure to trigger their behavior."
                        )

                    raise

            step_execution_context.log.debug(
                f"Notebook execution complete for {name} at {executed_notebook_path}."
            )
            if output_notebook_name is not None:
                # yield output notebook binary stream as an op output
                with open(executed_notebook_path, "rb") as fd:
                    yield Output(fd.read(), output_notebook_name)

            else:
                # backcompat
                executed_notebook_file_handle = None
                try:
                    # use binary mode when when moving the file since certain file_managers such as S3
                    # may try to hash the contents
                    with open(executed_notebook_path, "rb") as fd:
                        executed_notebook_file_handle = step_context.resources.file_manager.write(
                            fd, mode="wb", ext="ipynb"
                        )
                        executed_notebook_materialization_path = (
                            executed_notebook_file_handle.path_desc
                        )

                    yield AssetMaterialization(
                        asset_key=(asset_key_prefix + [f"{name}_output_notebook"]),
                        description="Location of output notebook in file manager",
                        metadata={
                            "path": MetadataValue.path(executed_notebook_materialization_path),
                            "executed_notebook": MetadataValue.url(
                                executed_nb.metadata.get("executed_notebook_url")
                            ),
                        },
                    )

                except Exception:
                    # if file manager writing errors, e.g. file manager is not provided, we throw a warning
                    # and fall back to the previously stored temp executed notebook.
                    step_context.log.warning(
                        "Error when attempting to materialize executed notebook using file manager: "
                        f"{str(serializable_error_info_from_exc_info(sys.exc_info()))}"
                        f"\nNow falling back to local: notebook execution was temporarily materialized at {executed_notebook_path}"  # noqa: E501
                        "\nIf you have supplied a file manager and expect to use it for materializing the "
                        'notebook, please include "file_manager" in the `required_resource_keys` argument '
                        f"to `{dagster_factory_name}`"
                    )

                if output_notebook is not None:
                    yield Output(executed_notebook_file_handle, output_notebook)

            # deferred import for perf
            import scrapbook

            output_nb = scrapbook.read_notebook(executed_notebook_path)

            for (
                output_name,
                _,
            ) in step_execution_context.solid_def.output_dict.items():
                data_dict = output_nb.scraps.data_dict
                if output_name in data_dict:
                    # read outputs that were passed out of process via io manager from `yield_result`
                    step_output_handle = StepOutputHandle(
                        step_key=step_execution_context.step.key,
                        output_name=output_name,
                    )
                    output_context = step_execution_context.get_output_context(step_output_handle)
                    io_manager = step_execution_context.get_io_manager(step_output_handle)
                    value = io_manager.load_input(
                        build_input_context(
                            upstream_output=output_context, dagster_type=output_context.dagster_type
                        )
                    )

                    yield Output(value, output_name)

            for key, value in output_nb.scraps.items():
                if key.startswith("event-"):
                    with open(value.data, "rb") as fd:
                        event = pickle.loads(fd.read())
                        if isinstance(event, (Failure, RetryRequested)):
                            raise event
                        else:
                            yield event

    return _t_fn


def define_noteable_dagster_op(
    name: str,
    notebook_id: str,
    ins: Optional[Mapping[str, In]] = None,
    outs: Optional[Mapping[str, Out]] = None,
    config_schema: Optional[Union[Any, Dict[str, Any]]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    output_notebook_name: Optional[str] = None,
    asset_key_prefix: Optional[Union[List[str], str]] = None,
    description: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None,
):
    """Wrap a Jupyter notebook in a op. Copied from `define_dagstermill_op`.

    Arguments:
        name (str): The name of the op.
        notebook_id (str): ID of the backing notebook.
        ins (Optional[Mapping[str, In]]): The op's inputs.
        outs (Optional[Mapping[str, Out]]): The op's outputs. Your notebook should
            call :py:func:`~dagstermill.yield_result` to yield each of these outputs.
        config_schema (Optional[Union[Any, Dict[str, Any]]]): The op's config schema.
        required_resource_keys (Optional[Set[str]]): The string names of any required resources.
        output_notebook_name: (Optional[str]): If set, will be used as the name of an injected output
            of type of :py:class:`~dagster.BufferedIOBase` that is the file object of the executed
            notebook (in addition to the :py:class:`~dagster.AssetMaterialization` that is always
            created). It allows the downstream ops to access the executed notebook via a file
            object.
        asset_key_prefix (Optional[Union[List[str], str]]): If set, will be used to prefix the
            asset keys for materialized notebooks.
        description (Optional[str]): If set, description used for op.
        tags (Optional[Dict[str, str]]): If set, additional tags used to annotate op.
            Dagster uses the tag keys `notebook_path` and `kind`, which cannot be
            overwritten by the user.
    Returns:
        :py:class:`~dagster.OpDefinition`
    """
    check.str_param(name, "name")

    # TODO - make these parameterizable/from env vars?
    notebook_path = f"noteable://{notebook_id}"
    domain = os.getenv("NOTEABLE_DOMAIN", "app.noteable.io")
    notebook_url = f"https://{domain}/f/{notebook_id}"

    required_resource_keys = set(
        check.opt_set_param(required_resource_keys, "required_resource_keys", of_type=str)
    )
    outs = check.opt_mapping_param(outs, "outs", key_type=str, value_type=Out)
    ins = check.opt_mapping_param(ins, "ins", key_type=str, value_type=In)

    if output_notebook_name is not None:
        required_resource_keys.add("output_notebook_io_manager")
        outs = {
            **outs,
            cast(str, output_notebook_name): Out(io_manager_key="output_notebook_io_manager"),
        }

    if isinstance(asset_key_prefix, str):
        asset_key_prefix = [asset_key_prefix]

    asset_key_prefix = check.opt_list_param(asset_key_prefix, "asset_key_prefix", of_type=str)

    default_description = f"This op is backed by the notebook at {notebook_url}"
    description = check.opt_str_param(description, "description", default=default_description)

    user_tags = validate_tags(tags)
    if tags is not None:
        check.invariant(
            "notebook_path" not in tags,
            "user-defined solid tags contains the `notebook_path` key, but the `notebook_path` key is reserved for use by Dagster",  # noqa: E501
        )
        check.invariant(
            "kind" not in tags,
            "user-defined solid tags contains the `kind` key, but the `kind` key is reserved for use by Dagster",
        )
    default_tags = {"notebook_path": notebook_url, "kind": "noteable"}

    return OpDefinition(
        name=name,
        compute_fn=_dm_compute(
            "define_noteable_dagster_op",
            name,
            notebook_path,
            output_notebook_name,
            asset_key_prefix=asset_key_prefix,
        ),
        ins=ins,
        outs=outs,
        config_schema=config_schema,
        required_resource_keys=required_resource_keys,
        description=description,
        tags={**user_tags, **default_tags},
    )
