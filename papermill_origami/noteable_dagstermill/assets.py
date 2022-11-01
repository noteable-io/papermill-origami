import copy
import os
import sys
import tempfile
import uuid
from base64 import b64encode
from typing import Any, Dict, Mapping, Optional, Set, Union

import cloudpickle as pickle
import dagster._check as check
import nbformat
import papermill
from dagster import (
    AssetIn,
    AssetKey,
    MetadataValue,
    Nothing,
    Output,
    PartitionsDefinition,
    ResourceDefinition,
    asset,
)
from dagster._core.definitions.events import CoercibleToAssetKeyPrefix
from dagster._core.definitions.utils import validate_tags
from dagster._core.execution.context.compute import SolidExecutionContext
from dagster._core.execution.context.system import StepExecutionContext
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
    name,
    notebook_path,
    notebook_url,
):
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    check.str_param(notebook_url, "notebook_url")

    def _t_fn(context, **inputs):
        check.inst_param(context, "context", SolidExecutionContext)
        check.param_invariant(
            isinstance(context.run_config, dict),
            "context",
            "StepExecutionContext must have valid run_config",
        )

        step_execution_context: StepExecutionContext = context.get_step_execution_context()

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
                        dagster_context=context,
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
            return Output(
                None,
                metadata={
                    "latest_successful_executed_notebook": MetadataValue.url(
                        executed_nb.metadata.get("executed_notebook_url")
                    )
                },
            )

    return _t_fn


def define_noteable_dagster_asset(
    name: str,
    notebook_id: str,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    ins: Optional[Mapping[str, AssetIn]] = None,
    non_argument_deps: Optional[Union[Set[AssetKey], Set[str]]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    config_schema: Optional[Union[Any, Dict[str, Any]]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
    description: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    op_tags: Optional[Dict[str, Any]] = None,
    group_name: Optional[str] = None,
):
    """Creates a Dagster asset for a Noteable notebook.

    Arguments:
        name (str): The name for the asset
        notebook_id (str): The id of the Noteable notebook
        key_prefix (Optional[Union[str, Sequence[str]]]): If provided, the asset's key is the
            concatenation of the key_prefix and the asset's name, which defaults to the name of
            the decorated function. Each item in key_prefix must be a valid name in dagster (ie only
            contains letters, numbers, and _) and may not contain python reserved keywords.
        ins (Optional[Mapping[str, AssetIn]]): A dictionary that maps input names to information
            about the input.
        non_argument_deps (Optional[Union[Set[AssetKey], Set[str]]]): Set of asset keys that are
            upstream dependencies, but do not pass an input to the asset.
        config_schema (Optional[ConfigSchema): The configuration schema for the asset's underlying
            op. If set, Dagster will check that config provided for the op matches this schema and fail
            if it does not. If not set, Dagster will accept any config provided for the op.
        metadata (Optional[Dict[str, Any]]): A dict of metadata entries for the asset.
        required_resource_keys (Optional[Set[str]]): Set of resource handles required by the notebook.
        description (Optional[str]): Description of the asset to display in Dagit.
        partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
            compose the asset.
        op_tags (Optional[Dict[str, Any]]): A dictionary of tags for the op that computes the asset.
            Frameworks may expect and require certain metadata to be attached to a op. Values that
            are not strings will be json encoded and must meet the criteria that
            `json.loads(json.dumps(value)) == value`.
        group_name (Optional[str]): A string name used to organize multiple assets into groups. If not provided,
            the name "default" is used.
        resource_defs (Optional[Mapping[str, ResourceDefinition]]):
            (Experimental) A mapping of resource keys to resource definitions. These resources
            will be initialized during execution, and can be accessed from the
            context within the notebook.
    """

    check.str_param(name, "name")
    check.str_param(notebook_id, "notebook_id")

    notebook_path = f"noteable://{notebook_id}"
    domain = os.getenv("NOTEABLE_DOMAIN", "app.noteable.io")
    notebook_url = f"https://{domain}/f/{notebook_id}"

    required_resource_keys = set(
        check.opt_set_param(required_resource_keys, "required_resource_keys", of_type=str)
    )
    ins = check.opt_mapping_param(ins, "ins", key_type=str, value_type=AssetIn)

    if isinstance(key_prefix, str):
        key_prefix = [key_prefix]

    key_prefix = check.opt_list_param(key_prefix, "key_prefix", of_type=str)

    default_description = f"This asset is backed by the notebook at {notebook_url}"
    description = check.opt_str_param(description, "description", default=default_description)

    user_tags = validate_tags(op_tags)
    if op_tags is not None:
        check.invariant(
            "notebook_path" not in op_tags,
            "user-defined op tags contains the `notebook_path` key, but the `notebook_path` key is reserved for use by Dagster",  # noqa: E501
        )
        check.invariant(
            "kind" not in op_tags,
            "user-defined op tags contains the `kind` key, but the `kind` key is reserved for use by Dagster",
        )
    default_tags = {"notebook_path": notebook_url, "kind": "noteable"}

    return asset(
        name=name,
        key_prefix=key_prefix,
        ins=ins,
        non_argument_deps=non_argument_deps,
        metadata=metadata,
        description=description,
        config_schema=config_schema,
        required_resource_keys=required_resource_keys,
        resource_defs=resource_defs,
        partitions_def=partitions_def,
        op_tags={**user_tags, **default_tags},
        group_name=group_name,
        output_required=False,
        dagster_type=Nothing,
    )(
        _dm_compute(
            name=name,
            notebook_path=notebook_path,
            notebook_url=notebook_url,
        )
    )
