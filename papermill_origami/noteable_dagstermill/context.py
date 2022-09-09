import sys
from pathlib import Path
from typing import Any, Optional, Union, cast

import cloudpickle
import dagster._check as check
from dagster import DagsterLogManager, DagsterRun, JobDefinition, OpDefinition
from dagster._core.definitions import Node, NodeHandle, SolidDefinition
from dagster._core.execution.context.compute import AbstractComputeExecutionContext


class SerializableExecutionContext(AbstractComputeExecutionContext):
    """The execution context for a papermill_origami run."""

    def __init__(self, pipeline_tags, op_config, resources, run_id, run, solid_handle) -> None:
        self._pipeline_tags = pipeline_tags
        self._op_config = op_config
        self._resources = resources
        self._run_id = run_id
        self._run = run
        self._solid_handle = solid_handle

    def has_tag(self, key) -> bool:
        return key in self._pipeline_tags

    def get_tag(self, key: str) -> Optional[str]:
        return self._pipeline_tags.get(key)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def solid_handle(self) -> NodeHandle:
        return self._solid_handle

    @property
    def solid_config(self) -> Any:
        return self._op_config

    @property
    def op_handle(self) -> NodeHandle:
        return self.solid_handle

    @property
    def solid(self) -> Node:
        return self._pipeline_def.get_solid(self.solid_handle)

    @property
    def op(self) -> Node:
        return self.solid

    @property
    def solid_def(self) -> SolidDefinition:
        """SolidDefinition: The current solid definition."""
        return self.solid_def

    @property
    def op_def(self) -> OpDefinition:
        """OpDefinition: The current op definition."""
        return cast(
            OpDefinition,
            check.inst(
                self.solid_def,
                OpDefinition,
                "Called op_def on a legacy solid. Use solid_def instead.",
            ),
        )

    @property
    def job_def(self) -> JobDefinition:
        return self._job_def

    @property
    def run(self) -> DagsterRun:
        return self._run

    @property
    def resources(self) -> Any:
        return self._resources

    @property
    def log(self) -> DagsterLogManager:
        return NotImplemented

    @property
    def op_config(self) -> Any:
        return self._op_config

    @classmethod
    def load(cls, path: Union[str, Path]) -> 'SerializableExecutionContext':
        pass

    def dump(self, path: Union[str, Path]) -> None:
        cloudpickle.register_pickle_by_value(sys.modules[__name__])
        with open(path, "wb") as f:
            cloudpickle.dump(self, f)

    def dumps(self) -> bytes:
        cloudpickle.register_pickle_by_value(sys.modules[__name__])
        return cloudpickle.dumps(self)
