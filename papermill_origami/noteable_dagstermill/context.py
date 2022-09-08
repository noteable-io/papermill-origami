from pathlib import Path
from typing import Any, Optional, Union

import dill
from dagster import DagsterLogManager, DagsterRun, JobDefinition, OpDefinition
from dagster._core.execution.context.compute import AbstractComputeExecutionContext


class SerializableExecutionContext(AbstractComputeExecutionContext):
    def __init__(self, pipeline_tags, op_config, resources, run_id, run) -> None:
        self._pipeline_tags = pipeline_tags
        self._op_config = op_config
        self._resources = resources
        self._run_id = run_id
        self._run = run

    def has_tag(self, key) -> bool:
        return key in self._pipeline_tags

    def get_tag(self, key: str) -> Optional[str]:
        return self._pipeline_tags.get(key)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def op_def(self) -> OpDefinition:
        pass

    @property
    def job_def(self) -> JobDefinition:
        pass

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
    def load(cls, path: Union[str, Path]) -> "SerializableExecutionContext":
        pass

    def dump(self, path: Union[str, Path]) -> None:
        with open(path, "wb") as f:
            dill.dump(self, f)

    def dumps(self) -> bytes:
        return dill.dumps(self)
