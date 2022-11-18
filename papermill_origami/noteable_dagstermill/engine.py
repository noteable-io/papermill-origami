"""A Papermill engine that combines Dagstermill and Noteable."""
from ..engine import NoteableEngine


class NoteableDagstermillEngine(NoteableEngine):
    async def execute(self, **kwargs):
        job_metadata = kwargs.setdefault("job_metadata", {})
        job_metadata["customer_job_definition_reference"] = {}
        job_metadata["customer_job_definition_reference"]["orchestrator_id"] = "dagster"
        job_metadata["customer_job_definition_reference"]["orchestrator_name"] = "Dagster"
        return await super().execute(**kwargs)
