import papermill as pm
from airflow.models.baseoperator import BaseOperator


class NoteablePapermillOperator(BaseOperator):
    """Executes a notebook via papermill on Noteable and returns the output notebook object."""

    def __init__(
        self, notebook_path: str, parameters: dict, output_path: str = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_path = notebook_path
        self.output_path = output_path
        self.parameters = parameters

    def execute(self, context):
        return pm.execute_notebook(
            self.notebook_path,
            self.output_path,
            parameters=self.parameters,
            engine_name="noteable",
            ext_logger=self.log,
        )
