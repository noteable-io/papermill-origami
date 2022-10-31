import papermill as pm
from airflow.models.baseoperator import BaseOperator

# TODO: should we use papermill-origami to register an entrypoint for the operator?
#       https://airflow.apache.org/docs/apache-airflow-providers/index.html#creating-your-own-providers


class NoteablePapermillOperator(BaseOperator):
    """Executes a notebook via papermill on Noteable and returns the output notebook object."""

    # TODO: should we support lineage and add inlets/outlets?

    def __init__(
        self, notebook_path: str, parameters: dict, output_path: str = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_path = notebook_path
        self.output_path = output_path
        self.parameters = parameters

    def execute(self, context):
        # TODO: Construct job instance metadata from context and pass to papermill
        return pm.execute_notebook(
            self.notebook_path,
            self.output_path,
            parameters=self.parameters,
            engine_name="noteable",
            ext_logger=self.log,
        )
