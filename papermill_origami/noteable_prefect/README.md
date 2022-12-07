# Noteable Prefect Integration

## Local setup (with example flow)

1. Install this package with prefect extras: `poetry install papermill-origami -E prefect`
1. Start the prefect server: `prefect orion start`
1. Set the `NOTEABLE_DOMAIN` and `NOTEABLE_TOKEN` environment variables.
1. Run the example flow as shown below

```python
from papermill_origami.noteable_prefect import notebook
from prefect import flow


@flow
def noteable_prefect_demo():
    notebook.execute_noteable_notebook(
        notebook_path="https://app.noteable-integration.us/f/f78d668e-13f3-49da-84a9-afdece1b1e2a",
        parameters={"foo": "Hello from Prefect!"},
    )


noteable_prefect_demo()
```

The parameterized notebook link can be obtained from the logs of the flow run.
