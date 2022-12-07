# Noteable Airflow Integration

## Local setup (with example DAG)

1. Install this package with airflow extras: `poetry install papermill-origami -E airflow`
1. Export `AIRFLOW_HOME` environment variable to point to your airflow home directory. The default is `~/airflow`.
    ```bash
    export AIRFLOW_HOME=~/airflow
    ```
1. Start the Airflow webserver and scheduler. The Standalone command will initialise the database, make a user, and start all components for you. Visit localhost:8080 in the browser and use the admin account details shown on the terminal to login.
   ```bash
   airflow standalone
   ```
   Note: If you don't want to see all the example DAGs that come with Airflow, you can edit the `airflow.cfg` file and set `load_examples = False`.
1. Use the following Python script to create a new example Airflow DAG and copy it into `~/airflow/dags` (or `{AIRFLOW_HOME}/dags`). The `dags` directory is where Airflow looks for DAGs by default. It needs to be created if it doesn't exist.

   Note: For the following example to work, you must have set `NOTEABLE_DOMAIN` and `NOTEABLE_TOKEN` environment variables.

    ```python
    import os
    import sys
    from datetime import datetime

    from airflow import DAG
    from papermill_origami.noteable_airflow.operator import \
        NoteablePapermillOperator

    # This is needed if the DAG is manually triggered via Airflow UI (on macOS, at least)
    if sys.platform == "darwin":
        # https://github.com/apache/airflow/discussions/24463#discussioncomment-3614767
        os.environ["NO_PROXY"] = "*"

    with DAG("example_noteable_airflow", start_date=datetime.now()):
        noteable_task = NoteablePapermillOperator(
            task_id="noteable-papermill-task",
            notebook_path="https://app.noteable.io/f/f78d668e-13f3-49da-84a9-afdece1b1e2a",
            parameters={"foo": "hello world this is running from airflow"},
        )
    ```
1. Now, you should see your DAG listed in the list of DAGs on `http://localhost:8080`
   You can trigger the DAG from the UI or via the CLI. The following CLI command will trigger the DAG and run the task.
   ```bash
   airflow dags test example_noteable_airflow
   ```
