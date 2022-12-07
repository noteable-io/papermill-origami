"""
Module containing Prefect tasks for executing Noteable notebooks.

Based on https://github.com/PrefectHQ/prefect-jupyter/blob/main/prefect_jupyter/notebook.py
"""
from enum import Enum
from typing import Any, Dict, Optional

import nbconvert
import nbformat
import papermill as pm
from prefect import get_run_logger, task


class OutputFormat(Enum):
    """
    Valid output formats of a notebook.
    """

    ASCIIDOC = "asciidoc"
    CUSTOM = "custom"
    HTML = "html"
    LATEX = "latext"
    MARKDOWN = "markdown"
    NOTEBOOK = "notebook"
    JSON = "notebook"
    PDF = "pdf"
    PYTHON = "python"
    RST = "rst"
    SCRIPT = "script"
    WEBPDF = "webpdf"


@task
def execute_noteable_notebook(
    notebook_path: str,
    parameters: Optional[Dict[str, Any]] = None,
    output_format: OutputFormat = OutputFormat.NOTEBOOK,
    **export_kwargs: Dict[str, Any],
):
    """Task for executing a notebook on Noteable."""
    nb: nbformat.NotebookNode = pm.execute_notebook(
        notebook_path,
        "-",
        parameters=parameters,
        engine_name="noteable",
        logger=get_run_logger(),
    )

    exporter = nbconvert.get_exporter(output_format.value)
    body, _ = nbconvert.export(exporter, nb, **export_kwargs)
    return body
