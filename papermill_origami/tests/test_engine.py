import copy
import logging
import sys
import uuid
from datetime import datetime
from unittest.mock import ANY

import httpx
import nbformat
import pytest
from nbclient.exceptions import CellExecutionError
from orjson import orjson

from papermill_origami.path_util import parse_noteable_file_id_and_version_number


@pytest.mark.parametrize(
    "d, parent_key_tuple, expected",
    [
        ({"a": {"b": 1, "c": 2}}, ("parent",), {("parent", "a", "b"): 1, ("parent", "a", "c"): 2}),
        (
            {"tags": ["parameters"], "jupyter": {"source_hidden": True}},
            ("metadata",),
            {
                (
                    "metadata",
                    "tags",
                ): ["parameters"],
                ("metadata", "jupyter", "source_hidden"): True,
            },
        ),
        ({}, (), {}),
    ],
)
def test_flatten_dict_with_parent_key_tuple(d, parent_key_tuple, expected):
    # avoid circular import due to papermill engine registration
    from papermill_origami.path_util import flatten_dict

    assert flatten_dict(d, parent_key_tuple) == expected


@pytest.mark.parametrize(
    "url, file_id, version_number",
    [
        ("https://app.noteable.io/f/fake_id/my-new-notebook.ipynb", "fake_id", None),
        ("https://app.noteable.io/f/fake_id", "fake_id", None),
        ("https://app.noteable.io/f/fake_id/v/12", "fake_id", 12),
    ],
)
def test_parse_noteable_file_id(url, file_id, version_number):
    assert parse_noteable_file_id_and_version_number(url) == (file_id, version_number)
