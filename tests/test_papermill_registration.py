import pytest
from papermill.engines import papermill_engines
from papermill.translators import papermill_translators

from papermill_origami.engine import NoteableEngine


@pytest.mark.parametrize(
    "name,expected_class",
    [
        ("noteable", NoteableEngine),
    ],
)
def test_engines_are_registered(name, expected_class):
    assert papermill_engines.get_engine(name) == expected_class
