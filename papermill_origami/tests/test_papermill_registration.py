import pytest
from papermill.engines import papermill_engines
from papermill.translators import papermill_translators

from papermill_origami import NoteableEngine
from papermill_origami.noteable_dagstermill import DagsterTranslator, NoteableDagstermillEngine


@pytest.mark.parametrize(
    "name,expected_class",
    [
        ("noteable", NoteableEngine),
        ("noteable-dagstermill", NoteableDagstermillEngine),
    ],
)
def test_engines_are_registered(name, expected_class):
    assert papermill_engines.get_engine(name) == expected_class


def test_translator_is_registered():
    assert papermill_translators.find_translator("python", "python") == DagsterTranslator
