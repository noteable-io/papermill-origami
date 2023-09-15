import pytest

from papermill_origami.path_util import parse_noteable_file_id_and_version_number


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
