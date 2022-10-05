import pytest

from papermill_origami.iorw import NoteableHandler


@pytest.fixture
def noteable_client(mocker, file):
    return mocker.patch('papermill_origami.iorw.NoteableClient', return_value=mocker.AsyncMock())


@pytest.fixture
def mock_run_sync(mocker, file):
    run_sync = mocker.patch('papermill_origami.iorw.run_sync')
    run_sync.side_effect = [mocker.Mock(return_value=None), mocker.Mock(return_value=file)]


def test_ensure_client_on_noteable_handler_read_function(noteable_client, mock_run_sync):
    """Ensure that a client is created when `read` is called on the NoteableHandler as a function"""
    NoteableHandler.read('test')

    # Assert that a client was created
    noteable_client.assert_called_once()


def test_ensure_client_on_noteable_handler_read_method(noteable_client, mock_run_sync):
    """Ensure that a client is created when `read` is called on the NoteableHandler as a method"""
    NoteableHandler(noteable_client).read('test')

    # Assert that a client was not created because one was passed in
    noteable_client.assert_not_called()
