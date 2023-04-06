# papermill-origami
    A papermill engine for running Noteable notebooks

<p align="center">
<a href="https://github.com/noteable-io/papermill-origami/actions/workflows/ci.yaml">
    <img src="https://github.com/noteable-io/papermill-origami/actions/workflows/ci.yaml/badge.svg" alt="CI" />
</a>
<img alt="PyPI - License" src="https://img.shields.io/pypi/l/papermill-origami" />
<img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/papermill-origami" />
<img alt="PyPI" src="https://img.shields.io/pypi/v/papermill-origami">
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

---------

[Install](#installation) | [Getting Started](#getting-started) | [Documentation](https://papermill-origami.readthedocs.io/) | [License](./LICENSE) | [Code of Conduct](./CODE_OF_CONDUCT.md) | [Contributing](./CONTRIBUTING.md)

<!-- --8<-- [start:intro] -->
## Intro to Papermill-Origami

Papermill-Origami is the bridge library between the [Origami Noteable SDK](https://noteable-origami.readthedocs.io/en/latest/) and [Papermill](https://papermill.readthedocs.io/en/latest/). It build a papermill engine that can talk to Noteable APIs to run Notebooks. 
<!-- --8<-- [end:intro] -->

<!-- --8<-- [start:requirements] -->
## Requirements

Python 3.8+
<!-- --8<-- [end:requirements] -->

<!-- --8<-- [start:install] -->
## Installation

### Poetry

```shell
poetry add papermill-origami
```

### Pip
```shell
pip install papermill-origami
```
<!-- --8<-- [end:install] -->

<!-- --8<-- [start:start] -->
## Getting Started

### API Token

Get your access token from your User Settings -> API Tokens

or alternatively you can generate a post request to generate a new token

```
curl -X 'POST' \
  'https://app.noteable.io/gate/api/v1/tokens' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "ttl": 31536000,
  "name": "my_token"
}'
```

### Engine Registration

The `noteable` engine keyword will use the following environment variables by default:

```bash
NOTEABLE_DOMAIN = app.noteable.io
NOTEABLE_TOKEN = MY_TOKEN_VALUE_HERE
```

Then the engine is enabled by running papermill as normal. But now you have access to
the `noteable://` scheme as well as the ability to tell papermill to use Noteable as
the execution location for your notebook.

```python
import papermill as pm

file_id = '...'

pm.execute_notebook(
    f'noteable://{file_id}',
    None, # Set no particular output notebook, but a log of the resulting exeuction link still prints
    # This turns on the Noteable API interface
    engine_name='noteable', # exclude this kwarg to run the Notebook locally
)
```

#### Advanced Setup

For more advanced control or reuse of a NoteableClient SDK object you can use
the async await pattern around a client constructor. This reuses the connection
throughout the life cycle of the context block.

```python
import papermill as pm
from papermill.iorw import papermill_io
from papermill_origami import ClientConfig, NoteableClient, NoteableHandler 


domain = 'app.noteable.io'
token = MY_TOKEN_VALUE_HERE
file_id = '...'

async with NoteableClient(token, config=ClientConfig(domain=domain)) as client:
    file = await client.get_notebook(file_id)
    papermill_io.register("noteable://", NoteableHandler(client))
    pm.execute_notebook(
        f'noteable://{file_id}',
        None,
        engine_name='noteable',
        # Noteable-specific kwargs
        file=file,
        client=client,
    )
```
<!-- --8<-- [end:start] -->

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

-------

<p align="center">Open sourced with ❤️ by <a href="https://noteable.io">Noteable</a> for the community.</p>

<img href="https://pages.noteable.io/private-beta-access" src="https://assets.noteable.io/github/2022-07-29/noteable.png" alt="Boost Data Collaboration with Notebooks">
