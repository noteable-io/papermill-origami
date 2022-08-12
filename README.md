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

[Install](#installation) | [Getting Started](#getting-started) | [License](./LICENSE) | [Code of Conduct](./CODE_OF_CONDUCT.md) | [Contributing](./CONTRIBUTING.md)

## Requirements

Python 3.8+

## Installation

### Poetry

```shell
poetry add papermill-origami
```


### Pip
```shell
pip install papermill-origami
```

## Getting Started

Get your access token from https://app.noteable.world/api/token

```python
import papermill as pm
from papermill_origami import NoteableClient, ClientConfig

domain = 'app.noteable.world'
token = 'ey...'
file_id = '...'

async with NoteableClient(token, config=ClientConfig(domain=domain)) as client:
    file = await client.get_notebook(file_id)
    pm.execute_notebook(
        f'noteable://{file_id}',
        None,
        engine_name='noteable', # exclude this kwarg to run the Notebook locally
        # Noteable-specific kwargs
        file=file,
        client=client,
    )
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

-------

<p align="center">Open sourced with ❤️ by <a href="https://noteable.io">Noteable</a> for the community.</p>

<img href="https://pages.noteable.io/private-beta-access" src="https://assets.noteable.io/github/2022-07-29/noteable.png" alt="Boost Data Collaboration with Notebooks">
