To rebuild the site run:

```bash
PYTHONPATH=path/to/papermill-origami/papermill_origami mkdocs build
```

To serve the docs run:

```bash
PYTHONPATH=path/to/papermill-origami/papermill_origami mkdocs serve
```

To verify a file has appropriate commenting to support mkdocs use:

```bash
python -m doctest papermill_origami/path/to/file.py
```

No output means the file has been fully commented and accepted.
