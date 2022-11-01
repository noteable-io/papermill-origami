from functools import lru_cache
from urllib.parse import urlparse


def flatten_dict(d, parent_key_tuple: tuple = ()):
    """Flattens a dictionary into a dict mapped from tuples of keys to values.

    Usage:
    >>> flatten_dict({"a": {"b": 1, "c": 2}})
    {("a", "b"): 1, ("a", "c"): 2}
    >>> flatten_dict({"tags": ["parameters"], "jupyter": {"source_hidden": True}})
    {("tags",): ["parameters"], ("jupyter", "source_hidden"): True}
    """
    items = {}
    for k, v in d.items():
        new_key_tuple = parent_key_tuple + (k,) if parent_key_tuple else (k,)
        if isinstance(v, dict) and v:
            items.update(flatten_dict(v, new_key_tuple))
        else:
            items[new_key_tuple] = v
    return items


@lru_cache()
def parse_noteable_file_id(path):
    """Parses a noteable file id from a noteable:// scheme path or a Noteable file HTTPs URL

    Examples:
    >>> parse_noteable_file_id('noteable://f78d668e-13f3-49da-84a9-afdece1b1e2a')
    'f78d668e-13f3-49da-84a9-afdece1b1e2a'
    >>> parse_noteable_file_id('https://app.noteable.io/f/f78d668e-13f3-49da-84a9-afdece1b1e2a')
    'f78d668e-13f3-49da-84a9-afdece1b1e2a'
    """
    url = urlparse(path)
    if url.scheme == "noteable":
        return url.netloc
    elif url.scheme == "https":
        paths = url.path.strip('/').split('/')
        return paths[1]
