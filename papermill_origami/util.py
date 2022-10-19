def removeprefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix) :]
    else:
        return s[:]


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
