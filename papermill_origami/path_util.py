from urllib.parse import urlparse


def parse_noteable_file_id_and_version_number(path) -> tuple[str, int]:
    """

    Examples:
    >>> parse_noteable_file_id('https://app.noteable.io/f/f78d668e-13f3-49da-84a9-afdece1b1e2a')
    'f78d668e-13f3-49da-84a9-afdece1b1e2a'
    """
    url = urlparse(path)
    if url.scheme.startswith("http"):
        paths = url.path.strip("/").split("/")
        if paths[0] != "f":
            raise ValueError("Invalid noteable file URL")

        # /f/<file_id>
        # /f/<file_id>/<file path>
        if len(paths) in (2, 3):
            return paths[1], None
        # /f/<file_id>/v/<version_number>
        elif len(paths) == 4 and paths[2] == "v":
            return paths[1], int(paths[3])

    raise ValueError("Invalid noteable file URL")
