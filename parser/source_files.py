"""Path/byte adapters at public archive and rendering boundaries."""

from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
import tempfile


def source_input(source):
    return BytesIO(source) if isinstance(source, bytes) else str(source)


def source_prefix(source, size=8192):
    if isinstance(source, bytes):
        return source[:size]
    with open(source, "rb") as handle:
        return handle.read(size)


def source_size(source):
    return len(source) if isinstance(source, bytes) else Path(source).stat().st_size


@contextmanager
def source_path(source, suffix=""):
    if not isinstance(source, bytes):
        yield str(source)
        return
    with tempfile.NamedTemporaryFile(suffix=suffix) as handle:
        handle.write(source)
        handle.flush()
        yield handle.name
