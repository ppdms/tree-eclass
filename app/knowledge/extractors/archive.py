from pathlib import PurePosixPath
import tarfile
import tempfile
import zipfile

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import ExtractionError, ExtractionLimitError, ExtractionLimits, enforce_limits, source_kind
from .notebook import extract as extract_notebook
from .text import _decode


EXECUTABLE_SUFFIXES = {".exe", ".dll", ".so", ".dylib", ".app", ".com", ".bat", ".cmd"}


def _safe(name: str) -> bool:
    path = PurePosixPath(name.replace("\\", "/"))
    return bool(name) and not path.is_absolute() and ".." not in path.parts and path.suffix.lower() not in EXECUTABLE_SUFFIXES


def _is_metadata_member(name: str) -> bool:
    path = PurePosixPath(name.replace("\\", "/"))
    return "__MACOSX" in path.parts or path.name.startswith("._")


def _members(path: str):
    if zipfile.is_zipfile(path):
        archive = zipfile.ZipFile(path)
        for item in archive.infolist():
            if not item.is_dir():
                yield item.filename, item.file_size, item.compress_size, lambda i=item: archive.read(i)
        archive.close()
        return
    try:
        archive = tarfile.open(path, mode="r:*")
    except tarfile.TarError as exc:
        raise ExtractionError(f"could not open archive: {exc}") from exc
    for item in archive.getmembers():
        if item.isfile():
            yield item.name, item.size, item.size, lambda i=item: archive.extractfile(i).read()  # type: ignore[union-attr]
    archive.close()


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    units, warnings, total = [], [], 0
    for number, (name, size, compressed, read) in enumerate(_members(path), 1):
        if number > limits.archive_max_members:
            raise ExtractionLimitError("archive member-count limit exceeded")
        if _is_metadata_member(name):
            continue
        if not _safe(name):
            warnings.append(f"skipped unsafe archive member: {name[:200]}")
            continue
        if size > limits.archive_max_member_bytes:
            warnings.append(f"skipped oversized archive member: {name[:200]}")
            continue
        total += size
        if total > limits.archive_max_expanded_bytes:
            raise ExtractionLimitError("archive expanded-size limit exceeded")
        if compressed > 0 and size / compressed > limits.archive_max_ratio:
            warnings.append(f"skipped high-ratio archive member: {name[:200]}")
            continue
        member_kind = source_kind(name)
        if member_kind not in {"text", "source", "html", "notebook"}:
            continue
        data = read()
        if member_kind == "notebook":
            # Reuse the notebook extractor, while giving each cell an archive
            # qualified locator so cells from different members remain
            # unambiguous in search results and read_material.
            with tempfile.NamedTemporaryFile(suffix=".ipynb") as member_file:
                member_file.write(data)
                member_file.flush()
                notebook = extract_notebook(member_file.name, source, limits)
            for unit in notebook.units:
                units.append(ExtractedUnit(
                    "archive_member", f"{name}#{unit.locator_start}", unit.text,
                    heading=unit.heading,
                    metadata={"member": name, "cell": unit.locator_start, **unit.metadata},
                ))
            continue
        text, encoding = _decode(data)
        units.append(ExtractedUnit("archive_member", name, text, heading=name,
                                   metadata={"encoding": encoding}))
    return enforce_limits(ExtractedDocument(source.display_name, "archive", units,
                                             {"indexed_members": len(units)}, warnings), limits)
