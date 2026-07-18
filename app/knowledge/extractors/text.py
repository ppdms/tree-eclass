from pathlib import Path

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import ExtractionLimits, enforce_limits


def _decode(data: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "cp1253", "latin-1"):
        try:
            return data.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace"), "utf-8-replace"


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    text, encoding = _decode(Path(path).read_bytes())
    lines = text.splitlines()
    step = 250
    units = [
        ExtractedUnit("line", str(start + 1), "\n".join(lines[start:start + step]),
                      str(min(start + step, len(lines))), source.display_name if start == 0 else None)
        for start in range(0, len(lines), step)
    ]
    return enforce_limits(ExtractedDocument(source.display_name, "text", units, {"encoding": encoding}), limits)
