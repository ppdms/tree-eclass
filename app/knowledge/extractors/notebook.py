import json
from pathlib import Path

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import ExtractionError, ExtractionLimits, enforce_limits


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    try:
        notebook = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise ExtractionError(f"could not open notebook: {exc}") from exc
    units = []
    for number, cell in enumerate(notebook.get("cells", []), 1):
        cell_type = cell.get("cell_type", "unknown")
        parts = ["".join(cell.get("source", []))]
        if cell_type == "code":
            for output in cell.get("outputs", []):
                value = output.get("text")
                if value:
                    parts.append("Output:\n" + ("".join(value) if isinstance(value, list) else str(value)))
        units.append(ExtractedUnit("cell", str(number), "\n".join(parts), heading=cell_type,
                                   metadata={"cell_type": cell_type}))
    return enforce_limits(ExtractedDocument(source.display_name, "notebook", units), limits)
