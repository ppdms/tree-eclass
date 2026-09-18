from pathlib import Path

from bs4 import BeautifulSoup

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import ExtractionLimits, enforce_limits
from .text import _decode


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    raw, encoding = _decode(Path(path).read_bytes())
    soup = BeautifulSoup(raw, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav"]):
        tag.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else source.display_name
    units: list[ExtractedUnit] = []
    heading = None
    buffer: list[str] = []
    ordinal = 1
    for element in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "pre", "table"]):
        value = element.get_text(" ", strip=True)
        if not value:
            continue
        if element.name.startswith("h"):
            if buffer:
                units.append(ExtractedUnit("section", str(ordinal), "\n".join(buffer), heading=heading))
                ordinal += 1
                buffer = []
            heading = value
        else:
            buffer.append(value)
    if buffer or heading:
        units.append(ExtractedUnit("section", str(ordinal), "\n".join(buffer) or heading or "", heading=heading))
    return enforce_limits(ExtractedDocument(title, "html", units, {"encoding": encoding}), limits)
