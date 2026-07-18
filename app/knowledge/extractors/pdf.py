import os
import subprocess
import sys
import tempfile

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import ExtractionError, ExtractionLimits, enforce_limits


def _poppler_page(path: str, number: int) -> str:
    """Use Poppler only as a bounded fallback for failed/sparse embedded extraction."""
    import subprocess
    try:
        result = subprocess.run(
            ["pdftotext", "-f", str(number), "-l", str(number), "-layout", path, "-"],
            capture_output=True, timeout=30, check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.decode("utf-8", errors="replace") if result.returncode == 0 else ""


def _pdfminer_page(path: str, number: int) -> str:
    """Use pdfminer.six as a second parser for PDFs pypdf/Poppler handle poorly."""
    script = (
        "from pdfminer.high_level import extract_text; "
        "from pdfminer.layout import LAParams; "
        "import sys; "
        "print(extract_text(sys.argv[1], page_numbers=[int(sys.argv[2])-1], "
        "laparams=LAParams()) or '', end='')"
    )
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, path, str(number)],
            capture_output=True, timeout=30, check=False,
        )
        return result.stdout.decode("utf-8", errors="replace") if result.returncode == 0 else ""
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        # Optional dependency and malformed documents must not prevent the next
        # bounded fallback (OCR) from running.
        return ""


def _ocr_page(path: str, number: int, limits: ExtractionLimits) -> tuple[str, dict[str, str]]:
    """Render one sparse page and OCR it with bounded local tools."""
    with tempfile.TemporaryDirectory(prefix="tree-eclass-ocr-") as directory:
        prefix = os.path.join(directory, "page")
        try:
            rendered = subprocess.run(
                ["pdftoppm", "-f", str(number), "-l", str(number), "-singlefile",
                 "-r", str(limits.ocr_dpi), "-png", path, prefix],
                capture_output=True, timeout=limits.ocr_page_timeout_seconds, check=False,
            )
            if rendered.returncode != 0:
                return "", {"error": "pdftoppm failed"}
            image = f"{prefix}.png"
            if not os.path.exists(image):
                return "", {"error": "pdftoppm produced no image"}
            recognized = subprocess.run(
                ["tesseract", image, "stdout", "-l", limits.ocr_languages, "--psm", "3"],
                capture_output=True, timeout=limits.ocr_page_timeout_seconds, check=False,
            )
            if recognized.returncode != 0:
                return "", {"error": "tesseract failed"}
            return recognized.stdout.decode("utf-8", errors="replace"), {
                "engine": "tesseract",
                "languages": limits.ocr_languages,
                "dpi": str(limits.ocr_dpi),
            }
        except FileNotFoundError as exc:
            return "", {"error": f"missing OCR tool: {exc.filename}"}
        except subprocess.TimeoutExpired:
            return "", {"error": "OCR page timeout"}


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    try:
        from pypdf import PdfReader
        reader = PdfReader(path)
    except Exception as exc:
        raise ExtractionError(f"could not open PDF: {exc}", reason="parser_failure") from exc
    if reader.is_encrypted:
        raise ExtractionError("PDF is encrypted and cannot be extracted", reason="encrypted_pdf")
    units, warnings = [], []
    for number, page in enumerate(reader.pages, 1):
        provenance = "embedded_text"
        try:
            text = page.extract_text() or ""
        except Exception as exc:
            text = ""
            warnings.append(f"page {number}: text extraction failed ({type(exc).__name__})")
        if len(text.strip()) < 40:
            fallback = _poppler_page(path, number)
            if len(fallback.strip()) > len(text.strip()):
                text = fallback
                provenance = "poppler_text"
        if len(text.strip()) < 40:
            fallback = _pdfminer_page(path, number)
            if len(fallback.strip()) > len(text.strip()):
                text = fallback
                provenance = "pdfminer_text"
        if len(text.strip()) < 40:
            if limits.ocr_enabled and number <= limits.ocr_max_pages:
                ocr_text, ocr_metadata = _ocr_page(path, number, limits)
                if len(ocr_text.strip()) > len(text.strip()):
                    text = ocr_text
                    provenance = "tesseract_ocr"
                    units.append(ExtractedUnit(
                        "page", str(number), text,
                        metadata={"provenance": provenance, "ocr": ocr_metadata},
                    ))
                    continue
                if ocr_metadata.get("error"):
                    warnings.append(f"page {number}: OCR unavailable ({ocr_metadata['error']})")
            elif limits.ocr_enabled and number > limits.ocr_max_pages:
                warnings.append(f"page {number}: OCR page limit reached")
            warnings.append(f"page {number}: sparse embedded text; diagrams, equations, or scans may be missing")
        units.append(ExtractedUnit("page", str(number), text, metadata={"provenance": provenance}))
    title = str(reader.metadata.title) if reader.metadata and reader.metadata.title else source.display_name
    return enforce_limits(ExtractedDocument(title, "pdf", units, {"page_count": len(reader.pages)}, warnings), limits)
