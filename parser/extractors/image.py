"""Bounded extraction for standalone JPEG and PNG study material."""

from __future__ import annotations

import subprocess

from ..models import ExtractedDocument, ExtractedUnit, SourceMetadata
from .base import (
    ExtractionError,
    ExtractionLimitError,
    ExtractionLimits,
    enforce_limits,
)


SUPPORTED_FORMATS = {"JPEG": "image/jpeg", "PNG": "image/png"}


def _inspect(path: str, limits: ExtractionLimits) -> tuple[str, int, int]:
    try:
        from PIL import Image, UnidentifiedImageError
    except ImportError as exc:  # pragma: no cover - dependency is explicit
        raise ExtractionError("Pillow is required for image validation", reason="parser_failure") from exc

    try:
        with Image.open(path) as image:
            image_format = str(image.format or "").upper()
            width, height = (int(value) for value in image.size)
            if image_format not in SUPPORTED_FORMATS:
                raise ExtractionError(
                    f"unsupported image format: {image_format or 'unknown'}",
                    reason="invalid_image",
                )
            if width < 1 or height < 1:
                raise ExtractionError("image dimensions are invalid", reason="invalid_image")
            if width * height > limits.image_max_pixels:
                raise ExtractionLimitError(
                    f"image pixel limit exceeded ({width * height} > {limits.image_max_pixels})",
                    reason="maximum_image_pixels",
                )
            # verify() decodes enough of the file structure to reject truncated
            # and malformed payloads without retaining the decompressed raster.
            image.verify()
    except ExtractionError:
        raise
    except (
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        SyntaxError,
        ValueError,
    ) as exc:
        raise ExtractionError(f"could not validate image: {exc}", reason="invalid_image") from exc
    return image_format, width, height


def _ocr(path: str, limits: ExtractionLimits) -> tuple[str, dict[str, str]]:
    try:
        recognized = subprocess.run(
            [
                "tesseract",
                path,
                "stdout",
                "-l",
                limits.ocr_languages,
                "--psm",
                "3",
            ],
            capture_output=True,
            timeout=limits.ocr_page_timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        return "", {"error": f"missing OCR tool: {exc.filename}"}
    except (subprocess.TimeoutExpired, OSError):
        return "", {"error": "OCR image timeout"}
    if recognized.returncode != 0:
        return "", {"error": "tesseract failed"}
    return recognized.stdout.decode("utf-8", errors="replace"), {
        "engine": "tesseract",
        "languages": limits.ocr_languages,
    }


def extract(path: str, source: SourceMetadata, limits: ExtractionLimits) -> ExtractedDocument:
    image_format, width, height = _inspect(path, limits)
    text = ""
    metadata: dict[str, object] = {
        "provenance": "standalone_image",
        "format": image_format,
        "mime_type": SUPPORTED_FORMATS[image_format],
        "width": width,
        "height": height,
    }
    warnings: list[str] = []
    if limits.ocr_enabled:
        text, ocr_metadata = _ocr(path, limits)
        if text.strip():
            metadata["provenance"] = "tesseract_ocr"
            metadata["ocr"] = ocr_metadata
        elif ocr_metadata.get("error"):
            warnings.append(f"image OCR unavailable ({ocr_metadata['error']})")
    if not text.strip():
        warnings.append("Standalone image has no extracted text; page vision is required")
    unit = ExtractedUnit("page", "1", text, metadata=metadata)
    return enforce_limits(
        ExtractedDocument(
            source.display_name,
            "image",
            [unit],
            {
                "page_count": 1,
                "format": image_format,
                "mime_type": SUPPORTED_FORMATS[image_format],
                "width": width,
                "height": height,
            },
            warnings,
        ),
        limits,
    )
