"""Bounded visual sampling for PDF enrichment."""

from __future__ import annotations

import base64
from io import BytesIO
import logging
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from PIL import Image, ImageOps, UnidentifiedImageError
from .source_files import source_input

LOGGER = logging.getLogger(__name__)


def representative_pdf_pages(page_count: int, max_images: int = 4) -> list[int]:
    """Select cover, interior, and final pages with even document coverage."""
    page_count = max(0, int(page_count or 0))
    maximum = max(1, int(max_images or 1))
    if not page_count:
        return []
    if page_count <= maximum:
        return list(range(1, page_count + 1))
    if maximum == 1:
        return [1]
    return sorted({1 + round(index * (page_count - 1) / (maximum - 1)) for index in range(maximum)})


def _render_pdf_page(
    source: str,
    page: int,
    prefix: str,
    dpi: int,
    max_dimension: int,
    timeout_seconds: int,
) -> bytes | None:
    """Render one PDF page to JPEG bytes, or None when it cannot be rendered."""
    try:
        result = subprocess.run(
            [
                "pdftoppm",
                "-f",
                str(page),
                "-l",
                str(page),
                "-singlefile",
                "-jpeg",
                "-jpegopt",
                "quality=78,optimize=y",
                "-r",
                str(max(72, dpi)),
                "-scale-to",
                str(max(600, max_dimension)),
                source,
                prefix,
            ],
            capture_output=True,
            check=False,
            timeout=max(1, timeout_seconds),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        LOGGER.warning("Could not render PDF page %s for vision: %s", page, exc)
        return None
    image_path = f"{prefix}.jpg"
    if result.returncode != 0 or not os.path.exists(image_path):
        detail = result.stderr.decode("utf-8", errors="replace")[:240]
        LOGGER.warning("pdftoppm failed for page %s: %s", page, detail)
        return None
    return Path(image_path).read_bytes()


def render_pdf_pages(
    data: bytes,
    page_numbers: list[int],
    *,
    dpi: int = 120,
    max_dimension: int = 1600,
    max_total_bytes: int = 12 * 1024 * 1024,
    timeout_seconds: int = 45,
) -> list[dict[str, Any]]:
    """Render selected PDF pages as bounded JPEG/base64 payloads for Ollama."""
    if not data or not page_numbers:
        return []
    rendered: list[dict[str, Any]] = []
    used_bytes = 0
    with tempfile.TemporaryDirectory(prefix="tree-eclass-vision-") as directory:
        source = str(data)
        if isinstance(data, bytes):
            source = os.path.join(directory, "source.pdf")
            Path(source).write_bytes(data)
        for page in page_numbers:
            prefix = os.path.join(directory, f"page-{page}")
            image = _render_pdf_page(
                source,
                page,
                prefix,
                dpi,
                max_dimension,
                timeout_seconds,
            )
            if image is None:
                raise RuntimeError(f"PDF page {page} could not be rendered")
            if not image or used_bytes + len(image) > max_total_bytes:
                LOGGER.warning("Vision image budget reached before PDF page %s", page)
                raise RuntimeError("Vision image budget exceeded")
            Path(f"{prefix}.jpg").unlink(missing_ok=True)
            used_bytes += len(image)
            rendered.append(
                {
                    "page": page,
                    "mime_type": "image/jpeg",
                    "byte_count": len(image),
                    "base64": base64.b64encode(image).decode("ascii"),
                }
            )
    return rendered


def _validate_image(source, max_pixels: int) -> bool:
    """Return True when the image format and dimensions are acceptable."""
    image_format = str(source.format or "").upper()
    width, height = (int(value) for value in source.size)
    if image_format not in {"JPEG", "PNG"}:
        LOGGER.warning(
            "Could not render standalone image: unsupported format %s",
            image_format or "unknown",
        )
        return False
    if width < 1 or height < 1 or width * height > max(1, max_pixels):
        LOGGER.warning(
            "Could not render standalone image: unsafe dimensions %sx%s",
            width,
            height,
        )
        return False
    source.verify()
    return True


def _flatten_for_jpeg(image) -> Any:
    """Flatten transparency onto white and normalize to RGB."""
    if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
        rgba = image.convert("RGBA")
        flattened = Image.new("RGB", rgba.size, "white")
        flattened.paste(rgba, mask=rgba.getchannel("A"))
        return flattened
    if image.mode != "RGB":
        return image.convert("RGB")
    return image


def render_image_page(
    data: bytes,
    *,
    max_dimension: int = 1600,
    max_total_bytes: int = 12 * 1024 * 1024,
    max_pixels: int = 80_000_000,
) -> list[dict[str, Any]]:
    """Validate and normalize one JPEG/PNG into a bounded vision payload."""
    if not data:
        return []
    try:
        with Image.open(source_input(data)) as source:
            if not _validate_image(source, max_pixels):
                return []

        # Reopen after verify(), normalize EXIF orientation, then downsample
        # before encoding. This bounds the provider payload and strips metadata.
        with Image.open(source_input(data)) as source:
            image = ImageOps.exif_transpose(source)
            image.thumbnail(
                (max(1, int(max_dimension)), max(1, int(max_dimension))),
                Image.Resampling.LANCZOS,
            )
            image = _flatten_for_jpeg(image)
            output = BytesIO()
            image.save(output, format="JPEG", quality=78, optimize=True)
            encoded = output.getvalue()
    except (
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        SyntaxError,
        ValueError,
    ) as exc:
        LOGGER.warning("Could not render standalone image for vision: %s", exc)
        return []
    if not encoded or len(encoded) > max(1, int(max_total_bytes)):
        LOGGER.warning(
            "Standalone vision image exceeds the payload budget (%s bytes)",
            len(encoded),
        )
        return []
    return [
        {
            "page": 1,
            "mime_type": "image/jpeg",
            "byte_count": len(encoded),
            "base64": base64.b64encode(encoded).decode("ascii"),
        }
    ]
