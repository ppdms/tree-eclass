"""Bounded visual sampling for PDF enrichment."""

from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any


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
    return sorted({
        1 + round(index * (page_count - 1) / (maximum - 1))
        for index in range(maximum)
    })


def render_pdf_pages(data: bytes, page_numbers: list[int], *, dpi: int = 120,
                     max_dimension: int = 1600, max_total_bytes: int = 12 * 1024 * 1024,
                     timeout_seconds: int = 45) -> list[dict[str, Any]]:
    """Render selected PDF pages as bounded JPEG/base64 payloads for Ollama."""
    if not data or not page_numbers:
        return []
    rendered: list[dict[str, Any]] = []
    used_bytes = 0
    with tempfile.TemporaryDirectory(prefix="tree-eclass-vision-") as directory:
        source = os.path.join(directory, "source.pdf")
        Path(source).write_bytes(data)
        for page in page_numbers:
            prefix = os.path.join(directory, f"page-{page}")
            try:
                result = subprocess.run(
                    [
                        "pdftoppm", "-f", str(page), "-l", str(page), "-singlefile",
                        "-jpeg", "-jpegopt", "quality=78,optimize=y", "-r", str(max(72, dpi)),
                        "-scale-to", str(max(600, max_dimension)), source, prefix,
                    ],
                    capture_output=True,
                    check=False,
                    timeout=max(1, timeout_seconds),
                )
            except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
                LOGGER.warning("Could not render PDF page %s for vision: %s", page, exc)
                continue
            image_path = f"{prefix}.jpg"
            if result.returncode != 0 or not os.path.exists(image_path):
                detail = result.stderr.decode("utf-8", errors="replace")[:240]
                LOGGER.warning("pdftoppm failed for page %s: %s", page, detail)
                continue
            image = Path(image_path).read_bytes()
            if not image or used_bytes + len(image) > max_total_bytes:
                LOGGER.warning("Vision image budget reached before PDF page %s", page)
                break
            used_bytes += len(image)
            rendered.append({
                "page": page,
                "mime_type": "image/jpeg",
                "byte_count": len(image),
                "base64": base64.b64encode(image).decode("ascii"),
            })
    return rendered
