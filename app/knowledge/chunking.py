"""Locator-preserving, bounded text chunking."""

import re

from .models import Chunk, ExtractedUnit
from .normalization import chunk_id, normalize_text, search_normalize, text_hash


def _pieces(text: str, target_chars: int, overlap_chars: int) -> list[str]:
    text = re.sub(r"[ \t]+", " ", normalize_text(text)).strip()
    if not text:
        return []
    if len(text) <= target_chars:
        return [text]
    paragraphs = re.split(r"\n\s*\n", text)
    result: list[str] = []
    current = ""
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        while len(paragraph) > target_chars:
            if current:
                result.append(current)
                current = ""
            cut = paragraph.rfind(" ", 0, target_chars)
            cut = cut if cut > target_chars // 2 else target_chars
            result.append(paragraph[:cut].strip())
            paragraph = paragraph[max(0, cut - overlap_chars):].strip()
        candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
        if len(candidate) > target_chars and current:
            result.append(current)
            tail = current[-overlap_chars:] if overlap_chars else ""
            current = f"{tail}\n\n{paragraph}".strip()
        else:
            current = candidate
    if current:
        result.append(current)
    return result


def chunk_units(document: str, source_hash: str, units: list[ExtractedUnit],
                target_chars: int = 4_000, overlap_chars: int = 300) -> list[Chunk]:
    chunks: list[Chunk] = []
    for unit in units:
        for text in _pieces(unit.text, target_chars, overlap_chars):
            ordinal = len(chunks)
            chunks.append(Chunk(
                id=chunk_id(document, source_hash, unit.locator_type, unit.locator_start,
                            unit.locator_end, ordinal),
                document_id=document,
                ordinal=ordinal,
                locator_type=unit.locator_type,
                locator_start=unit.locator_start,
                locator_end=unit.locator_end,
                heading=unit.heading,
                text=text,
                normalized_text=search_normalize(text),
                content_hash=text_hash(text),
                metadata=dict(unit.metadata),
            ))
    return chunks
