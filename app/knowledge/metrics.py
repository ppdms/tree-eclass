"""Deterministic, language-agnostic document metrics.

The complexity score deliberately describes the density of the extracted text,
not whether a student will find the underlying ideas difficult.  Keeping that
distinction explicit makes the number reproducible and avoids presenting an LLM
opinion as objective metadata.
"""

from __future__ import annotations

import math
import re
from typing import Iterable


WORD_RE = re.compile(r"[^\W_]+(?:[-'’][^\W_]+)*", re.UNICODE)
SENTENCE_RE = re.compile(r"[.!?;·…]+(?:\s|$)")
TECHNICAL_SYMBOL_RE = re.compile(r"[{}\[\]()=<>/*+\-^%|]")


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def complexity_label(score: int) -> str:
    if score < 30:
        return "Accessible"
    if score < 55:
        return "Moderate"
    if score < 75:
        return "Dense"
    return "Very dense"


def document_metrics(texts: Iterable[str], source_size_bytes: int | None = None) -> dict[str, int | str | None]:
    """Return stable counts and a bounded text-density score for extracted units."""
    text = "\n".join(value for value in texts if value)
    words = WORD_RE.findall(text)
    word_count = len(words)
    character_count = len(text)
    sentence_count = max(1, len(SENTENCE_RE.findall(text))) if word_count else 0

    if word_count:
        average_word_length = sum(len(word) for word in words) / word_count
        average_sentence_length = word_count / sentence_count
        long_word_ratio = sum(len(word) >= 9 for word in words) / word_count
        symbol_density = len(TECHNICAL_SYMBOL_RE.findall(text)) / max(1, character_count)
        score = round(100 * (
            0.38 * _clamp(average_sentence_length / 32)
            + 0.24 * _clamp((average_word_length - 3.5) / 4.5)
            + 0.23 * _clamp(long_word_ratio / 0.24)
            + 0.15 * _clamp(symbol_density / 0.07)
        ))
    else:
        score = 0

    # 220 wpm is a deliberately conservative reading estimate for mixed
    # English/Greek academic prose.  Slides and source code remain estimates.
    reading_minutes = math.ceil(word_count / 220) if word_count else 0
    return {
        "source_size_bytes": source_size_bytes,
        "character_count": character_count,
        "word_count": word_count,
        "reading_minutes": reading_minutes,
        "complexity_score": score,
        "complexity_label": complexity_label(score),
    }


def merge_chunk_texts(rows: Iterable[dict], overlap_limit: int = 500) -> list[str]:
    """Reconstruct units from stored chunks while removing their exact overlap.

    This is used only for migrating documents indexed before metrics existed.
    Fresh indexing calculates metrics from extractor units directly.
    """
    units: list[str] = []
    current_locator: tuple[str | None, str | None] | None = None
    current = ""
    for row in rows:
        locator = (row.get("locator_type"), row.get("locator_start"))
        piece = row.get("text") or ""
        if locator != current_locator:
            if current:
                units.append(current)
            current_locator = locator
            current = piece
            continue
        maximum = min(overlap_limit, len(current), len(piece))
        overlap = 0
        for length in range(maximum, 19, -1):
            if current[-length:] == piece[:length]:
                overlap = length
                break
        current += piece[overlap:]
    if current:
        units.append(current)
    return units
