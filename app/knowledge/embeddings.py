"""Hosted and local embedding providers for hybrid retrieval.

The configured default is OpenRouter's OpenAI-compatible embeddings endpoint
using ``openai/text-embedding-3-small``.  The deterministic hash embedding is
kept as a dependency-free fallback so indexing and search remain available
when no API key is configured or the hosted request fails.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import struct
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .normalization import search_normalize


DEFAULT_BACKEND = "openrouter"
DEFAULT_MODEL = "openai/text-embedding-3-small"
LOCAL_MODEL_NAME = "hash-384-v1"
LOCAL_DIMENSIONS = 384
# Backward-compatible aliases for callers that only know about the local model.
MODEL_NAME = LOCAL_MODEL_NAME
DIMENSIONS = LOCAL_DIMENSIONS
_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)
LOGGER = logging.getLogger(__name__)


def _add(vector: list[float], value: str, weight: float) -> None:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    bucket = int.from_bytes(digest[:4], "big") % LOCAL_DIMENSIONS
    sign = 1.0 if digest[4] & 1 else -1.0
    vector[bucket] += sign * weight


def embed_text(text: str) -> list[float]:
    """Return a normalized fixed-width local embedding."""
    normalized = search_normalize(text)
    vector = [0.0] * LOCAL_DIMENSIONS
    tokens = _TOKEN_RE.findall(normalized)
    for token in tokens:
        _add(vector, f"w:{token}", 1.0)
        padded = f"^{token}$"
        for index in range(max(0, len(padded) - 2)):
            _add(vector, f"c:{padded[index:index + 3]}", 0.35)
    norm = math.sqrt(sum(value * value for value in vector))
    return [value / norm for value in vector] if norm else vector


def pack_vector(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)


def unpack_vector(data: bytes, dimensions: int) -> list[float]:
    return list(struct.unpack(f"<{dimensions}f", data))


def cosine(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


class EmbeddingError(RuntimeError):
    """Raised when a hosted embedding response cannot be used."""


@dataclass(frozen=True)
class EmbeddingBatch:
    model: str
    vectors: list[list[float]]
    used_fallback: bool = False
    error: str | None = None


class EmbeddingProvider:
    """Embed text with the configured hosted provider and local fallback."""

    def __init__(self, backend: str = DEFAULT_BACKEND, model: str = DEFAULT_MODEL,
                 api_key: str | None = None,
                 base_url: str = "https://openrouter.ai/api/v1",
                 timeout_seconds: int = 30, batch_size: int = 32,
                 local_fallback: bool = True):
        self.backend = (backend or "local").strip().lower()
        self.model = model or DEFAULT_MODEL
        self.api_key = api_key.strip() if api_key else None
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(1, timeout_seconds)
        self.batch_size = max(1, batch_size)
        self.local_fallback = local_fallback

    @classmethod
    def from_env(cls) -> "EmbeddingProvider":
        backend = os.getenv("KNOWLEDGE_EMBEDDING_BACKEND", DEFAULT_BACKEND).strip().lower()
        if backend == "local":
            api_key = None
        else:
            api_key = os.getenv("KNOWLEDGE_EMBEDDING_API_KEY")
            if not api_key:
                api_key = os.getenv("OPENAI_API_KEY" if backend == "openai" else "OPENROUTER_API_KEY")
        default_base_url = (
            "https://api.openai.com/v1" if backend == "openai"
            else "https://openrouter.ai/api/v1"
        )
        return cls(
            backend=backend,
            model=os.getenv("KNOWLEDGE_EMBEDDING_MODEL", DEFAULT_MODEL),
            api_key=api_key,
            base_url=os.getenv("KNOWLEDGE_EMBEDDING_BASE_URL", default_base_url),
            timeout_seconds=_env_int("KNOWLEDGE_EMBEDDING_TIMEOUT_SECONDS", 30, 1),
            batch_size=_env_int("KNOWLEDGE_EMBEDDING_BATCH_SIZE", 32, 1),
            local_fallback=_env_bool("KNOWLEDGE_EMBEDDING_LOCAL_FALLBACK", True),
        )

    @classmethod
    def from_config(cls, config: Any) -> "EmbeddingProvider":
        return cls(
            backend=config.embedding_backend,
            model=config.embedding_model,
            api_key=config.embedding_api_key,
            base_url=config.embedding_base_url,
            timeout_seconds=config.embedding_timeout_seconds,
            batch_size=config.embedding_batch_size,
            local_fallback=config.embedding_local_fallback,
        )

    @property
    def fallback_model(self) -> str:
        return LOCAL_MODEL_NAME

    @property
    def configured_model(self) -> str:
        return self.model if self.backend != "local" else LOCAL_MODEL_NAME

    @property
    def can_use_hosted(self) -> bool:
        return self.backend != "local" and bool(self.api_key)

    def embed_texts(self, texts: list[str]) -> EmbeddingBatch:
        if not texts:
            return EmbeddingBatch(self.configured_model, [])
        if self.backend == "local" or not self.api_key:
            return self._local_batch(texts, error="hosted embedding API key is not configured")
        try:
            vectors: list[list[float]] = []
            for start in range(0, len(texts), self.batch_size):
                vectors.extend(self._remote_batch(texts[start:start + self.batch_size]))
            return EmbeddingBatch(self.model, vectors)
        except (EmbeddingError, OSError, TimeoutError, ValueError, TypeError, AttributeError) as exc:
            if not self.local_fallback:
                raise
            message = str(exc)[:300]
            LOGGER.warning("Hosted embeddings unavailable; using local fallback: %s", message)
            return self._local_batch(texts, error=message)

    def _local_batch(self, texts: list[str], error: str | None = None) -> EmbeddingBatch:
        return EmbeddingBatch(
            LOCAL_MODEL_NAME,
            [embed_text(text) for text in texts],
            used_fallback=self.backend != "local",
            error=error,
        )

    def _remote_batch(self, texts: list[str]) -> list[list[float]]:
        endpoint = self.base_url if self.base_url.endswith("/embeddings") else f"{self.base_url}/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.backend == "openrouter":
            site_url = os.getenv("OPENROUTER_SITE_URL")
            app_name = os.getenv("OPENROUTER_APP_NAME", "tree-eclass knowledge")
            if site_url:
                headers["HTTP-Referer"] = site_url
            if app_name:
                headers["X-Title"] = app_name
        request = Request(
            endpoint,
            data=json.dumps({"model": self.model, "input": texts}).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:  # nosec B310
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise EmbeddingError(f"embedding HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            raise EmbeddingError(f"embedding request failed: {exc}") from exc
        if payload.get("error"):
            error = payload["error"]
            detail = error.get("message", error) if isinstance(error, dict) else error
            raise EmbeddingError(f"embedding provider error: {detail}")
        data = payload.get("data")
        if not isinstance(data, list) or len(data) != len(texts):
            raise EmbeddingError("embedding provider returned an invalid vector count")
        try:
            ordered = sorted(data, key=lambda item: item.get("index", 0))
            vectors = [[float(value) for value in item["embedding"]] for item in ordered]
        except (KeyError, TypeError, ValueError) as exc:
            raise EmbeddingError("embedding provider returned invalid vectors") from exc
        if not vectors or not vectors[0] or any(len(vector) != len(vectors[0]) for vector in vectors):
            raise EmbeddingError("embedding provider returned inconsistent vector dimensions")
        return vectors


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default
