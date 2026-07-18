import json
import os
import unittest
from unittest.mock import MagicMock, patch

from app.knowledge.config import KnowledgeConfig
from app.knowledge.embeddings import (DEFAULT_MODEL, LOCAL_MODEL_NAME, EmbeddingProvider,
                                      embed_text)


class KnowledgeEmbeddingProviderTests(unittest.TestCase):
    def test_hosted_model_is_default_and_missing_key_uses_local_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            config = KnowledgeConfig.from_env()
            provider = EmbeddingProvider.from_env()
            result = provider.embed_texts(["εξέταση αλγορίθμων"])

        self.assertEqual(config.embedding_backend, "openrouter")
        self.assertEqual(config.embedding_model, DEFAULT_MODEL)
        self.assertEqual(provider.configured_model, DEFAULT_MODEL)
        self.assertEqual(result.model, LOCAL_MODEL_NAME)
        self.assertTrue(result.used_fallback)
        self.assertEqual(result.vectors[0], embed_text("εξέταση αλγορίθμων"))

    def test_hosted_response_is_batched_and_model_is_recorded(self):
        response = MagicMock()
        response.read.return_value = json.dumps({
            "data": [
                {"index": 1, "embedding": [0.3, 0.4]},
                {"index": 0, "embedding": [0.1, 0.2]},
            ],
        }).encode("utf-8")
        response.__enter__.return_value = response

        provider = EmbeddingProvider(
            backend="openrouter", model=DEFAULT_MODEL, api_key="test-key", batch_size=2,
            base_url="https://openrouter.invalid/api/v1",
        )
        with patch("app.knowledge.embeddings.urlopen", return_value=response) as request:
            result = provider.embed_texts(["first", "second"])

        self.assertEqual(result.model, DEFAULT_MODEL)
        self.assertFalse(result.used_fallback)
        self.assertEqual(result.vectors, [[0.1, 0.2], [0.3, 0.4]])
        payload = json.loads(request.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(payload["model"], DEFAULT_MODEL)
        self.assertEqual(payload["input"], ["first", "second"])

    def test_hosted_failure_uses_local_vectors(self):
        provider = EmbeddingProvider(
            backend="openrouter", model=DEFAULT_MODEL, api_key="test-key",
        )
        with patch("app.knowledge.embeddings.urlopen", side_effect=OSError("offline")):
            result = provider.embed_texts(["offline query"])

        self.assertEqual(result.model, LOCAL_MODEL_NAME)
        self.assertTrue(result.used_fallback)
        self.assertIn("offline", result.error)


if __name__ == "__main__":
    unittest.main()
