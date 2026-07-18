import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app.knowledge.chunking import chunk_units
from app.knowledge.embeddings import DEFAULT_MODEL, LOCAL_MODEL_NAME, EmbeddingProvider
from app.knowledge.models import ExtractedUnit, SourceMetadata
from app.knowledge.normalization import document_id, normalize_path, search_normalize
from app.knowledge.store import KnowledgeStore


class KnowledgeStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = KnowledgeStore(os.path.join(self.temp.name, "knowledge.db"))
        self.source = SourceMetadata(
            course_id=7, course_name="Αλγόριθμοι", course_short_name="ALG",
            source_path="/Μάθημα/Σημειώσεις.pdf", source_url="https://example.invalid/file",
            display_name="Σημειώσεις.pdf", source_hash="hash-one", mime_type="application/pdf",
        )

    def tearDown(self):
        self.temp.cleanup()

    def _replace(self, text, source_hash="hash-one"):
        self.source.source_hash = source_hash
        opaque = document_id(self.source.course_id, self.source.source_path)
        chunks = chunk_units(opaque, source_hash, [ExtractedUnit("page", "1", text)])
        self.store.replace_document(self.source, "pdf", chunks, "test")
        return opaque

    def test_unicode_path_and_accent_insensitive_search(self):
        decomposed = "/Μαθημα/Σημειω\u0301σεις.pdf"
        composed = "/Μαθημα/Σημειώσεις.pdf"
        self.assertEqual(normalize_path(decomposed), normalize_path(composed))
        self.assertEqual(document_id(1, decomposed), document_id(1, composed))
        self.assertEqual(search_normalize("ΆΛΓΟΡΙΘΜΟΣ"), "αλγοριθμοσ")
        self._replace("Ο αλγόριθμος χρησιμοποιεί δυναμικό προγραμματισμό.")
        results = self.store.search("αλγοριθμος", {"course_ids": [7]}, 5)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["locator_start"], "1")

    def test_replace_delete_and_fts_rebuild(self):
        opaque = self._replace("old searchable phrase")
        self.assertEqual(len(self.store.search("old", {"course_ids": [7]}, 5)), 1)
        self._replace("new replacement phrase", "hash-two")
        self.assertEqual(self.store.search("old", {"course_ids": [7]}, 5), [])
        self.assertEqual(len(self.store.search("replacement", {"course_ids": [7]}, 5)), 1)
        self.assertEqual(self.store.rebuild_fts(), 1)
        self.store.mark_deleted(7, self.source.source_path)
        self.assertEqual(self.store.search("replacement", {"course_ids": [7]}, 5), [])
        self.assertIsNone(self.store.get_document(opaque))

    def test_job_deduplication_claim_and_failed_release(self):
        self.assertTrue(self.store.enqueue(7, self.source.source_path, "h", "upsert"))
        self.assertFalse(self.store.enqueue(7, self.source.source_path, "h", "upsert"))
        job = self.store.claim_job()
        self.assertEqual(job["attempts"], 1)
        self.store.finish_job(job["id"], "failed", "safe error")
        self.assertTrue(self.store.enqueue(7, self.source.source_path, "h", "upsert"))
        retried = self.store.claim_job()
        self.assertEqual(retried["id"], job["id"])
        self.assertEqual(retried["attempts"], 2)

    def test_hybrid_search_fuses_lexical_and_local_semantic_scores(self):
        self._replace("αλγόριθμος και δυναμικός προγραμματισμός")
        results = self.store.search("αλγοριθμοι", {"course_ids": [7]}, 5, mode="hybrid")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["retrieval_mode"], "hybrid")
        self.assertIsNotNone(results[0]["semantic_score"])
        self.assertIn("lexical_score", results[0])

    def test_search_prefers_policy_metadata_and_deduplicates_documents(self):
        policy = SourceMetadata(
            course_id=7, course_name="Αλγόριθμοι", course_short_name="ALG",
            source_path="/Μάθημα/00_Course_Intro.pdf", source_url=None,
            display_name="00_Course_Intro.pdf", source_hash="policy-hash", mime_type="application/pdf",
        )
        policy_id = document_id(policy.course_id, policy.source_path)
        self.store.replace_document(
            policy, "pdf", chunk_units(policy_id, policy.source_hash, [
                ExtractedUnit("page", "1", "grading formula evidence"),
                ExtractedUnit("page", "2", "grading formula neighboring evidence"),
            ]), "test",
        )
        notes = SourceMetadata(
            course_id=7, course_name="Αλγόριθμοι", course_short_name="ALG",
            source_path="/Μάθημα/notes.pdf", source_url=None,
            display_name="notes.pdf", source_hash="notes-hash", mime_type="application/pdf",
        )
        notes_id = document_id(notes.course_id, notes.source_path)
        self.store.replace_document(
            notes, "pdf", chunk_units(notes_id, notes.source_hash, [
                ExtractedUnit("page", "1", "grading formula ordinary evidence"),
            ]), "test",
        )
        results = self.store.search("grading formula", {"course_ids": [7]}, 5, mode="lexical")
        self.assertEqual(len({row["document_id"] for row in results}), len(results))
        self.assertEqual(results[0]["document_id"], policy_id)
        self.assertEqual(results[0]["document_priority"], "course_policy")

    def test_hosted_index_keeps_local_vectors_for_search_fallback(self):
        response = MagicMock()
        response.read.return_value = json.dumps({
            "data": [{"index": 0, "embedding": [1.0, 0.0]}],
        }).encode("utf-8")
        response.__enter__.return_value = response
        provider = EmbeddingProvider(
            backend="openrouter", model=DEFAULT_MODEL, api_key="test-key",
        )
        hosted_store = KnowledgeStore(os.path.join(self.temp.name, "hosted.db"), provider)
        chunks = chunk_units(
            document_id(7, self.source.source_path), "hosted-hash",
            [ExtractedUnit("page", "1", "hosted semantic evidence")],
        )
        self.source.source_hash = "hosted-hash"
        with patch("app.knowledge.embeddings.urlopen", return_value=response):
            hosted_store.replace_document(self.source, "pdf", chunks, "test")
        with hosted_store.connection() as conn:
            models = {row[0] for row in conn.execute("SELECT model FROM chunk_embeddings")}
        self.assertEqual(models, {DEFAULT_MODEL, LOCAL_MODEL_NAME})
        coverage = hosted_store.embedding_status([7])
        self.assertEqual(coverage["chunks"], 1)
        self.assertEqual(coverage["embedded_chunks"], 1)

        with patch("app.knowledge.embeddings.urlopen", side_effect=OSError("offline")):
            results = hosted_store.search("semantic evidence", {"course_ids": [7]}, 5, mode="semantic")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["embedding_model"], LOCAL_MODEL_NAME)


if __name__ == "__main__":
    unittest.main()
