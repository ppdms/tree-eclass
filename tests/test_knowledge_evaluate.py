import unittest

from app.knowledge.evaluate import evaluate_rows, load_qrels


class FakeStore:
    def get_document_by_path(self, course_id, source_path):
        return {"id": f"doc-{course_id}-{source_path}"}


class FakeService:
    store = FakeStore()

    def search(self, request):
        return {"results": [{"document_id": "doc-1-/Course/lecture.pdf"}]}


class KnowledgeEvaluationTests(unittest.TestCase):
    def test_evaluation_resolves_source_paths_and_reports_metrics(self):
        result = evaluate_rows(FakeService(), [{
            "query": "dynamic programming",
            "relevant_source_paths": [{"course_id": 1, "source_path": "/Course/lecture.pdf"}],
        }], k=5)
        self.assertEqual(result["queries"], 1)
        self.assertEqual(result["recall_at_k"], 1.0)
        self.assertEqual(result["mrr_at_k"], 1.0)

    def test_qrels_loader_skips_comments_and_blank_lines(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qrels.jsonl"
            path.write_text("# comment\n\n{\"query\": \"x\", \"relevant_document_ids\": [\"d\"]}\n", encoding="utf-8")
            self.assertEqual(len(load_qrels(path)), 1)


if __name__ == "__main__":
    unittest.main()
