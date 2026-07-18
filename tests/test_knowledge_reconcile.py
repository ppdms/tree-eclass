import os
import tempfile
import unittest

from app.knowledge.reconcile import KnowledgeReconciler
from app.knowledge.store import KnowledgeStore
from app.services.tree_builder import File, Node


class KnowledgeReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = KnowledgeStore(os.path.join(self.temp.name, "knowledge.db"))
        self.reconciler = KnowledgeReconciler(self.store, os.path.join(self.temp.name, "source.db"))
        self.course = {"id": 1, "name": "Course", "short_name": "C"}

    def tearDown(self):
        self.temp.cleanup()

    def tree(self, source_hash="a"):
        return Node("Course", "https://example.invalid", "/Course", files=[
            File("https://example.invalid/a", "lecture.pdf", md5_hash=source_hash,
                 local_path="/Course/lecture.pdf"),
            File("https://example.invalid/e", "video", md5_hash="redirect",
                 redirect_url="https://video.invalid"),
            File("https://example.invalid/u", "binary.bin", md5_hash="bin",
                 local_path="/Course/binary.bin"),
            File("https://example.invalid/d", "old_diff.pdf", md5_hash="diff",
                 local_path="/Course/.versions/old_diff.pdf"),
        ])

    def test_add_unchanged_change_and_delete(self):
        first = self.reconciler.reconcile_course(1, self.tree(), self.course)
        self.assertEqual(first["enqueued"], 2)
        self.assertEqual(first["external"], 1)
        self.assertEqual(first["unsupported"], 0)
        self.assertEqual(first["pending_detection"], 1)
        pending = self.store.get_document_by_path(1, "/Course/binary.bin")
        self.assertEqual(pending["status"], "pending")
        self.assertEqual(pending["diagnostic_reason"], "content_detection_pending")
        status = self.store.status([1])
        self.assertEqual(status["coverage"][0]["pending_documents"], 2)
        second = self.reconciler.reconcile_course(1, self.tree(), self.course)
        self.assertEqual(second["enqueued"], 0)
        changed = self.reconciler.reconcile_course(1, self.tree("b"), self.course)
        self.assertEqual(changed["enqueued"], 1)
        empty = Node("Course", "https://example.invalid", "/Course")
        deleted = self.reconciler.reconcile_course(1, empty, self.course)
        self.assertEqual(deleted["deleted"], 3)


if __name__ == "__main__":
    unittest.main()
