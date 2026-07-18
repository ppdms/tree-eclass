from dataclasses import replace
import hashlib
import os
import tempfile
import unittest
from unittest.mock import patch

from app.knowledge.config import KnowledgeConfig
from app.knowledge.models import SourceMetadata
from app.knowledge.store import KnowledgeStore
from app.knowledge.worker import KnowledgeWorker


class FakeDatabase:
    def __init__(self, _path):
        pass

    def close(self):
        pass


class FakeUploader:
    def __init__(self, data):
        self.data = data

    def download_file(self, _path):
        return self.data


class FakeUploaderWithMetadata(FakeUploader):
    def download_file_with_metadata(self, _path):
        return self.data, {"content-type": "text/plain; charset=utf-8"}


class KnowledgeWorkerTests(unittest.TestCase):
    def test_queue_to_extract_to_search(self):
        data = "Greek Ελληνικά worker evidence".encode()
        digest = hashlib.md5(data).hexdigest()
        source = SourceMetadata(1, "Course", "C", "/Course/notes.txt", None,
                                "notes.txt", digest, "text/plain")
        with tempfile.TemporaryDirectory() as directory:
            store = KnowledgeStore(os.path.join(directory, "knowledge.db"))
            config = replace(KnowledgeConfig.from_env(), db_file=store.db_file,
                             source_db_file=os.path.join(directory, "source.db"))
            store.enqueue(1, source.source_path, digest, "upsert")
            worker = KnowledgeWorker(config, store, FakeUploader(data))
            with patch("app.knowledge.worker.DatabaseManager", FakeDatabase), \
                 patch("app.knowledge.worker._current_source", return_value=source):
                self.assertTrue(worker.run_once())
            results = store.search("evidence", {"course_ids": [1]}, 5)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["locator_type"], "line")
            self.assertEqual(store.job_counts()["completed"], 1)

    def test_response_mime_is_retained_for_content_detection(self):
        data = b"header-detected MIME evidence"
        digest = hashlib.md5(data).hexdigest()
        source = SourceMetadata(1, "Course", "C", "/Course/notes.bin", None,
                                "notes.bin", digest, "application/octet-stream")
        with tempfile.TemporaryDirectory() as directory:
            store = KnowledgeStore(os.path.join(directory, "knowledge.db"))
            config = replace(KnowledgeConfig.from_env(), db_file=store.db_file,
                             source_db_file=os.path.join(directory, "source.db"))
            store.enqueue(1, source.source_path, digest, "upsert")
            worker = KnowledgeWorker(config, store, FakeUploaderWithMetadata(data))
            with patch("app.knowledge.worker.DatabaseManager", FakeDatabase), \
                 patch("app.knowledge.worker._current_source", return_value=source):
                self.assertTrue(worker.run_once())
            document = store.get_document_by_path(1, source.source_path)
            self.assertEqual(document["mime_type"], "text/plain")
            self.assertEqual(document["status"], "ready")

    def test_html_uses_source_path_when_display_name_is_a_title(self):
        data = b"<html><title>Regression</title><h1>Evidence</h1><p>Grade formula</p></html>"
        digest = hashlib.md5(data).hexdigest()
        source = SourceMetadata(1, "Course", "C", "/Course/regression.slides.html", None,
                                "Regression", digest, "text/plain")
        with tempfile.TemporaryDirectory() as directory:
            store = KnowledgeStore(os.path.join(directory, "knowledge.db"))
            config = replace(KnowledgeConfig.from_env(), db_file=store.db_file,
                             source_db_file=os.path.join(directory, "source.db"))
            store.enqueue(1, source.source_path, digest, "upsert")
            worker = KnowledgeWorker(config, store, FakeUploaderWithMetadata(data))
            with patch("app.knowledge.worker.DatabaseManager", FakeDatabase), \
                 patch("app.knowledge.worker._current_source", return_value=source):
                self.assertTrue(worker.run_once())
            document = store.get_document_by_path(1, source.source_path)
            self.assertEqual(document["status"], "ready")
            self.assertEqual(document["document_kind"], "html")

    def test_unsupported_content_settles_job_without_failure(self):
        data = b"not an extractable image"
        digest = hashlib.md5(data).hexdigest()
        source = SourceMetadata(1, "Course", "C", "/Course/figure.png", None,
                                "figure.png", digest, "image/png")
        with tempfile.TemporaryDirectory() as directory:
            store = KnowledgeStore(os.path.join(directory, "knowledge.db"))
            config = replace(KnowledgeConfig.from_env(), db_file=store.db_file,
                             source_db_file=os.path.join(directory, "source.db"))
            store.enqueue(1, source.source_path, digest, "upsert")
            worker = KnowledgeWorker(config, store, FakeUploader(data))
            with patch("app.knowledge.worker.DatabaseManager", FakeDatabase), \
                 patch("app.knowledge.worker._current_source", return_value=source):
                self.assertTrue(worker.run_once())
            document = store.get_document_by_path(1, source.source_path)
            self.assertEqual(document["status"], "unsupported")
            self.assertEqual(store.job_counts().get("completed"), 1)
            self.assertEqual(store.job_counts().get("failed", 0), 0)


if __name__ == "__main__":
    unittest.main()
