from dataclasses import replace
import os
import tempfile
import unittest

from app.knowledge.chunking import chunk_units
from app.knowledge.config import KnowledgeConfig
from app.knowledge.models import (ExtractedUnit, Locator, ReadRequest, SearchRequest,
                                  SourceMetadata)
from app.knowledge.normalization import document_id
from app.knowledge.service import KnowledgeService
from app.knowledge.store import KnowledgeStore
from app.services.persistence import DatabaseManager


class KnowledgeServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        source_db = os.path.join(self.temp.name, "eclass.db")
        knowledge_db = os.path.join(self.temp.name, "knowledge.db")
        self.db = DatabaseManager(source_db)
        self.db.save_course(1, "Visible", "/Visible")
        self.db.save_course(2, "Hidden", "/Hidden")
        self.db.set_course_hidden(2, True)
        self.store = KnowledgeStore(knowledge_db)
        config = replace(KnowledgeConfig.from_env(), db_file=knowledge_db,
                         source_db_file=source_db, read_max_chars=25, search_limit_max=3)
        self.service = KnowledgeService(self.store, config)
        for course_id, name in ((1, "Visible"), (2, "Hidden")):
            source = SourceMetadata(course_id, name, None, f"/{name}/lecture.txt", None,
                                    "lecture.txt", f"hash-{course_id}")
            opaque = document_id(course_id, source.source_path)
            chunks = chunk_units(opaque, source.source_hash,
                                 [ExtractedUnit("line", "1", "retrievable course evidence long enough")])
            self.store.replace_document(source, "text", chunks, "test")
        self.visible_id = document_id(1, "/Visible/lecture.txt")
        self.hidden_id = document_id(2, "/Hidden/lecture.txt")

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_search_filters_hidden_courses_and_caps_limit(self):
        result = self.service.search(SearchRequest("retrievable", limit=99))
        self.assertEqual([item["course_id"] for item in result["results"]], [1])
        with self.assertRaises(ValueError):
            self.service.search(SearchRequest("retrievable", course_ids=[2]))

    def test_read_uses_opaque_id_and_character_cap(self):
        result = self.service.read(ReadRequest(
            self.visible_id, [Locator("line", "1")], include_neighbors=False, max_characters=1000
        ))
        self.assertEqual(result["characters"], 25)
        self.assertTrue(result["truncated"])
        with self.assertRaises(ValueError):
            self.service.read(ReadRequest(self.hidden_id))
        with self.assertRaises(ValueError):
            self.service.read(ReadRequest("/Visible/lecture.txt"))

    def test_admin_overview_reports_embedding_coverage_without_hidden_courses(self):
        overview = self.service.admin_overview()
        self.assertEqual([course["course_id"] for course in overview["courses"]], [1])
        self.assertEqual(overview["embedding"]["chunks"], 1)
        self.assertEqual(overview["embedding"]["embedded_chunks"], 1)
        self.assertEqual({document["course_id"] for document in overview["documents"]}, {1})


if __name__ == "__main__":
    unittest.main()
