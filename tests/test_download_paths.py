import os
import tempfile
import unittest

from app.services.download_paths import course_download_path, normalize_download_base_path
from app.services.persistence import DatabaseManager
from app.services.checker import _relocate_course_tree


class DownloadPathTests(unittest.TestCase):
    def test_course_paths_use_global_base_and_hidden_namespace(self):
        self.assertEqual(
            course_download_path("/University/", "Τεχνολογία Λογισμικού"),
            "/University/Τεχνολογία Λογισμικού/eclass",
        )
        self.assertEqual(
            course_download_path("University", "Τεχνολογία Λογισμικού", hidden=True),
            "/University/.hidden/Τεχνολογία Λογισμικού/eclass",
        )

    def test_database_derives_folder_from_preferences(self):
        handle, path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        try:
            db = DatabaseManager(path)
            db.save_preferences(download_base_path="/School")
            db.save_course(1, "Operating Systems")

            self.assertEqual(
                db.get_course(1)["webdav_folder"],
                "/School/Operating Systems/eclass",
            )
            db.set_course_hidden(1, True)
            self.assertEqual(
                db.get_course(1, include_hidden=True)["webdav_folder"],
                "/School/.hidden/Operating Systems/eclass",
            )
            db.close()
        finally:
            os.unlink(path)

    def test_base_path_is_normalized(self):
        self.assertEqual(normalize_download_base_path("  University//Courses/ "), "/University/Courses")

    def test_relocation_moves_existing_files_and_rewrites_tree_paths(self):
        class File:
            def __init__(self, local_path):
                self.local_path = local_path

        class Node:
            def __init__(self, local_path, children=None, files=None):
                self.local_path = local_path
                self.children = children or []
                self.files = files or []

        class Uploader:
            def __init__(self):
                self.moves = []

            def move_file(self, source, destination):
                self.moves.append((source, destination))
                return True

        root = Node("/University/Algorithms/eclass", [
            Node("/University/Algorithms/eclass/Lectures", files=[
                File("/University/Algorithms/eclass/Lectures/week-1.pdf")
            ])
        ])
        uploader = Uploader()

        _relocate_course_tree(root, "/University/.hidden/Algorithms/eclass", uploader)

        self.assertEqual(root.local_path, "/University/.hidden/Algorithms/eclass")
        self.assertEqual(root.children[0].local_path, "/University/.hidden/Algorithms/eclass/Lectures")
        self.assertEqual(
            root.children[0].files[0].local_path,
            "/University/.hidden/Algorithms/eclass/Lectures/week-1.pdf",
        )
        self.assertEqual(uploader.moves, [(
            "/University/Algorithms/eclass/Lectures/week-1.pdf",
            "/University/.hidden/Algorithms/eclass/Lectures/week-1.pdf",
        )])


if __name__ == "__main__":
    unittest.main()
