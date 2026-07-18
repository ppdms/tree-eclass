import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import date
from pathlib import Path

# Import service modules without executing app.services.__init__, which eagerly
# imports scraper dependencies unrelated to planner unit tests.
ROOT = Path(__file__).resolve().parents[1]
app_package = types.ModuleType("app")
app_package.__path__ = [str(ROOT / "app")]
services_package = types.ModuleType("app.services")
services_package.__path__ = [str(ROOT / "app" / "services")]
sys.modules.setdefault("app", app_package)
sys.modules.setdefault("app.services", services_package)
tree_builder_stub = types.ModuleType("app.services.tree_builder")
tree_builder_stub.Node = type("Node", (), {})
tree_builder_stub.File = type("File", (), {})
differ_stub = types.ModuleType("app.services.differ")
differ_stub.ChangeItem = type("ChangeItem", (), {})
sys.modules.setdefault("app.services.tree_builder", tree_builder_stub)
sys.modules.setdefault("app.services.differ", differ_stub)

from app.services.persistence import DatabaseManager, SCHEMA_VERSION
from app.services.study_planner import build_exam_calendar


def exam(course_id, name, exam_at, remaining_blocks, importance=1.0, max_daily_blocks=3):
    return {
        "course_id": course_id,
        "course_name": name,
        "exam_at": exam_at,
        "remaining_blocks": remaining_blocks,
        "importance": importance,
        "max_daily_blocks": max_daily_blocks,
    }


class StudyPlannerTests(unittest.TestCase):
    def test_calendar_contains_every_day_through_final_exam(self):
        plan = build_exam_calendar(
            [
                exam(1, "Early", "2026-09-04T09:00", 6),
                exam(2, "Later", "2026-09-10T09:00", 6),
            ],
            start_date=date(2026, 9, 1),
        )
        self.assertEqual(len(plan["days"]), 10)
        self.assertEqual(plan["days"][0]["date"], date(2026, 9, 1))
        self.assertEqual(plan["days"][-1]["date"], date(2026, 9, 10))

    def test_exam_day_is_in_calendar_and_marked(self):
        plan = build_exam_calendar(
            [exam(1, "One", "2026-09-03T09:00", 5)],
            start_date=date(2026, 9, 1),
        )
        exam_day = plan["days"][-1]
        self.assertEqual(exam_day["date"], date(2026, 9, 3))
        self.assertEqual(exam_day["exams"][0]["course_name"], "One")

    def test_past_exams_are_reported_as_warnings(self):
        plan = build_exam_calendar(
            [exam(1, "Past", "2026-08-31T09:00", 0)],
            start_date=date(2026, 9, 1),
        )
        self.assertEqual(plan["days"], [])
        self.assertEqual(plan["warnings"], ["Past: the exam date has passed."])

    def test_invalid_exam_dates_are_ignored(self):
        plan = build_exam_calendar(
            [{"course_id": 1, "course_name": "Broken", "exam_at": "not-a-date"}],
            start_date=date(2026, 9, 1),
        )
        self.assertEqual(plan["days"], [])
        self.assertEqual(plan["warnings"], [])


class StudyPlannerPersistenceTests(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        self.db = DatabaseManager(self.path)

    def tearDown(self):
        self.db.close()
        os.unlink(self.path)

    def test_schema_and_course_plan_round_trip(self):
        self.db.save_course(10, "Algorithms", "/Algorithms")
        self.db.save_study_planner_settings(7, 45)
        self.db.save_course_exam_plan(10, "2026-09-23T08:30", 24, 1.25, 3, True)

        self.assertEqual(self.db._get_schema_version(), SCHEMA_VERSION)
        self.assertEqual(self.db.get_study_planner_settings(), {"daily_blocks": 7, "block_minutes": 45})
        row = self.db.get_course_exam_plans()[0]
        self.assertEqual(row["exam_at"], "2026-09-23T08:30")
        self.assertEqual(row["remaining_blocks"], 24)
        self.assertTrue(row["enabled"])

        # Updating a course must not delete its planner row via SQLite REPLACE.
        self.db.save_course(10, "Algorithms II", "/Algorithms")
        self.assertEqual(self.db.get_course_exam_plans()[0]["remaining_blocks"], 24)

    def test_hidden_courses_are_preserved_but_excluded_from_ui_reads(self):
        self.db.save_course(10, "Visible", "/Visible")
        self.db.save_course(20, "Hidden", "/Hidden")
        self.assertTrue(self.db.set_course_hidden(20, True))

        self.assertEqual([c["id"] for c in self.db.get_courses()], [10])
        all_courses = self.db.get_courses(include_hidden=True)
        self.assertEqual([c["id"] for c in all_courses], [10, 20])
        self.assertTrue(all_courses[1]["hidden"])
        self.assertIsNone(self.db.get_course(20))
        self.assertEqual(self.db.get_course(20, include_hidden=True)["name"], "Hidden")

        # Ordinary course updates must not accidentally reveal it.
        self.db.save_course(20, "Hidden II", "/Hidden-II")
        self.assertTrue(self.db.get_course(20, include_hidden=True)["hidden"])

        cursor = self.db.conn.cursor()
        for course_id in (10, 20):
            cursor.execute(
                """INSERT INTO change_records
                   (course_id, change_no, message, changes_count)
                   VALUES (?, ?, '+ 1 − 0 ~ 0', 1)""",
                (course_id, f"2026-07-15T10:00:0{course_id // 10}"),
            )
            cursor.execute(
                """INSERT INTO announcements
                   (course_id, announcement_id, title, link, pub_date)
                   VALUES (?, ?, ?, 'https://example.test', '2026-07-15T10:00:00')""",
                (course_id, f"ann-{course_id}", f"Announcement {course_id}"),
            )
            cursor.execute(
                """INSERT INTO exercises
                   (course_id, exercise_id, title, link)
                   VALUES (?, ?, ?, 'https://example.test')""",
                (course_id, f"ex-{course_id}", f"Exercise {course_id}"),
            )
        self.db.conn.commit()

        self.assertEqual(
            {row["course_id"] for row in self.db.get_change_records()}, {10}
        )
        self.assertEqual(
            {row["course_id"] for row in self.db.get_timeline_data()}, {10}
        )
        self.assertEqual(
            {row["course_id"] for row in self.db.get_announcements()}, {10}
        )
        self.assertEqual(
            {row["course_id"] for row in self.db.get_exercises()}, {10}
        )

        # Synchronization reads can still see hidden-course exercise state.
        synced = self.db.get_exercises(
            course_id=20,
            include_ignored=True,
            include_hidden_courses=True,
        )
        self.assertEqual([row["exercise_id"] for row in synced], ["ex-20"])

        self.db.set_check_status(True, 20)
        status = self.db.get_check_status()
        self.assertTrue(status["is_checking"])
        self.assertIsNone(status["current_course_id"])
        self.assertIsNone(status["course_name"])


class CourseVisibilityMigrationTests(unittest.TestCase):
    def test_version_23_database_migrates_existing_courses_to_visible(self):
        handle, path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        try:
            conn = sqlite3.connect(path)
            conn.execute("""
                CREATE TABLE schema_version (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("INSERT INTO schema_version (id, version) VALUES (1, 23)")
            conn.execute("""
                CREATE TABLE courses (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    webdav_folder TEXT NOT NULL,
                    sort_order INTEGER DEFAULT 0
                )
            """)
            conn.execute(
                "INSERT INTO courses (id, name, webdav_folder) VALUES (1, 'Legacy', '/Legacy')"
            )
            conn.commit()
            conn.close()

            db = DatabaseManager(path)
            try:
                self.assertEqual(db._get_schema_version(), SCHEMA_VERSION)
                self.assertEqual(db.get_courses()[0]["name"], "Legacy")
                self.assertFalse(db.get_courses()[0]["hidden"])
            finally:
                db.close()
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
