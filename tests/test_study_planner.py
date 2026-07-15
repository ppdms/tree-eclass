import os
import sys
import tempfile
import types
import unittest
from datetime import date, datetime
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
from app.services.study_planner import build_study_plan


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
        plan = build_study_plan(
            [
                exam(1, "Early", "2026-09-04T09:00", 6),
                exam(2, "Later", "2026-09-10T09:00", 6),
            ],
            start_date=date(2026, 9, 1),
        )
        self.assertEqual(len(plan["days"]), 10)
        self.assertEqual(plan["days"][0]["date"], date(2026, 9, 1))
        self.assertEqual(plan["days"][-1]["date"], date(2026, 9, 10))

    def test_reviews_are_highlighted_at_seven_three_and_one_days(self):
        plan = build_study_plan(
            [exam(1, "Algorithms", datetime(2026, 9, 11, 9), 0)],
            start_date=date(2026, 9, 1),
        )
        review_days = {
            day["date"]
            for day in plan["days"]
            if day["focus"] and day["focus"]["is_review"]
        }
        self.assertEqual(review_days, {
            date(2026, 9, 4), date(2026, 9, 8), date(2026, 9, 10)
        })

    def test_exam_day_is_in_calendar_and_marked(self):
        plan = build_study_plan(
            [exam(1, "One", "2026-09-03T09:00", 5)],
            start_date=date(2026, 9, 1),
        )
        exam_day = plan["days"][-1]
        self.assertEqual(exam_day["date"], date(2026, 9, 3))
        self.assertEqual(exam_day["exams"][0]["course_name"], "One")

    def test_multiple_courses_receive_focus_days(self):
        plan = build_study_plan(
            [
                exam(1, "Earlier", "2026-09-08T09:00", 10),
                exam(2, "Later", "2026-09-12T09:00", 10),
            ],
            start_date=date(2026, 9, 1),
        )
        focused = {day["focus"]["course_id"] for day in plan["days"] if day["focus"]}
        self.assertEqual(focused, {1, 2})

    def test_review_can_be_rescheduled(self):
        plan = build_study_plan(
            [exam(1, "Algorithms", "2026-09-11T09:00", 0)],
            start_date=date(2026, 9, 1),
            review_overrides={(1, 3): "2026-09-07"},
        )
        review_dates = {
            day["date"]: [review["offset"] for review in day["reviews"]]
            for day in plan["days"] if day["reviews"]
        }
        self.assertIn(3, review_dates[date(2026, 9, 7)])
        self.assertNotIn(date(2026, 9, 8), review_dates)


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

    def test_calendar_items_and_review_overrides_round_trip(self):
        self.db.save_course(10, "Algorithms", "/Algorithms")
        item_id = self.db.add_study_plan_item(10, "2026-09-10", "review")
        self.db.update_study_plan_item(item_id, scheduled_date="2026-09-11", completed=True)
        item = self.db.get_study_plan_items()[0]
        self.assertEqual(item["scheduled_date"], "2026-09-11")
        self.assertTrue(item["completed"])

        self.db.save_study_review_override(10, 3, "2026-09-20")
        self.assertEqual(self.db.get_study_review_overrides()[(10, 3)], "2026-09-20")
        self.assertTrue(self.db.delete_study_plan_item(item_id))
        self.assertEqual(self.db.get_study_plan_items(), [])


if __name__ == "__main__":
    unittest.main()
