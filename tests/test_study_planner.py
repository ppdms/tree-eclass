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
    def test_earliest_deadline_receives_the_needed_capacity(self):
        plan = build_study_plan(
            [
                exam(1, "Early", "2026-09-04T09:00", 6),
                exam(2, "Later", "2026-09-10T09:00", 6),
            ],
            daily_blocks=3,
            start_date=date(2026, 9, 1),
        )
        early = next(item for item in plan["course_totals"] if item["course_id"] == 1)
        self.assertEqual(early["scheduled_core_blocks"], 6)
        self.assertEqual(early["unscheduled_core_blocks"], 0)
        self.assertTrue(all(day["blocks"] <= 3 for day in plan["days"]))

    def test_review_blocks_are_reserved_at_seven_three_and_one_days(self):
        plan = build_study_plan(
            [exam(1, "Algorithms", datetime(2026, 9, 11, 9), 0)],
            daily_blocks=4,
            start_date=date(2026, 9, 1),
        )
        review_days = {
            day["date"]: day["allocations"][0]["review_blocks"]
            for day in plan["days"]
        }
        self.assertEqual(review_days[date(2026, 9, 4)], 1)
        self.assertEqual(review_days[date(2026, 9, 8)], 1)
        self.assertEqual(review_days[date(2026, 9, 10)], 2)

    def test_impossible_cluster_reports_shortage(self):
        plan = build_study_plan(
            [
                exam(1, "One", "2026-09-03T09:00", 5),
                exam(2, "Two", "2026-09-03T14:00", 5),
            ],
            daily_blocks=3,
            start_date=date(2026, 9, 1),
        )
        self.assertFalse(plan["feasible"])
        self.assertTrue(any("short" in warning for warning in plan["warnings"]))

    def test_daily_course_cap_includes_review_blocks(self):
        plan = build_study_plan(
            [exam(1, "Capped", "2026-09-08T09:00", 10, max_daily_blocks=2)],
            daily_blocks=8,
            start_date=date(2026, 9, 1),
        )
        for day in plan["days"]:
            for allocation in day["allocations"]:
                self.assertLessEqual(allocation["blocks"], 2)


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


if __name__ == "__main__":
    unittest.main()
