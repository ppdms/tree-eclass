"""Exam-date driven calendar with a countdown to each exam."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class Exam:
    course_id: int
    name: str
    exam_at: datetime

    @property
    def exam_date(self) -> date:
        return self.exam_at.date()


def _coerce_exam(raw: Dict[str, Any]) -> Optional[Exam]:
    exam_at = raw.get("exam_at")
    if isinstance(exam_at, str):
        try:
            exam_at = datetime.fromisoformat(exam_at)
        except ValueError:
            return None
    if not isinstance(exam_at, datetime):
        return None
    return Exam(
        course_id=int(raw["course_id"]),
        name=str(raw["course_name"]),
        exam_at=exam_at,
    )


def build_exam_calendar(
    raw_exams: Iterable[Dict[str, Any]],
    start_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Lay out exam days on a calendar and a countdown to each exam."""
    start_date = start_date or date.today()
    exams = sorted(
        (exam for raw in raw_exams if (exam := _coerce_exam(raw)) is not None),
        key=lambda exam: (exam.exam_at, exam.course_id),
    )

    warnings: List[str] = []
    active: List[Exam] = []
    for exam in exams:
        if exam.exam_date < start_date:
            warnings.append(f"{exam.name}: the exam date has passed.")
        else:
            active.append(exam)

    if not active:
        return {
            "start_date": start_date,
            "end_date": start_date,
            "days": [],
            "exams": [],
            "warnings": warnings,
        }

    end_date = max(exam.exam_date for exam in active)
    days = []
    cursor = start_date
    while cursor <= end_date:
        days.append({
            "date": cursor,
            "is_today": cursor == start_date,
            "exams": [
                {
                    "course_id": exam.course_id,
                    "course_name": exam.name,
                    "exam_at": exam.exam_at,
                    "days_left": (exam.exam_date - start_date).days,
                }
                for exam in active if exam.exam_date == cursor
            ],
        })
        cursor += timedelta(days=1)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "warnings": warnings,
    }
