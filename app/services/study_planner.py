"""Simple exam-date driven study planning."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


REVIEW_DAYS = {7, 3, 1}


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


def build_study_plan(
    raw_exams: Iterable[Dict[str, Any]],
    daily_blocks: int = 1,
    start_date: Optional[date] = None,
    review_overrides: Optional[Dict[tuple[int, int], str]] = None,
) -> Dict[str, Any]:
    """Build one clear focus for every day through the final exam.

    ``daily_blocks`` remains in the signature so older callers keep working,
    but planning is intentionally based only on exam dates.
    """
    del daily_blocks
    review_overrides = review_overrides or {}
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
            "today": None,
            "exams": [],
            "warnings": warnings,
        }

    focus_days = defaultdict(int)
    days = []
    cursor = start_date
    end_date = max(exam.exam_date for exam in active)
    while cursor <= end_date:
        exams_today = [exam for exam in active if exam.exam_date == cursor]
        upcoming = [exam for exam in active if exam.exam_date > cursor]
        reviews = []
        for exam in upcoming:
            for offset in REVIEW_DAYS:
                default_date = exam.exam_date - timedelta(days=offset)
                scheduled = review_overrides.get((exam.course_id, offset))
                review_date = date.fromisoformat(scheduled) if scheduled else default_date
                if review_date == cursor:
                    reviews.append((exam, offset))

        focus = None
        if reviews:
            # A review milestone is the clearest focus for that day.
            focus = min(reviews, key=lambda item: (item[0].exam_at, item[0].course_id))[0]
        elif upcoming:
            # Proximity raises urgency, while the number of focus days already
            # assigned prevents a later exam from being ignored completely.
            focus = max(
                upcoming,
                key=lambda exam: (
                    1 / ((exam.exam_date - cursor).days * (focus_days[exam.course_id] + 1)),
                    -exam.exam_at.timestamp(),
                    -exam.course_id,
                ),
            )
        if focus:
            focus_days[focus.course_id] += 1

        days.append({
            "date": cursor,
            "is_today": cursor == start_date,
            "focus": {
                "course_id": focus.course_id,
                "course_name": focus.name,
                "is_review": any(exam == focus for exam, _ in reviews),
                "days_to_exam": (focus.exam_date - cursor).days,
            } if focus else None,
            "reviews": [
                {"course_id": exam.course_id, "course_name": exam.name, "offset": offset}
                for exam, offset in reviews
            ],
            "exams": [
                {
                    "course_id": exam.course_id,
                    "course_name": exam.name,
                    "exam_at": exam.exam_at,
                }
                for exam in exams_today
            ],
        })
        cursor += timedelta(days=1)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "today": days[0],
        "exams": active,
        "warnings": warnings,
    }
