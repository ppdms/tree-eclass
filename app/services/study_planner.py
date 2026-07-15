"""Deadline-aware exam study planning.

The planner uses a weighted least-slack heuristic.  Review blocks are reserved
at fixed offsets, then the remaining daily capacity is assigned one block at a
time to the course with the greatest required pace.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


REVIEW_RULES = ((7, 1), (3, 1), (1, 2))


@dataclass
class ExamWorkload:
    course_id: int
    name: str
    exam_at: datetime
    remaining_blocks: int
    importance: float = 1.0
    max_daily_blocks: int = 3

    @property
    def exam_date(self) -> date:
        return self.exam_at.date()


def _coerce_exam(raw: Dict[str, Any]) -> Optional[ExamWorkload]:
    exam_at = raw.get("exam_at")
    if isinstance(exam_at, str):
        try:
            exam_at = datetime.fromisoformat(exam_at)
        except ValueError:
            return None
    if not isinstance(exam_at, datetime):
        return None
    return ExamWorkload(
        course_id=int(raw["course_id"]),
        name=str(raw["course_name"]),
        exam_at=exam_at,
        remaining_blocks=max(0, int(raw.get("remaining_blocks", 0))),
        importance=max(0.25, float(raw.get("importance", 1.0))),
        max_daily_blocks=max(1, int(raw.get("max_daily_blocks", 3))),
    )


def build_study_plan(
    raw_exams: Iterable[Dict[str, Any]],
    daily_blocks: int,
    start_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Build a complete plan from ``start_date`` to the final exam.

    ``remaining_blocks`` represents core study work. Review work is added by
    ``REVIEW_RULES`` and labelled separately in the output.
    """
    start_date = start_date or date.today()
    daily_blocks = max(1, int(daily_blocks))
    exams = sorted(
        (exam for raw in raw_exams if (exam := _coerce_exam(raw)) is not None),
        key=lambda exam: (exam.exam_at, exam.course_id),
    )
    warnings: List[str] = []
    active: List[ExamWorkload] = []
    for exam in exams:
        if exam.exam_date <= start_date:
            warnings.append(
                f"{exam.name}: the exam is today or has passed, so no preparation days remain."
            )
        else:
            active.append(exam)

    if not active:
        return {
            "start_date": start_date,
            "daily_blocks": daily_blocks,
            "days": [],
            "today": None,
            "course_totals": [],
            "warnings": warnings,
            "feasible": not warnings,
            "total_required_blocks": 0,
            "total_scheduled_blocks": 0,
        }

    last_study_date = max(exam.exam_date for exam in active) - timedelta(days=1)
    study_dates: List[date] = []
    cursor = start_date
    while cursor <= last_study_date:
        study_dates.append(cursor)
        cursor += timedelta(days=1)

    exam_by_id = {exam.course_id: exam for exam in active}
    remaining = {exam.course_id: exam.remaining_blocks for exam in active}
    scheduled_core = defaultdict(int)
    scheduled_review = defaultdict(int)
    allocations: Dict[date, Dict[int, Dict[str, int]]] = {
        day: defaultdict(lambda: {"core_blocks": 0, "review_blocks": 0})
        for day in study_dates
    }

    reviews_due: Dict[date, List[int]] = defaultdict(list)
    review_required = defaultdict(int)
    for exam in active:
        for offset, count in REVIEW_RULES:
            review_day = exam.exam_date - timedelta(days=offset)
            if review_day >= start_date:
                reviews_due[review_day].extend([exam.course_id] * count)
                review_required[exam.course_id] += count

    # A cumulative deadline check catches impossible exam clusters before the
    # heuristic is run. It intentionally counts every course due by the date.
    for deadline in sorted({exam.exam_date for exam in active}):
        available = (deadline - start_date).days * daily_blocks
        core_required = sum(
            exam.remaining_blocks
            for exam in active
            if exam.exam_date <= deadline
        )
        reviews_required_by_deadline = sum(
            len(course_ids)
            for review_day, course_ids in reviews_due.items()
            if review_day < deadline
        )
        required = core_required + reviews_required_by_deadline
        if required > available:
            warnings.append(
                f"By {deadline.strftime('%d %b')}, the plan needs {required} blocks "
                f"but only {available} are available ({required - available} short)."
            )

    for day in study_dates:
        capacity = daily_blocks
        daily_course_blocks = defaultdict(int)
        due_reviews = Counter(reviews_due.get(day, []))

        while capacity > 0:
            candidates = []
            for exam in active:
                course_id = exam.course_id
                if day >= exam.exam_date:
                    continue
                if remaining[course_id] <= 0 and due_reviews[course_id] <= 0:
                    continue
                if daily_course_blocks[course_id] >= exam.max_daily_blocks:
                    continue
                days_left = max(1, (exam.exam_date - day).days)
                pressure = (remaining[course_id] + due_reviews[course_id]) / days_left
                proximity = 1.5 if days_left <= 3 else 1.0
                score = pressure * exam.importance * proximity
                future_reviews = sum(
                    ids.count(course_id)
                    for review_day, ids in reviews_due.items()
                    if day < review_day < exam.exam_date
                )
                available_course_slots = (
                    exam.max_daily_blocks - daily_course_blocks[course_id]
                    + (days_left - 1) * exam.max_daily_blocks
                )
                obligations = remaining[course_id] + due_reviews[course_id] + future_reviews
                slack = available_course_slots - obligations
                candidates.append((-slack, score, -days_left, -exam.course_id, exam))
            if not candidates:
                break
            exam = max(candidates, key=lambda item: item[:4])[4]
            course_id = exam.course_id
            if due_reviews[course_id] > 0:
                allocations[day][course_id]["review_blocks"] += 1
                scheduled_review[course_id] += 1
                due_reviews[course_id] -= 1
            else:
                allocations[day][course_id]["core_blocks"] += 1
                scheduled_core[course_id] += 1
                remaining[course_id] -= 1
            daily_course_blocks[course_id] += 1
            capacity -= 1

        for course_id, count in due_reviews.items():
            if count > 0:
                warnings.append(
                    f"{exam_by_id[course_id].name}: {count} review block(s) due "
                    f"{day.strftime('%d %b')} could not be scheduled."
                )

    course_totals = []
    for exam in active:
        course_id = exam.course_id
        missing_review = review_required[course_id] - scheduled_review[course_id]
        if remaining[course_id] > 0:
            warnings.append(
                f"{exam.name}: {remaining[course_id]} core block(s) remain unscheduled before the exam."
            )
        course_totals.append({
            "course_id": course_id,
            "course_name": exam.name,
            "exam_at": exam.exam_at,
            "required_core_blocks": exam.remaining_blocks,
            "required_review_blocks": review_required[course_id],
            "scheduled_core_blocks": scheduled_core[course_id],
            "scheduled_review_blocks": scheduled_review[course_id],
            "unscheduled_core_blocks": remaining[course_id],
            "unscheduled_review_blocks": max(0, missing_review),
        })

    days = []
    for day in study_dates:
        day_items = []
        for course_id, counts in allocations[day].items():
            total = counts["core_blocks"] + counts["review_blocks"]
            if not total:
                continue
            exam = exam_by_id[course_id]
            day_items.append({
                "course_id": course_id,
                "course_name": exam.name,
                "exam_at": exam.exam_at,
                "core_blocks": counts["core_blocks"],
                "review_blocks": counts["review_blocks"],
                "blocks": total,
            })
        day_items.sort(key=lambda item: (item["exam_at"], item["course_id"]))
        if day_items:
            days.append({
                "date": day,
                "allocations": day_items,
                "blocks": sum(item["blocks"] for item in day_items),
            })

    total_required = sum(
        exam.remaining_blocks + review_required[exam.course_id] for exam in active
    )
    total_scheduled = sum(day["blocks"] for day in days)
    today = next((day for day in days if day["date"] == start_date), None)
    return {
        "start_date": start_date,
        "daily_blocks": daily_blocks,
        "days": days,
        "today": today,
        "course_totals": course_totals,
        "warnings": list(dict.fromkeys(warnings)),
        "feasible": not warnings,
        "total_required_blocks": total_required,
        "total_scheduled_blocks": total_scheduled,
    }
