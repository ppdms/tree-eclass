"""Deterministic student priorities built from reusable document intelligence."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any


NON_STUDY_TYPES = {"results", "administrative"}
IMPORTANCE_WEIGHT = {"essential": 2.6, "useful": 1.6, "reference": 0.65}
ASSESSMENT_WEIGHT = {"high": 1.55, "medium": 1.2, "low": 0.75, "unknown": 0.95}
DIFFICULTY_WEIGHT = {"advanced": 1.25, "intermediate": 1.08, "introductory": 0.95}


def _exam_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _material_priority(material: dict[str, Any], exam: dict[str, Any] | None,
                       level: int, today: date) -> float:
    ai = material.get("ai") or {}
    if ai.get("material_type") in NON_STUDY_TYPES:
        return 0.0
    gap = {0: 1.0, 1: 0.82, 2: 0.6, 3: 0.34, 4: 0.0, 5: 0.0}.get(level, 1.0)
    if not gap:
        return 0.0
    exam_day = _exam_date(exam.get("exam_at")) if exam else None
    if exam_day:
        days = max(0, (exam_day - today).days)
        urgency = 1.0 + 24.0 / max(3.0, days + 2.0)
    else:
        urgency = 0.75
    density = 1.0 + min(0.25, float(material.get("complexity_score") or 0) / 400)
    return (
        gap
        * IMPORTANCE_WEIGHT.get(ai.get("importance"), 1.15)
        * ASSESSMENT_WEIGHT.get(ai.get("assessment_relevance"), 0.95)
        * DIFFICULTY_WEIGHT.get(ai.get("difficulty"), 1.0)
        * urgency
        * density
    )


def _exam_runways(materials: list[dict[str, Any]], exams: list[dict[str, Any]],
                   study_levels: dict[str, dict[str, int]], today: date) -> list[dict[str, Any]]:
    by_course: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for material in materials:
        by_course[material["course_id"]].append(material)
    result = []
    for exam in exams:
        exam_day = _exam_date(exam.get("exam_at"))
        if not exam_day or exam_day < today:
            continue
        course_id = exam["course_id"]
        if course_id not in by_course:
            continue
        levels = study_levels.get(str(course_id), {})
        course_materials = by_course.get(course_id, [])
        candidates = []
        total_weight = completed_weight = 0.0
        for material in course_materials:
            ai = material.get("ai") or {}
            if ai.get("material_type") in NON_STUDY_TYPES:
                continue
            level = int(levels.get(material["source_path"], 0))
            weight = IMPORTANCE_WEIGHT.get(ai.get("importance"), 1.0)
            total_weight += weight
            completed_weight += weight * min(4, level) / 4
            priority = _material_priority(material, exam, level, today)
            if priority > 0:
                candidates.append({**material, "level": level, "priority": round(priority, 3)})
        candidates.sort(key=lambda item: (-item["priority"], item["display_name"]))
        result.append({
            "course_id": course_id,
            "course_name": exam.get("short_name") or exam.get("course_name"),
            "exam_at": exam["exam_at"],
            "exam_date": exam_day,
            "days_left": (exam_day - today).days,
            "readiness": round(100 * completed_weight / total_weight) if total_weight else 0,
            "remaining_count": len(candidates),
            "essential_remaining": sum(
                (item.get("ai") or {}).get("importance") == "essential" for item in candidates
            ),
            "next_materials": candidates[:3],
        })
    result.sort(key=lambda item: (item["exam_date"], item["course_id"]))
    return result


def _focus_queue(materials: list[dict[str, Any]], exams_by_course: dict[int, dict[str, Any]],
                 study_levels: dict[str, dict[str, int]], today: date,
                 limit: int = 8) -> list[dict[str, Any]]:
    ranked = []
    for material in materials:
        levels = study_levels.get(str(material["course_id"]), {})
        level = int(levels.get(material["source_path"], 0))
        priority = _material_priority(material, exams_by_course.get(material["course_id"]), level, today)
        if priority <= 0:
            continue
        exam = exams_by_course.get(material["course_id"])
        exam_day = _exam_date(exam.get("exam_at")) if exam else None
        ranked.append({
            **material,
            "level": level,
            "priority": round(priority, 3),
            "days_left": max(0, (exam_day - today).days) if exam_day else None,
            "recommended_action": (material.get("ai") or {}).get("recommended_action") or "Review this material",
        })
    ranked.sort(key=lambda item: (-item["priority"], item["course_name"], item["display_name"]))

    # Preserve urgency while preventing one course from monopolizing the list.
    selected, per_course = [], Counter()
    for item in ranked:
        if per_course[item["course_id"]] >= 2:
            continue
        selected.append(item)
        per_course[item["course_id"]] += 1
        if len(selected) >= limit:
            return selected
    for item in ranked:
        if item not in selected:
            selected.append(item)
            if len(selected) >= limit:
                break
    return selected


def _exam_collisions(runways: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for first, second in zip(runways, runways[1:]):
        gap = (second["exam_date"] - first["exam_date"]).days
        if gap > 4:
            continue
        result.append({
            "first": first,
            "second": second,
            "gap_days": gap,
            "message": (
                f"Front-load {second['course_name']} before the final review for "
                f"{first['course_name']}."
            ),
        })
    return result


def build_study_intelligence(materials: list[dict[str, Any]], exam_plans: list[dict[str, Any]],
                             study_levels: dict[str, dict[str, int]], *,
                             selected_course_id: int | None = None,
                             today: date | None = None) -> dict[str, Any]:
    """Combine cached AI evidence with live exam and comprehension state."""
    today = today or date.today()
    exams = [row for row in exam_plans if row.get("enabled") and row.get("exam_at")]
    exams_by_course = {row["course_id"]: row for row in exams}
    scoped = [item for item in materials
              if selected_course_id is None or item["course_id"] == selected_course_id]
    runways = _exam_runways(scoped, exams, study_levels, today)
    collisions = _exam_collisions(_exam_runways(materials, exams, study_levels, today))
    if selected_course_id is not None:
        collisions = [item for item in collisions if selected_course_id in {
            item["first"]["course_id"], item["second"]["course_id"]
        }]
    enriched = sum(bool(item.get("enriched")) for item in scoped)
    coverage_percent = round(100 * enriched / len(scoped)) if scoped else 0
    if enriched < len(scoped):
        # Rounding must not advertise completion while work is still missing.
        coverage_percent = min(99, coverage_percent)
    return {
        "focus_queue": _focus_queue(scoped, exams_by_course, study_levels, today),
        "exam_runways": runways,
        "exam_collisions": collisions,
        "coverage": {
            "enriched": enriched,
            "total": len(scoped),
            "percent": coverage_percent,
        },
    }
