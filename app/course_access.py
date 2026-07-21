"""Shared visible-course access checks for course knowledge backends."""

from typing import Any

from app.services.persistence import DatabaseManager


def visible_courses(source_db_file: str, include_hidden: bool = False) -> dict[int, dict[str, Any]]:
    database = DatabaseManager(source_db_file)
    try:
        return {
            int(course["id"]): course
            for course in database.get_courses(include_hidden=include_hidden)
        }
    finally:
        database.close()


def enforce_course_ids(source_db_file: str, requested: list[int] | None) -> list[int]:
    visible = visible_courses(source_db_file)
    if requested is None:
        return list(visible)
    unknown = set(requested) - set(visible)
    if unknown:
        raise ValueError("one or more requested courses are unavailable")
    return list(dict.fromkeys(requested))
