"""Helpers for constructing the WebDAV path used for course downloads."""

import posixpath


DEFAULT_DOWNLOAD_BASE_PATH = "/University"


def normalize_download_base_path(base_path: str | None) -> str:
    """Return a safe, absolute WebDAV directory path."""
    value = (base_path or DEFAULT_DOWNLOAD_BASE_PATH).strip().replace("\\", "/")
    value = "/" + value.strip("/") if value.strip("/") else DEFAULT_DOWNLOAD_BASE_PATH
    return posixpath.normpath(value)


def _course_path_component(course_name: str) -> str:
    """Keep a course name as one WebDAV path component."""
    component = " ".join((course_name or "Unnamed course").split())
    component = component.replace("/", "-").replace("\\", "-")
    component = component.strip(" .")
    return component or "Unnamed course"


def course_download_path(base_path: str | None, course_name: str, hidden: bool = False) -> str:
    """Construct ``/<base>/<course>/eclass`` (or the hidden equivalent)."""
    base = normalize_download_base_path(base_path)
    parts = [base]
    if hidden:
        parts.append(".hidden")
    parts.extend([_course_path_component(course_name), "eclass"])
    return posixpath.join(*parts)
