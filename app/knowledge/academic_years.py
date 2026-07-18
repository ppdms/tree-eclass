"""Extract academic-year provenance from synchronized material paths."""

import re


_ACADEMIC_YEAR_IN_PATH = re.compile(
    r"(?<!\d)(20\d{2})\s*[-_/]\s*(20\d{2}|\d{2})(?!\d)"
)


def academic_year_from_path(path: str) -> str | None:
    """Extract the first academic year embedded in a material path."""
    match = _ACADEMIC_YEAR_IN_PATH.search(path)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)[-2:]}"
