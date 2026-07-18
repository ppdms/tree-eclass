"""Transport-independent knowledge models."""

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class ExtractedUnit:
    locator_type: str
    locator_start: str
    text: str
    locator_end: Optional[str] = None
    heading: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractedDocument:
    title: str
    kind: str
    units: list[ExtractedUnit]
    metadata: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class SourceMetadata:
    course_id: int
    course_name: str
    course_short_name: Optional[str]
    source_path: str
    source_url: Optional[str]
    display_name: str
    source_hash: str
    mime_type: Optional[str] = None
    academic_year: Optional[str] = None
    response_mime_type: Optional[str] = None
    source_modified_at: Optional[str] = None


@dataclass
class Chunk:
    id: str
    document_id: str
    ordinal: int
    locator_type: str
    locator_start: str
    locator_end: Optional[str]
    heading: Optional[str]
    text: str
    normalized_text: str
    content_hash: str
    metadata: dict[str, Any] = field(default_factory=dict)


class ResultMixin:
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)  # type: ignore[arg-type]


@dataclass
class CourseSummary(ResultMixin):
    course_id: int
    name: str
    short_name: Optional[str]
    discovered_documents: int
    supported_documents: int
    indexed_documents: int
    total_supported_documents: int
    unsupported_documents: int
    unsupported_reasons: dict[str, int]
    pending_jobs: int
    failed_jobs: int
    latest_indexed_at: Optional[str]


@dataclass
class ListMaterialsRequest:
    course_id: int
    path_prefix: Optional[str] = None
    document_kinds: Optional[list[str]] = None
    changed_since: Optional[str] = None
    cursor: Optional[str] = None
    limit: int = 50


@dataclass
class SearchRequest:
    query: str
    course_ids: Optional[list[int]] = None
    document_kinds: Optional[list[str]] = None
    folder_prefix: Optional[str] = None
    limit: int = 8
    retrieval_mode: str = "hybrid"


@dataclass
class Locator:
    type: str
    start: str
    end: Optional[str] = None


@dataclass
class ReadRequest:
    document_id: str
    locators: list[Locator] = field(default_factory=list)
    include_neighbors: bool = True
    max_characters: int = 30_000


@dataclass
class RecentChangesRequest:
    course_ids: Optional[list[int]] = None
    since: Optional[str] = None
    limit: int = 100
