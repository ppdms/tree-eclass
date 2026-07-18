"""Validated MCP input schemas."""

from pydantic import BaseModel, Field


class ListMaterialsInput(BaseModel):
    course_id: int
    path_prefix: str | None = None
    document_kinds: list[str] | None = None
    changed_since: str | None = None
    include_insights: bool = False
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=100)


class SearchMaterialsInput(BaseModel):
    query: str = Field(min_length=1, max_length=1000)
    course_ids: list[int] | None = None
    document_kinds: list[str] | None = None
    folder_prefix: str | None = None
    limit: int = Field(default=8, ge=1, le=100)
    retrieval_mode: str = Field(default="hybrid", pattern="^(lexical|semantic|hybrid)$")


class LocatorInput(BaseModel):
    type: str = Field(
        description="Source locator kind, for example page, slide, section, line, sheet, or cell.",
        examples=["page"],
    )
    start: str = Field(
        description="Inclusive locator start as a string; page 25 is represented as start='25'.",
        examples=["25"],
    )
    end: str | None = Field(
        default=None,
        description="Optional inclusive locator end. Omit it to read one source unit.",
        examples=["27"],
    )


class ReadMaterialInput(BaseModel):
    document_id: str
    locators: list[LocatorInput] = Field(default_factory=list)
    include_neighbors: bool = True
    max_characters: int = Field(default=30_000, ge=1)


class RecentChangesInput(BaseModel):
    course_ids: list[int] | None = None
    since: str | None = None
    limit: int = Field(default=100, ge=1, le=200)


class IndexStatusInput(BaseModel):
    course_ids: list[int] | None = None
