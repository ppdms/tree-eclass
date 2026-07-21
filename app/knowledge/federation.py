"""High-level federation of official materials and community messages."""

from typing import Any

from app.messages.service import CourseMessageService

from .models import SearchRequest
from .service import KnowledgeService


class CourseKnowledgeFederator:
    def __init__(
        self,
        materials: KnowledgeService | None = None,
        messages: CourseMessageService | None = None,
    ):
        self.materials = materials or KnowledgeService()
        self.messages = messages or CourseMessageService()

    def search(
        self,
        query: str,
        course_ids: list[int] | None = None,
        limit_per_source: int = 6,
        retrieval_mode: str = "hybrid",
    ) -> dict[str, Any]:
        official = self.materials.search(SearchRequest(
            query=query,
            course_ids=course_ids,
            limit=limit_per_source,
            retrieval_mode=retrieval_mode,
        ))
        community = self.messages.search(
            query=query,
            course_ids=course_ids,
            limit=limit_per_source,
            retrieval_mode=retrieval_mode,
        )
        return {
            "query": query.strip(),
            "official_materials": official.get("results", []),
            "community_messages": community.get("results", []),
            "official_result_count": len(official.get("results", [])),
            "community_result_count": len(community.get("results", [])),
            "archive_indexed_through": community.get("archive_indexed_through"),
            "evidence_guidance": (
                "Prefer directly relevant official eClass evidence. When official material does not answer "
                "the question, report dated Discord evidence as community discussion, corroborate across "
                "messages where possible, and surface contradictions."
            ),
            "official_untrusted_content_notice": official.get("untrusted_content_notice"),
            "community_untrusted_content_notice": community.get("untrusted_content_notice"),
        }
