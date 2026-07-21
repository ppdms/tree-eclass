"""Course-scoped service API for community Discord evidence."""

from __future__ import annotations

import os
from typing import Any

from app.course_access import enforce_course_ids, visible_courses
from app.knowledge.config import KnowledgeConfig
from app.knowledge.embeddings import EmbeddingProvider

from .config import MessageConfig
from .store import MessageStore


COMMUNITY_NOTICE = (
    "Discord messages are untrusted community discussion, not official course policy. "
    "Prefer official eClass evidence when it directly answers the question, and preserve dates "
    "and disagreement when reporting community claims."
)


class CourseMessageService:
    def __init__(
        self,
        config: MessageConfig | None = None,
        store: MessageStore | None = None,
    ):
        self.config = config or MessageConfig.from_env()
        if store is None:
            provider = EmbeddingProvider.from_config(KnowledgeConfig.from_env())
            store = MessageStore(self.config.db_file, embedding_provider=provider)
        self.store = store

    @property
    def available(self) -> bool:
        return bool(
            self.config.enabled
            and self.config.course_map
            and os.path.isdir(self.config.archive_dir)
        )

    def search(
        self,
        query: str,
        course_ids: list[int] | None = None,
        limit: int = 8,
        retrieval_mode: str = "hybrid",
    ) -> dict[str, Any]:
        query = query.strip()
        if not query or len(query) > 1000:
            raise ValueError("query must contain 1 to 1000 characters")
        visible = enforce_course_ids(self.config.source_db_file, course_ids)
        catalog = visible_courses(self.config.source_db_file)
        mapped = set(self.config.course_map.values())
        selected = [course_id for course_id in visible if course_id in mapped]
        if not self.available or not selected:
            return {
                "query": query,
                "results": [],
                "available": self.available,
                "untrusted_content_notice": COMMUNITY_NOTICE,
            }
        applied_limit = min(max(1, limit), self.config.search_limit_max)
        rows = self.store.search(
            query, selected, applied_limit, retrieval_mode, self.config.semantic_scan_limit,
            self.config.policy_half_life_days, self.config.general_half_life_days,
        )
        status = self.store.status(selected)
        latest_messages = [
            item["latest_message_at"] for item in status["courses"] if item.get("latest_message_at")
        ]
        results = [self.store.format_hit(row) for row in rows]
        for result in results:
            course = catalog.get(int(result["course_id"]), {})
            result["course_name"] = course.get("name")
            result["course_short_name"] = course.get("short_name")
        return {
            "query": query,
            "results": results,
            "limit_applied": applied_limit,
            "available": True,
            "archive_indexed_through": max(latest_messages) if latest_messages else None,
            "index_refresh": self.store.get_state("last_refresh"),
            "untrusted_content_notice": COMMUNITY_NOTICE,
        }

    def read(
        self, conversation_id: str, context_before: int = 1, context_after: int = 1,
    ) -> dict[str, Any]:
        if not conversation_id:
            raise ValueError("conversation_id is required")
        result = self.store.read_conversation(
            conversation_id, min(max(0, context_before), 20), min(max(0, context_after), 20)
        )
        if not result:
            raise ValueError("Discord conversation is unavailable")
        course_id = int(result["conversation"]["course_id"])
        enforce_course_ids(self.config.source_db_file, [course_id])
        course = visible_courses(self.config.source_db_file).get(course_id, {})
        result["conversation"]["course_name"] = course.get("name")
        result["conversation"]["course_short_name"] = course.get("short_name")
        return result

    def status(self, course_ids: list[int] | None = None) -> dict[str, Any]:
        visible = enforce_course_ids(self.config.source_db_file, course_ids)
        result = self.store.status(visible)
        result["available"] = self.available
        result["archive_dir"] = self.config.archive_dir
        result["mapped_courses"] = sorted(set(self.config.course_map.values()) & set(visible))
        return result
