"""Durable job queue helpers."""

from .store import KnowledgeStore


class KnowledgeJobs:
    def __init__(self, store: KnowledgeStore):
        self.store = store

    def enqueue_upsert(self, course_id: int, path: str, source_hash: str | None) -> bool:
        return self.store.enqueue(course_id, path, source_hash, "upsert")

    def enqueue_delete(self, course_id: int, path: str) -> bool:
        return self.store.enqueue(course_id, path, None, "delete")

    def claim(self):
        return self.store.claim_job()
