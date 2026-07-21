"""Dedicated Discord course-message indexing and retrieval."""

from .config import MessageConfig
from .service import CourseMessageService

__all__ = ["CourseMessageService", "MessageConfig"]
