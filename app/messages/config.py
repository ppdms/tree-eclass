"""Configuration for the dedicated Discord message knowledge backend."""

from dataclasses import dataclass
import os

from app.services.persistence import DatabaseManager


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def _course_map(source_db_file: str) -> dict[str, int]:
    database = None
    try:
        database = DatabaseManager(source_db_file)
        return database.get_discord_course_map()
    except Exception:
        return {}
    finally:
        if database is not None:
            database.close()


@dataclass(frozen=True)
class MessageConfig:
    enabled: bool
    worker_enabled: bool
    db_file: str
    source_db_file: str
    archive_dir: str
    course_map: dict[str, int]
    poll_seconds: int
    verify_hashes: bool
    window_gap_seconds: int
    window_max_messages: int
    window_max_characters: int
    search_limit_max: int
    semantic_scan_limit: int
    policy_half_life_days: float
    general_half_life_days: float

    @classmethod
    def from_env(cls) -> "MessageConfig":
        source_db = os.getenv("DB_FILE", "eclass.db")
        default_db = os.path.join(os.path.dirname(source_db) or ".", "discord_knowledge.db")
        return cls(
            enabled=_bool("DISCORD_KNOWLEDGE_ENABLED", True),
            worker_enabled=_bool("DISCORD_KNOWLEDGE_WORKER_ENABLED", True),
            db_file=os.getenv("DISCORD_KNOWLEDGE_DB_FILE", default_db),
            source_db_file=source_db,
            archive_dir=os.getenv("DISCORD_ARCHIVE_DIR", "/data/discord_exports"),
            course_map=_course_map(source_db),
            poll_seconds=_int("DISCORD_KNOWLEDGE_POLL_SECONDS", 300, 15),
            verify_hashes=_bool("DISCORD_KNOWLEDGE_VERIFY_HASHES", True),
            window_gap_seconds=_int("DISCORD_WINDOW_GAP_SECONDS", 900, 30),
            window_max_messages=_int("DISCORD_WINDOW_MAX_MESSAGES", 10, 1),
            window_max_characters=_int("DISCORD_WINDOW_MAX_CHARACTERS", 3500, 250),
            search_limit_max=_int("DISCORD_SEARCH_LIMIT_MAX", 20, 1),
            semantic_scan_limit=_int("DISCORD_SEMANTIC_SCAN_LIMIT", 5000, 100),
            policy_half_life_days=_float("DISCORD_POLICY_HALF_LIFE_DAYS", 180.0, 1.0),
            general_half_life_days=_float("DISCORD_GENERAL_HALF_LIFE_DAYS", 730.0, 1.0),
        )
