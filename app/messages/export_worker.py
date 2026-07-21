"""Scheduled Discord archive exports owned by the eClass messages module."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import threading

from app.services.persistence import DatabaseManager

from . import exporter
from .config import MessageConfig


@dataclass(frozen=True)
class ExportConfig:
    enabled: bool
    token: str
    config_file: Path
    executable: Path
    archive_dir: Path
    course_roots: tuple[str, ...]
    interval_seconds: int
    include_threads: str
    media: bool
    parallel: int

    @classmethod
    def from_env(cls, messages: MessageConfig | None = None) -> "ExportConfig":
        messages = messages or MessageConfig.from_env()
        data_dir = Path(os.getenv("DISCORD_EXPORT_DATA_DIR", "/data"))
        database = DatabaseManager(messages.source_db_file)
        try:
            persisted = database.get_discord_export_settings()
        finally:
            database.close()
        return cls(
            enabled=bool(persisted["enabled"]),
            token=str(persisted["token"]),
            config_file=Path(
                os.getenv("DISCORD_EXPORT_CONFIG_FILE", str(data_dir / "discord-export.json"))
            ),
            executable=Path(
                os.getenv(
                    "DISCORD_EXPORT_EXECUTABLE",
                    "/opt/discord-exporter/DiscordChatExporter.Cli",
                )
            ),
            archive_dir=Path(messages.archive_dir),
            course_roots=tuple(messages.course_map),
            interval_seconds=int(persisted["interval_seconds"]),
            include_threads=str(persisted["include_threads"]),
            media=bool(persisted["media"]),
            parallel=int(persisted["parallel"]),
        )

    def write_runtime_config(self) -> None:
        """Write non-secret runtime configuration; the token stays in the environment."""
        payload = {
            "exporter": str(self.executable),
            "output_dir": str(self.archive_dir),
            "state_file": str(self.archive_dir / ".discord-export-state.json"),
            "work_dir": str(self.archive_dir / ".discord-export-work"),
            "channels": list(self.course_roots),
            "token_env": "DISCORD_TOKEN",
            "env_file": None,
            "include_threads": self.include_threads,
            "media": self.media,
            "reuse_media": True,
            "utc": True,
            "parallel": self.parallel,
            "extra_args": [],
        }
        exporter.atomic_write_json(self.config_file, payload)


class ExportWorker:
    """Run one resumable export immediately and then on a fixed interval."""

    def __init__(self, config: ExportConfig | None = None):
        self.config = config or ExportConfig.from_env()
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()

    def run_once(self) -> bool:
        if not self.config.enabled:
            logging.debug("Discord export skipped: exporter is disabled")
            return False
        if not self.config.token:
            logging.warning("Discord export skipped: no token is configured")
            return False
        if not self.config.course_roots:
            logging.info("Discord export skipped: no course channel mapping is configured")
            return False
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)
        self.config.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.config.write_runtime_config()
        settings = exporter.load_settings(self.config.config_file)
        environment = exporter.child_environment(settings, self.config.token)
        with exporter.state_lock(settings):
            state = exporter.load_state(settings.state_file)
            exporter.recover_pending(settings, state)
            return exporter.run_exports(
                settings, state, live=False, environment=environment
            ) == 0

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                succeeded = self.run_once()
                logging.info("Discord archive export completed (success=%s)", succeeded)
            except Exception:
                logging.exception("Discord archive export failed")
            self.wake_event.wait(self.config.interval_seconds)
            self.wake_event.clear()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake_event.set()

    def reload_config(self, messages: MessageConfig | None = None) -> None:
        """Reload frontend-managed mappings and schedule an immediate export."""
        self.config = ExportConfig.from_env(messages)
        self.wake_event.set()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run one export instead of scheduling")
    args = parser.parse_args()
    worker = ExportWorker()
    if args.once:
        return 0 if worker.run_once() else 1
    worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
