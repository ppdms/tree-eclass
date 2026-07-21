"""Independent background refresh loop and CLI for Discord messages."""

import argparse
import json
import logging
import signal
import threading

from app.knowledge.config import KnowledgeConfig
from app.knowledge.embeddings import EmbeddingProvider

from .config import MessageConfig
from .indexer import MessageIndexer
from .store import MessageStore


class MessageWorker:
    def __init__(self, config: MessageConfig | None = None):
        self.config = config or MessageConfig.from_env()
        knowledge_config = KnowledgeConfig.from_env()
        self.store = MessageStore(
            self.config.db_file,
            embedding_provider=EmbeddingProvider.from_config(knowledge_config),
        )
        self.indexer = MessageIndexer(self.config, self.store)
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                result = self.indexer.refresh()
                if result.get("sources_indexed") or result.get("failures"):
                    logging.info("Discord message refresh: %s", result)
            except Exception:
                logging.exception("Discord message refresh failed")
            self.wake_event.wait(self.config.poll_seconds)
            self.wake_event.clear()

    def run_once(self) -> dict[str, object]:
        return self.indexer.refresh()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake_event.set()

    def reload_config(self) -> None:
        """Reload frontend-managed mappings and wake the indexer immediately."""
        config = MessageConfig.from_env()
        self.config = config
        self.indexer = MessageIndexer(config, self.store)
        self.wake_event.set()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Discord course-message indexing worker")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--forever", action="store_true")
    mode.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    worker = MessageWorker()
    if args.status:
        print(json.dumps(worker.store.status(), ensure_ascii=False, indent=2))
    elif args.once:
        print(json.dumps(worker.run_once(), ensure_ascii=False, indent=2))
    else:
        signal.signal(signal.SIGTERM, lambda *_: worker.stop())
        signal.signal(signal.SIGINT, lambda *_: worker.stop())
        worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
