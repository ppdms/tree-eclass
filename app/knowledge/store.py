"""SQLite storage for the rebuildable knowledge index."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sqlite3
from typing import Any, Iterator, Optional

from .models import Chunk, SourceMetadata
from .embeddings import (LOCAL_MODEL_NAME, EmbeddingProvider, cosine, embed_text,
                         pack_vector, unpack_vector)
from .normalization import document_id, normalize_path, search_normalize


SCHEMA_VERSION = 5

POLICY_KEYWORDS = tuple(search_normalize(value) for value in (
    "intro", "introduction", "course", "description", "syllabus", "grading",
    "grade", "βαθμολογία", "περιγραφή", "εισαγωγή", "οργάνωση",
))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SCHEMA = """
CREATE TABLE IF NOT EXISTS knowledge_schema_version (
    id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    course_id INTEGER NOT NULL,
    course_name TEXT NOT NULL,
    course_short_name TEXT,
    source_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    source_url TEXT,
    display_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    mime_type TEXT,
    response_mime_type TEXT,
    document_kind TEXT NOT NULL,
    academic_year TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    page_count INTEGER,
    language_hint TEXT,
    extractor_name TEXT,
    extractor_version TEXT,
    indexed_at TEXT,
    error TEXT,
    diagnostic_reason TEXT,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    UNIQUE(course_id, normalized_path)
);
CREATE INDEX IF NOT EXISTS idx_documents_course_current ON documents(course_id, is_current, status);
CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    locator_type TEXT NOT NULL,
    locator_start TEXT,
    locator_end TEXT,
    heading TEXT,
    text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    UNIQUE(document_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_chunks_document_locator ON chunks(document_id, locator_type, locator_start);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    chunk_id UNINDEXED, text, normalized_text, heading, display_name, source_path, course_name,
    tokenize='unicode61 remove_diacritics 2'
);
CREATE TABLE IF NOT EXISTS index_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    requested_hash TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL CHECK(action IN ('upsert', 'delete')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed', 'stale')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    completed_at TEXT,
    error TEXT,
    UNIQUE(course_id, normalized_path, requested_hash, action)
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON index_jobs(status, available_at, id);
CREATE TABLE IF NOT EXISTS knowledge_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chunk_embeddings (
    chunk_id TEXT NOT NULL,
    model TEXT NOT NULL,
    vector BLOB NOT NULL,
    dimensions INTEGER NOT NULL,
    PRIMARY KEY(chunk_id, model),
    FOREIGN KEY(chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
);
"""


class KnowledgeStore:
    def __init__(self, db_file: str, embedding_provider: EmbeddingProvider | None = None):
        self.db_file = db_file
        self.embedding_provider = embedding_provider or EmbeddingProvider.from_env()
        parent = os.path.dirname(os.path.abspath(db_file))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connection() as conn:
            conn.executescript(SCHEMA)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(chunks)")}
            if "metadata_json" not in columns:
                conn.execute("ALTER TABLE chunks ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'")
            document_columns = {row[1] for row in conn.execute("PRAGMA table_info(documents)")}
            if "diagnostic_reason" not in document_columns:
                conn.execute("ALTER TABLE documents ADD COLUMN diagnostic_reason TEXT")
            if "response_mime_type" not in document_columns:
                conn.execute("ALTER TABLE documents ADD COLUMN response_mime_type TEXT")
            embedding_primary_key = [row[1] for row in conn.execute("PRAGMA table_info(chunk_embeddings)")
                                     if row[5]]
            if embedding_primary_key == ["chunk_id"]:
                conn.execute("ALTER TABLE chunk_embeddings RENAME TO chunk_embeddings_legacy")
                conn.execute(
                    """CREATE TABLE chunk_embeddings (
                        chunk_id TEXT NOT NULL,
                        model TEXT NOT NULL,
                        vector BLOB NOT NULL,
                        dimensions INTEGER NOT NULL,
                        PRIMARY KEY(chunk_id, model),
                        FOREIGN KEY(chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
                    )"""
                )
                conn.execute(
                    """INSERT INTO chunk_embeddings(chunk_id,model,vector,dimensions)
                       SELECT chunk_id,model,vector,dimensions FROM chunk_embeddings_legacy"""
                )
                conn.execute("DROP TABLE chunk_embeddings_legacy")
            conn.execute(
                "INSERT INTO knowledge_schema_version(id, version, updated_at) VALUES(1, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET version=excluded.version, updated_at=excluded.updated_at",
                (SCHEMA_VERSION, utc_now()),
            )
            conn.commit()
        self.backfill_embeddings()

    def backfill_embeddings(self) -> int:
        """Populate vectors for chunks that have no embedding record at all."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT c.id,c.text FROM chunks c "
                "WHERE NOT EXISTS (SELECT 1 FROM chunk_embeddings e WHERE e.chunk_id=c.id)"
            ).fetchall()
        if not rows:
            return 0
        embedding_sets = self._embedding_sets([row["text"] for row in rows])
        with self.connection() as conn:
            for model, vectors in embedding_sets:
                conn.executemany(
                    "INSERT OR IGNORE INTO chunk_embeddings(chunk_id,model,vector,dimensions) VALUES(?,?,?,?)",
                    ((row["id"], model, pack_vector(vector), len(vector))
                     for row, vector in zip(rows, vectors)),
                )
            conn.commit()
            return len(rows)

    def _embedding_sets(self, texts: list[str]) -> list[tuple[str, list[list[float]]]]:
        batch = self.embedding_provider.embed_texts(texts)
        result = [(batch.model, batch.vectors)]
        if batch.model != LOCAL_MODEL_NAME and self.embedding_provider.local_fallback:
            result.append((LOCAL_MODEL_NAME, [embed_text(text) for text in texts]))
        return result

    def rebuild(self) -> None:
        with self.connection() as conn:
            conn.executescript("""
                DROP TABLE IF EXISTS chunks_fts;
                DROP TABLE IF EXISTS chunk_embeddings;
                DROP TABLE IF EXISTS chunks;
                DROP TABLE IF EXISTS documents;
                DROP TABLE IF EXISTS index_jobs;
                DROP TABLE IF EXISTS knowledge_state;
                DROP TABLE IF EXISTS knowledge_schema_version;
            """)
            conn.commit()
        self.initialize()

    def set_state(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO knowledge_state(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, encoded, utc_now()),
            )
            conn.commit()

    def enqueue(self, course_id: int, source_path: str, requested_hash: Optional[str], action: str) -> bool:
        path = normalize_path(source_path)
        with self.connection() as conn:
            cursor = conn.execute(
                "INSERT INTO index_jobs(course_id,source_path,normalized_path,requested_hash,action,status,available_at) "
                "VALUES(?,?,?,?,?,'pending',?) "
                "ON CONFLICT(course_id,normalized_path,requested_hash,action) DO UPDATE SET "
                "status='pending',available_at=excluded.available_at,claimed_at=NULL,completed_at=NULL,error=NULL "
                "WHERE index_jobs.status IN ('failed','stale')",
                (course_id, source_path, path, requested_hash or "", action, utc_now()),
            )
            conn.commit()
            return cursor.rowcount == 1

    def release_failed(self) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE index_jobs SET status='pending',available_at=?,claimed_at=NULL,error=NULL "
                "WHERE status='failed'",
                (utc_now(),),
            )
            conn.commit()
            return cursor.rowcount

    def recover_claims(self, older_than_seconds: int = 900) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat()
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE index_jobs SET status='pending',claimed_at=NULL WHERE status='running' AND claimed_at < ?",
                (cutoff,),
            )
            conn.commit()
            return cursor.rowcount

    def claim_job(self) -> Optional[dict[str, Any]]:
        now = utc_now()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM index_jobs WHERE status='pending' AND available_at <= ? ORDER BY id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            conn.execute(
                "UPDATE index_jobs SET status='running', attempts=attempts+1, claimed_at=? WHERE id=?",
                (now, row["id"]),
            )
            claimed = conn.execute("SELECT * FROM index_jobs WHERE id=?", (row["id"],)).fetchone()
            conn.commit()
            return dict(claimed)

    def finish_job(self, job_id: int, status: str = "completed", error: Optional[str] = None,
                   retry_at: Optional[str] = None) -> None:
        with self.connection() as conn:
            if retry_at:
                conn.execute(
                    "UPDATE index_jobs SET status='pending',available_at=?,claimed_at=NULL,error=? WHERE id=?",
                    (retry_at, error, job_id),
                )
            else:
                conn.execute(
                    "UPDATE index_jobs SET status=?,completed_at=?,error=? WHERE id=?",
                    (status, utc_now(), error, job_id),
                )
            conn.commit()

    def get_document_by_path(self, course_id: int, path: str) -> Optional[dict[str, Any]]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM documents WHERE course_id=? AND normalized_path=?",
                (course_id, normalize_path(path)),
            ).fetchone()
            return dict(row) if row else None

    def get_document(self, opaque_id: str, current_only: bool = True) -> Optional[dict[str, Any]]:
        sql = "SELECT * FROM documents WHERE id=?"
        if current_only:
            sql += " AND is_current=1"
        with self.connection() as conn:
            row = conn.execute(sql, (opaque_id,)).fetchone()
            return dict(row) if row else None

    def record_manifest_document(self, source: SourceMetadata, kind: str, status: str,
                                 error: Optional[str] = None,
                                 diagnostic_reason: Optional[str] = None) -> str:
        doc_id = document_id(source.course_id, source.source_path)
        with self.connection() as conn:
            conn.execute(
                """INSERT INTO documents(
                    id,course_id,course_name,course_short_name,source_path,normalized_path,source_url,
                    display_name,source_hash,mime_type,response_mime_type,document_kind,academic_year,is_current,status,error,diagnostic_reason)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                    course_name=excluded.course_name,course_short_name=excluded.course_short_name,
                    source_path=excluded.source_path,source_url=excluded.source_url,display_name=excluded.display_name,
                    source_hash=excluded.source_hash,mime_type=excluded.mime_type,response_mime_type=excluded.response_mime_type,
                    document_kind=excluded.document_kind,
                    academic_year=excluded.academic_year,is_current=1,status=excluded.status,error=excluded.error,
                    diagnostic_reason=excluded.diagnostic_reason""",
                (doc_id, source.course_id, source.course_name, source.course_short_name, source.source_path,
                 normalize_path(source.source_path), source.source_url, source.display_name, source.source_hash or "",
                 source.mime_type, source.response_mime_type, kind, source.academic_year, status, error, diagnostic_reason),
            )
            conn.commit()
        return doc_id

    def replace_document(self, source: SourceMetadata, kind: str, chunks: list[Chunk],
                         extractor_name: str, extractor_version: str = "1",
                         warnings: Optional[list[str]] = None, page_count: Optional[int] = None) -> str:
        doc_id = document_id(source.course_id, source.source_path)
        indexed_at = utc_now()
        embedding_sets = self._embedding_sets([chunk.text for chunk in chunks])
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            old_ids = [row[0] for row in conn.execute("SELECT id FROM chunks WHERE document_id=?", (doc_id,))]
            if old_ids:
                conn.executemany("DELETE FROM chunks_fts WHERE chunk_id=?", ((value,) for value in old_ids))
            conn.execute("DELETE FROM chunks WHERE document_id=?", (doc_id,))
            conn.execute(
                """INSERT INTO documents(
                    id,course_id,course_name,course_short_name,source_path,normalized_path,source_url,
                    display_name,source_hash,mime_type,response_mime_type,document_kind,academic_year,is_current,status,page_count,
                    extractor_name,extractor_version,indexed_at,error,diagnostic_reason,warnings_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ready',?,?,?,?,NULL,NULL,?)
                   ON CONFLICT(id) DO UPDATE SET
                    course_name=excluded.course_name,course_short_name=excluded.course_short_name,
                    source_path=excluded.source_path,source_url=excluded.source_url,display_name=excluded.display_name,
                    source_hash=excluded.source_hash,mime_type=excluded.mime_type,response_mime_type=excluded.response_mime_type,
                    document_kind=excluded.document_kind,
                    academic_year=excluded.academic_year,is_current=1,status='ready',page_count=excluded.page_count,
                    extractor_name=excluded.extractor_name,extractor_version=excluded.extractor_version,
                    indexed_at=excluded.indexed_at,error=NULL,diagnostic_reason=NULL,warnings_json=excluded.warnings_json""",
                (doc_id, source.course_id, source.course_name, source.course_short_name, source.source_path,
                 normalize_path(source.source_path), source.source_url, source.display_name, source.source_hash,
                 source.mime_type, source.response_mime_type, kind, source.academic_year, page_count, extractor_name, extractor_version,
                 indexed_at, json.dumps(warnings or [], ensure_ascii=False)),
            )
            for index, chunk in enumerate(chunks):
                conn.execute(
                    "INSERT INTO chunks(id,document_id,ordinal,locator_type,locator_start,locator_end,heading,text,normalized_text,content_hash,metadata_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (chunk.id, doc_id, chunk.ordinal, chunk.locator_type, chunk.locator_start,
                     chunk.locator_end, chunk.heading, chunk.text, chunk.normalized_text, chunk.content_hash,
                     json.dumps(chunk.metadata, ensure_ascii=False)),
                )
                conn.execute(
                    "INSERT INTO chunks_fts(chunk_id,text,normalized_text,heading,display_name,source_path,course_name) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (chunk.id, chunk.text, chunk.normalized_text, chunk.heading or "", source.display_name,
                     source.source_path, source.course_name),
                )
                for model, vectors in embedding_sets:
                    vector = vectors[index]
                    conn.execute(
                        "INSERT INTO chunk_embeddings(chunk_id,model,vector,dimensions) VALUES(?,?,?,?)",
                        (chunk.id, model, pack_vector(vector), len(vector)),
                    )
            conn.commit()
        return doc_id

    def mark_deleted(self, course_id: int, path: str) -> None:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM documents WHERE course_id=? AND normalized_path=?",
                (course_id, normalize_path(path)),
            ).fetchone()
            if row:
                ids = [item[0] for item in conn.execute("SELECT id FROM chunks WHERE document_id=?", (row["id"],))]
                conn.executemany("DELETE FROM chunks_fts WHERE chunk_id=?", ((value,) for value in ids))
                conn.execute("DELETE FROM chunks WHERE document_id=?", (row["id"],))
                conn.execute("UPDATE documents SET is_current=0,status='deleted' WHERE id=?", (row["id"],))
            conn.commit()

    def mark_missing(self, course_id: int, current_paths: set[str]) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute("SELECT source_path,normalized_path FROM documents WHERE course_id=? AND is_current=1", (course_id,)).fetchall()
        missing = [row["source_path"] for row in rows if row["normalized_path"] not in current_paths]
        for path in missing:
            self.mark_deleted(course_id, path)
        return missing

    def mark_error(self, source: SourceMetadata, kind: str, status: str, error: str,
                   diagnostic_reason: Optional[str] = None) -> None:
        concise = error.replace("\n", " ")[:1000]
        self.record_manifest_document(source, kind, status, concise, diagnostic_reason)

    @staticmethod
    def _metadata_rank(row: dict[str, Any], query: str) -> tuple[float, str]:
        """Score metadata signals without letting them replace source evidence."""
        name = search_normalize(row.get("display_name") or "")
        path = search_normalize(row.get("source_path") or "")
        query_normalized = search_normalize(query)
        tokens = [token for token in query_normalized.split() if token]
        bonus = 0.0
        if query_normalized and query_normalized in name:
            bonus += 0.16
        bonus += min(0.12, sum(0.04 for token in tokens if token in name))
        bonus += min(0.06, sum(0.02 for token in tokens if token in path))
        is_policy = any(keyword in name or keyword in path for keyword in POLICY_KEYWORDS)
        if is_policy:
            bonus += 0.08
        year = row.get("academic_year") or ""
        match = re.match(r"(20\d{2})", year)
        if match:
            age = max(0, datetime.now(timezone.utc).year - int(match.group(1)))
            bonus += 0.04 / (1 + age)
        return bonus, "course_policy" if is_policy else "general_material"

    @classmethod
    def _rank_metadata(cls, rows: list[dict[str, Any]], query: str, semantic: bool) -> list[dict[str, Any]]:
        for row in rows:
            bonus, priority = cls._metadata_rank(row, query)
            row["metadata_score"] = round(bonus, 6)
            row["document_priority"] = priority
            row["score"] = row["score"] + bonus if semantic else row["score"] - bonus
        rows.sort(key=lambda item: (-item["score"], item["document_id"], item["ordinal"]) if semantic
                  else (item["score"], item["document_id"], item["ordinal"]))
        return rows

    @staticmethod
    def _document_diversity(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            document_id = row["document_id"]
            if document_id in seen:
                continue
            seen.add(document_id)
            selected.append(row)
            if len(selected) >= limit:
                break
        return selected

    def _lexical_search(self, query: str, filters: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        normalized = search_normalize(query)
        tokens = [token for token in normalized.split() if token]
        if not tokens:
            return []
        # Quoting each token prevents FTS operators in model/user input from changing query semantics.
        match = " OR ".join('"' + token.replace('"', '""') + '"' for token in tokens)
        clauses = ["d.is_current=1", "d.status='ready'", "chunks_fts MATCH ?"]
        params: list[Any] = [match]
        course_ids = filters.get("course_ids")
        if course_ids:
            clauses.append(f"d.course_id IN ({','.join('?' for _ in course_ids)})")
            params.extend(course_ids)
        kinds = filters.get("document_kinds")
        if kinds:
            clauses.append(f"d.document_kind IN ({','.join('?' for _ in kinds)})")
            params.extend(kinds)
        if filters.get("academic_year"):
            clauses.append("d.academic_year=?")
            params.append(filters["academic_year"])
        if filters.get("folder_prefix"):
            clauses.append("d.normalized_path LIKE ? ESCAPE '\\'")
            prefix = normalize_path(filters["folder_prefix"]).replace("%", "\\%").replace("_", "\\_")
            params.append(prefix.rstrip("/") + "/%")
        params.append(limit)
        sql = f"""SELECT c.*, d.course_id,d.course_name,d.course_short_name,d.source_path,d.source_url,
                         d.display_name,d.source_hash,d.document_kind,d.academic_year,d.response_mime_type,d.indexed_at,
                         bm25(chunks_fts,0,1,0.6,0.3,0.2,0.2,0.2) AS score,
                         snippet(chunks_fts,1,'[',']',' … ',24) AS excerpt
                  FROM chunks_fts JOIN chunks c ON c.id=chunks_fts.chunk_id
                  JOIN documents d ON d.id=c.document_id
                  WHERE {' AND '.join(clauses)} ORDER BY score, d.id, c.ordinal LIMIT ?"""
        with self.connection() as conn:
            rows = [dict(row) for row in conn.execute(sql, params)]
        return self._rank_metadata(rows, query, semantic=False)

    def _semantic_search(self, query: str, filters: dict[str, Any], limit: int,
                         embedding: Any | None = None) -> list[dict[str, Any]]:
        embedding = embedding or self.embedding_provider.embed_texts([query])
        query_vector = embedding.vectors[0]
        clauses = ["d.is_current=1", "d.status='ready'", "e.model=?"]
        params: list[Any] = [embedding.model]
        course_ids = filters.get("course_ids")
        if course_ids:
            clauses.append(f"d.course_id IN ({','.join('?' for _ in course_ids)})")
            params.extend(course_ids)
        kinds = filters.get("document_kinds")
        if kinds:
            clauses.append(f"d.document_kind IN ({','.join('?' for _ in kinds)})")
            params.extend(kinds)
        if filters.get("academic_year"):
            clauses.append("d.academic_year=?")
            params.append(filters["academic_year"])
        if filters.get("folder_prefix"):
            clauses.append("d.normalized_path LIKE ? ESCAPE '\\'")
            prefix = normalize_path(filters["folder_prefix"]).replace("%", "\\%").replace("_", "\\_")
            params.append(prefix.rstrip("/") + "/%")
        with self.connection() as conn:
            rows = conn.execute(
                f"""SELECT c.*, d.course_id,d.course_name,d.course_short_name,d.source_path,d.source_url,
                           d.display_name,d.source_hash,d.document_kind,d.academic_year,d.response_mime_type,d.indexed_at,
                           e.vector,e.dimensions,e.model AS embedding_model
                    FROM chunk_embeddings e JOIN chunks c ON c.id=e.chunk_id
                    JOIN documents d ON d.id=c.document_id
                    WHERE {' AND '.join(clauses)}""",
                params,
            ).fetchall()
        scored = []
        for row in rows:
            score = cosine(query_vector, unpack_vector(row["vector"], row["dimensions"]))
            item = dict(row)
            item["score"] = score
            item["semantic_score"] = score
            item.pop("vector", None)
            item.pop("dimensions", None)
            scored.append(item)
        scored.sort(key=lambda item: (-item["score"], item["document_id"], item["ordinal"]))
        return self._rank_metadata(scored, query, semantic=True)[:limit]

    def search(self, query: str, filters: dict[str, Any], limit: int,
               mode: str = "lexical", lexical_weight: float = 0.55,
               semantic_weight: float = 0.45) -> list[dict[str, Any]]:
        """Search lexically, semantically, or with reciprocal-rank fusion."""
        if mode not in {"lexical", "semantic", "hybrid"}:
            raise ValueError("search mode must be lexical, semantic, or hybrid")
        if mode == "lexical":
            rows = self._lexical_search(query, filters, limit)
            for row in rows:
                row["retrieval_mode"] = "lexical"
            return self._document_diversity(rows, limit)
        if mode == "semantic":
            rows = self._semantic_search(query, filters, limit)
            for row in rows:
                row["retrieval_mode"] = "semantic"
            return self._document_diversity(rows, limit)

        candidate_limit = max(limit * 8, 40)
        lexical = self._lexical_search(query, filters, candidate_limit)
        semantic = self._semantic_search(query, filters, candidate_limit)
        merged: dict[str, dict[str, Any]] = {}
        for rank, row in enumerate(lexical, 1):
            item = merged.setdefault(row["id"], dict(row))
            item["lexical_score"] = row["score"]
            item["_lexical_rank"] = rank
        for rank, row in enumerate(semantic, 1):
            item = merged.setdefault(row["id"], dict(row))
            item.update({key: value for key, value in row.items() if key not in {"score", "semantic_score"}})
            item["semantic_score"] = row["semantic_score"]
            item["_semantic_rank"] = rank
        for item in merged.values():
            lexical_rank = item.get("_lexical_rank")
            semantic_rank = item.get("_semantic_rank")
            item["score"] = (
                lexical_weight / (60 + lexical_rank) if lexical_rank else 0.0
            ) + (
                semantic_weight / (60 + semantic_rank) if semantic_rank else 0.0
            )
            item["retrieval_mode"] = "hybrid"
            item.setdefault("lexical_score", None)
            item.setdefault("semantic_score", None)
            item.pop("_lexical_rank", None)
            item.pop("_semantic_rank", None)
        results = sorted(merged.values(), key=lambda item: (-item["score"], item["document_id"], item["ordinal"]))
        return self._document_diversity(results, limit)

    def read_chunks(self, opaque_id: str, locators: list[dict[str, Optional[str]]],
                    include_neighbors: bool) -> list[dict[str, Any]]:
        with self.connection() as conn:
            doc = conn.execute("SELECT * FROM documents WHERE id=? AND is_current=1 AND status='ready'", (opaque_id,)).fetchone()
            if not doc:
                return []
            if not locators:
                rows = conn.execute("SELECT * FROM chunks WHERE document_id=? ORDER BY ordinal", (opaque_id,)).fetchall()
                return [dict(row) for row in rows]
            ordinals: set[int] = set()
            for locator in locators:
                kind, start, end = locator["type"], locator["start"], locator.get("end") or locator["start"]
                rows = conn.execute(
                    "SELECT ordinal,locator_start FROM chunks WHERE document_id=? AND locator_type=?",
                    (opaque_id, kind),
                ).fetchall()
                for row in rows:
                    value = row["locator_start"] or ""
                    try:
                        selected = int(start) <= int(value) <= int(end)
                    except (ValueError, TypeError):
                        selected = start <= value <= end
                    if selected:
                        ordinals.add(row["ordinal"])
            if include_neighbors:
                ordinals |= {value + delta for value in list(ordinals) for delta in (-1, 1) if value + delta >= 0}
            if not ordinals:
                return []
            placeholders = ",".join("?" for _ in ordinals)
            rows = conn.execute(
                f"SELECT * FROM chunks WHERE document_id=? AND ordinal IN ({placeholders}) ORDER BY ordinal",
                [opaque_id, *sorted(ordinals)],
            ).fetchall()
            return [dict(row) for row in rows]

    def list_materials(self, course_id: int, limit: int, cursor: Optional[str] = None,
                       path_prefix: Optional[str] = None, kinds: Optional[list[str]] = None,
                       academic_year: Optional[str] = None, changed_since: Optional[str] = None) -> list[dict[str, Any]]:
        clauses = ["course_id=?", "is_current=1"]
        params: list[Any] = [course_id]
        if cursor:
            clauses.append("id>?")
            params.append(cursor)
        if path_prefix:
            clauses.append("normalized_path LIKE ?")
            params.append(normalize_path(path_prefix).rstrip("/") + "/%")
        if kinds:
            clauses.append(f"document_kind IN ({','.join('?' for _ in kinds)})")
            params.extend(kinds)
        if academic_year:
            clauses.append("academic_year=?")
            params.append(academic_year)
        if changed_since:
            clauses.append("indexed_at>=?")
            params.append(changed_since)
        params.append(limit)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM documents WHERE {' AND '.join(clauses)} ORDER BY id LIMIT ?", params
            ).fetchall()
            return [dict(row) for row in rows]

    def job_counts(self, course_id: Optional[int] = None) -> dict[str, int]:
        where, params = (" WHERE course_id=?", [course_id]) if course_id is not None else ("", [])
        with self.connection() as conn:
            rows = conn.execute(f"SELECT status,count(*) count FROM index_jobs{where} GROUP BY status", params).fetchall()
            return {row["status"]: row["count"] for row in rows}

    def status(self, course_ids: Optional[list[int]] = None) -> dict[str, Any]:
        clauses, params = [], []
        if course_ids is not None:
            if not course_ids:
                return {"coverage": [], "documents": [], "jobs": [], "state": {},
                        "unsupported_documents": [], "failed_documents": []}
            clauses.append(f"course_id IN ({','.join('?' for _ in course_ids)})")
            params.extend(course_ids)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        current_where = (where + " AND " if where else " WHERE ") + "is_current=1"
        with self.connection() as conn:
            docs = [dict(row) for row in conn.execute(
                f"SELECT course_id,status,document_kind,count(*) count,max(indexed_at) latest_indexed_at "
                f"FROM documents{current_where} GROUP BY course_id,status,document_kind",
                params,
            )]
            # Keep a compact per-course summary alongside the grouped rows. These
            # counts describe the current manifest, not historical deleted records.
            coverage = [dict(row) for row in conn.execute(
                f"""SELECT course_id,
                           count(*) AS discovered_documents,
                           sum(CASE WHEN status='ready' THEN 1 ELSE 0 END) AS indexed_documents,
                           sum(CASE WHEN status IN ('pending','running') THEN 1 ELSE 0 END) AS pending_documents,
                           sum(CASE WHEN status IN ('failed','skipped_limit') THEN 1 ELSE 0 END) AS failed_documents,
                           sum(CASE WHEN status='unsupported' THEN 1 ELSE 0 END) AS unsupported_documents,
                           sum(CASE WHEN status='external' THEN 1 ELSE 0 END) AS external_documents
                    FROM documents
                    {current_where}
                    GROUP BY course_id""",
                params,
            )]
            reason_where = (where + " AND " if where else "WHERE ") + "is_current=1 AND status='unsupported'"
            unsupported_reasons = [dict(row) for row in conn.execute(
                f"""SELECT course_id,coalesce(diagnostic_reason,'unknown') AS reason,count(*) AS count
                    FROM documents {reason_where}
                    GROUP BY course_id,coalesce(diagnostic_reason,'unknown')
                    ORDER BY course_id,count(*) DESC""",
                params,
            )]
            diagnostic_where = (where + " AND " if where else "WHERE ") + \
                "is_current=1 AND status IN ('unsupported','failed','skipped_limit')"
            diagnostics = [dict(row) for row in conn.execute(
                f"""SELECT course_id,status,document_kind,display_name,source_path,
                           diagnostic_reason,error,mime_type,response_mime_type
                    FROM documents {diagnostic_where}
                    ORDER BY course_id,status,source_path LIMIT 500""",
                params,
            )]
            reasons_by_course: dict[int, dict[str, int]] = {}
            for row in unsupported_reasons:
                reasons_by_course.setdefault(row["course_id"], {})[row["reason"]] = row["count"]
            for row in coverage:
                row["indexed_documents"] = row["indexed_documents"] or 0
                row["pending_documents"] = row["pending_documents"] or 0
                row["failed_documents"] = row["failed_documents"] or 0
                row["unsupported_documents"] = row["unsupported_documents"] or 0
                row["external_documents"] = row["external_documents"] or 0
                row["supported_documents"] = (
                    row["discovered_documents"] - row["unsupported_documents"] - row["external_documents"]
                )
                row["unsupported_reasons"] = reasons_by_course.get(row["course_id"], {})
            job_where = where
            jobs = [dict(row) for row in conn.execute(
                f"SELECT course_id,status,count(*) count FROM index_jobs{job_where} GROUP BY course_id,status", params
            )]
            state = {row["key"]: row["value"] for row in conn.execute("SELECT key,value FROM knowledge_state")}
        unsupported_documents = [row for row in diagnostics if row["status"] == "unsupported"]
        failed_documents = [row for row in diagnostics if row["status"] in {"failed", "skipped_limit"}]
        return {
            "coverage": coverage,
            "documents": docs,
            "jobs": jobs,
            "state": state,
            "unsupported_documents": unsupported_documents,
            "failed_documents": failed_documents,
            "diagnostics_truncated": len(diagnostics) >= 500,
        }

    def list_documents_admin(self, course_id: Optional[int] = None, status: Optional[str] = None,
                             query: Optional[str] = None, limit: int = 200) -> list[dict[str, Any]]:
        clauses, params = [], []
        if course_id is not None:
            clauses.append("d.course_id=?")
            params.append(course_id)
        if status:
            clauses.append("d.status=?")
            params.append(status)
        if query:
            clauses.append("(d.display_name LIKE ? OR d.source_path LIKE ?)")
            pattern = f"%{query}%"
            params.extend([pattern, pattern])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connection() as conn:
            rows = conn.execute(
                f"""SELECT d.*,count(c.id) chunk_count,count(DISTINCT e.chunk_id) embedding_count
                    FROM documents d LEFT JOIN chunks c ON c.document_id=d.id
                    LEFT JOIN chunk_embeddings e ON e.chunk_id=c.id
                    {where} GROUP BY d.id ORDER BY coalesce(d.indexed_at,'') DESC,d.source_path LIMIT ?""",
                [*params, min(max(1, limit), 500)],
            ).fetchall()
            return [dict(row) for row in rows]

    def list_jobs_admin(self, course_id: Optional[int] = None, limit: int = 200) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if course_id is not None:
            where = " WHERE course_id=?"
            params.append(course_id)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM index_jobs{where} ORDER BY CASE status WHEN 'failed' THEN 0 WHEN 'running' THEN 1 ELSE 2 END,id DESC LIMIT ?",
                [*params, min(max(1, limit), 500)],
            ).fetchall()
            return [dict(row) for row in rows]

    def embedding_status(self, course_ids: Optional[list[int]] = None) -> dict[str, Any]:
        clauses, params = ["d.is_current=1"], []
        if course_ids is not None:
            if not course_ids:
                return {"chunks": 0, "embedded_chunks": 0,
                        "model": self.embedding_provider.configured_model,
                        "models": [], "backend": self.embedding_provider.backend,
                        "hosted_available": self.embedding_provider.can_use_hosted}
            clauses.append(f"d.course_id IN ({','.join('?' for _ in course_ids)})")
            params.extend(course_ids)
        with self.connection() as conn:
            row = conn.execute(
                f"""SELECT count(DISTINCT c.id) chunks,count(DISTINCT e.chunk_id) embedded
                    FROM chunks c JOIN documents d ON d.id=c.document_id
                    LEFT JOIN chunk_embeddings e ON e.chunk_id=c.id
                    WHERE {' AND '.join(clauses)}""", params,
            ).fetchone()
        with self.connection() as conn:
            models = [dict(item) for item in conn.execute(
                f"""SELECT e.model,count(*) count
                    FROM chunks c JOIN documents d ON d.id=c.document_id
                    JOIN chunk_embeddings e ON e.chunk_id=c.id
                    WHERE {' AND '.join(clauses)} GROUP BY e.model ORDER BY count(*) DESC""", params,
            )]
        return {
            "chunks": row["chunks"], "embedded_chunks": row["embedded"],
            "model": self.embedding_provider.configured_model, "models": models,
            "backend": self.embedding_provider.backend,
            "hosted_available": self.embedding_provider.can_use_hosted,
        }

    def rebuild_fts(self) -> int:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM chunks_fts")
            conn.execute("""INSERT INTO chunks_fts(chunk_id,text,normalized_text,heading,display_name,source_path,course_name)
                            SELECT c.id,c.text,c.normalized_text,coalesce(c.heading,''),d.display_name,d.source_path,d.course_name
                            FROM chunks c JOIN documents d ON d.id=c.document_id
                            WHERE d.is_current=1 AND d.status='ready'""")
            count = conn.execute("SELECT count(*) FROM chunks_fts").fetchone()[0]
            conn.commit()
            return count
