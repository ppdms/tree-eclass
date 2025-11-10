"""
Handles all database interactions for the e-class checker application.
"""
import logging
import sqlite3
import pickle
import os
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from app.services.tree_builder import Node, File

DB_FILE = os.getenv("DB_FILE", "eclass.db")

class DatabaseManager:
    """Manages all SQLite database operations."""

    def __init__(self, db_file=DB_FILE):
        try:
            self.conn = sqlite3.connect(db_file)
            self._create_tables()
            logging.debug(f"Database connection established: {db_file}")
        except Exception as e:
            logging.error(f"Failed to initialize database: {e}", exc_info=True)
            raise

    def _create_tables(self):
        """Creates all necessary tables if they don't already exist."""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS credentials (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                password TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_data (
                key TEXT PRIMARY KEY,
                value BLOB
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS courses (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                download_folder TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                local_path TEXT NOT NULL,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES nodes (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                md5_hash TEXT,
                etag TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                change_no TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                message TEXT,
                changes_count INTEGER DEFAULT 0,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                UNIQUE(course_id, change_no)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_record_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_record_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                FOREIGN KEY (change_record_id) REFERENCES change_records (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_config (
                id INTEGER PRIMARY KEY,
                smtp_server TEXT,
                smtp_port INTEGER,
                smtp_username TEXT,
                smtp_password TEXT,
                from_email TEXT,
                to_email TEXT,
                use_tls INTEGER DEFAULT 1
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS preferences (
                id INTEGER PRIMARY KEY,
                check_interval_minutes INTEGER DEFAULT 60,
                max_concurrent_downloads INTEGER DEFAULT 3,
                request_timeout_seconds INTEGER DEFAULT 30,
                retry_attempts INTEGER DEFAULT 3,
                notification_enabled INTEGER DEFAULT 1,
                notification_on_error INTEGER DEFAULT 1
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS check_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                log_type TEXT NOT NULL,
                course_id INTEGER,
                message TEXT,
                status TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE SET NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS check_status (
                id INTEGER PRIMARY KEY,
                is_checking INTEGER DEFAULT 0,
                started_at DATETIME,
                current_course_id INTEGER,
                FOREIGN KEY (current_course_id) REFERENCES courses (id) ON DELETE SET NULL
            )
        """)
        self.conn.commit()

    # --- Credentials methods ---
    def save_credentials(self, username: str, password: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO credentials (id, username, password) VALUES (1, ?, ?)", (username, password))
            self.conn.commit()
            logging.debug("Credentials saved successfully")
        except Exception as e:
            logging.error(f"Failed to save credentials: {e}", exc_info=True)
            raise

    def get_credentials(self) -> Optional[Tuple[str, str]]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT username, password FROM credentials WHERE id = 1")
            row = cursor.fetchone()
            return row if row else None
        except Exception as e:
            logging.error(f"Failed to get credentials: {e}", exc_info=True)
            raise

    # --- Email configuration methods ---
    def save_email_config(self, smtp_server: str, smtp_port: int, smtp_username: str, 
                         smtp_password: str, from_email: str, to_email: str, use_tls: bool = True):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO email_config 
                (id, smtp_server, smtp_port, smtp_username, smtp_password, from_email, to_email, use_tls) 
                VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            """, (smtp_server, smtp_port, smtp_username, smtp_password, from_email, to_email, int(use_tls)))
            self.conn.commit()
            logging.debug("Email configuration saved successfully")
        except Exception as e:
            logging.error(f"Failed to save email configuration: {e}", exc_info=True)
            raise

    def get_email_config(self) -> Optional[Dict[str, any]]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT smtp_server, smtp_port, smtp_username, smtp_password, from_email, to_email, use_tls 
                FROM email_config WHERE id = 1
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'smtp_server': row[0],
                    'smtp_port': row[1],
                    'smtp_username': row[2],
                    'smtp_password': row[3],
                    'from_email': row[4],
                    'to_email': row[5],
                    'use_tls': bool(row[6])
                }
            return None
        except Exception as e:
            logging.error(f"Failed to get email configuration: {e}", exc_info=True)
            raise

    # --- Preferences methods ---
    def save_preferences(self, check_interval_minutes: int = 60, 
                        max_concurrent_downloads: int = 3,
                        request_timeout_seconds: int = 30,
                        retry_attempts: int = 3,
                        notification_enabled: bool = True,
                        notification_on_error: bool = True):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO preferences 
                (id, check_interval_minutes, max_concurrent_downloads, request_timeout_seconds, 
                 retry_attempts, notification_enabled, notification_on_error) 
                VALUES (1, ?, ?, ?, ?, ?, ?)
            """, (check_interval_minutes, max_concurrent_downloads, request_timeout_seconds, 
                  retry_attempts, int(notification_enabled), int(notification_on_error)))
            self.conn.commit()
            logging.debug("Preferences saved successfully")
        except Exception as e:
            logging.error(f"Failed to save preferences: {e}", exc_info=True)
            raise

    def get_preferences(self) -> Dict[str, any]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT check_interval_minutes, max_concurrent_downloads, request_timeout_seconds,
                       retry_attempts, notification_enabled, notification_on_error
                FROM preferences WHERE id = 1
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'check_interval_minutes': row[0],
                    'max_concurrent_downloads': row[1],
                    'request_timeout_seconds': row[2],
                    'retry_attempts': row[3],
                    'notification_enabled': bool(row[4]),
                    'notification_on_error': bool(row[5])
                }
            # Return defaults if no preferences set
            return {
                'check_interval_minutes': 60,
                'max_concurrent_downloads': 3,
                'request_timeout_seconds': 30,
                'retry_attempts': 3,
                'notification_enabled': True,
                'notification_on_error': True
            }
        except Exception as e:
            logging.error(f"Failed to get preferences: {e}", exc_info=True)
            raise

    # --- Cookie methods ---
    def save_cookie_jar(self, cookie_jar):
        pickled_jar = pickle.dumps(cookie_jar)
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO app_data (key, value) VALUES ('session_cookie', ?)", (pickled_jar,))
        self.conn.commit()

    def load_cookie_jar(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM app_data WHERE key = 'session_cookie'")
        row = cursor.fetchone()
        if row and row[0]:
            return pickle.loads(row[0])
        return None

    # --- Course methods ---
    def save_course(self, course_id: int, name: str, download_folder: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO courses (id, name, download_folder) VALUES (?, ?, ?)", (course_id, name, download_folder))
            self.conn.commit()
            logging.debug(f"Saved course: {name} (ID: {course_id})")
        except Exception as e:
            logging.error(f"Failed to save course {name} (ID: {course_id}): {e}", exc_info=True)
            raise

    def get_courses(self) -> List[Dict]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, name, download_folder FROM courses")
            rows = cursor.fetchall()
            return [{"id": row[0], "name": row[1], "download_folder": row[2]} for row in rows]
        except Exception as e:
            logging.error(f"Failed to get courses: {e}", exc_info=True)
            raise

    def delete_course(self, course_id: int):
        """Delete a course and all its associated data."""
        try:
            cursor = self.conn.cursor()
            # Explicitly delete check logs for this course
            cursor.execute("DELETE FROM check_logs WHERE course_id = ?", (course_id,))
            # Delete the course (this will cascade delete nodes, files, change_history, change_records, etc.)
            cursor.execute("DELETE FROM courses WHERE id = ?", (course_id,))
            self.conn.commit()
            logging.debug(f"Deleted course ID: {course_id} and all associated data")
        except Exception as e:
            logging.error(f"Failed to delete course {course_id}: {e}", exc_info=True)
            raise

    def reset_course_data(self, course_id: int):
        """Reset all data for a course (tree and change history) without deleting the course itself."""
        try:
            cursor = self.conn.cursor()
            # Delete the file tree
            cursor.execute("DELETE FROM nodes WHERE course_id = ?", (course_id,))
            # Delete change history
            cursor.execute("DELETE FROM change_history WHERE course_id = ?", (course_id,))
            # Delete change records (this will cascade delete change_record_items)
            cursor.execute("DELETE FROM change_records WHERE course_id = ?", (course_id,))
            # Delete check logs for this course
            cursor.execute("DELETE FROM check_logs WHERE course_id = ?", (course_id,))
            self.conn.commit()
            logging.debug(f"Reset data for course ID: {course_id}")
        except Exception as e:
            logging.error(f"Failed to reset course data for {course_id}: {e}", exc_info=True)
            raise

    # --- Tree methods ---
    def save_tree(self, course_id: int, root_node: Node):
        """Saves an entire Node tree to the database for a given course."""
        cursor = self.conn.cursor()
        
        def _save_recursive(node: Node, parent_db_id: Optional[int]):
            cursor.execute(
                "INSERT INTO nodes (course_id, parent_id, name, url, local_path) VALUES (?, ?, ?, ?, ?)",
                (course_id, parent_db_id, node.name, node.url, node.local_path)
            )
            current_db_id = cursor.lastrowid

            for file in node.files:
                cursor.execute(
                    "INSERT INTO files (node_id, url, name, md5_hash, etag) VALUES (?, ?, ?, ?, ?)",
                    (current_db_id, file.url, file.name, file.md5_hash, file.etag)
                )

            for child_node in node.children:
                _save_recursive(child_node, current_db_id)

        try:
            # Clear old data for this course
            cursor.execute("DELETE FROM nodes WHERE course_id = ?", (course_id,))
            # Save new tree
            _save_recursive(root_node, None)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def load_tree(self, course_id: int) -> Optional[Node]:
        """Loads a Node tree from the database for a given course."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, parent_id, name, url, local_path FROM nodes WHERE course_id = ?", (course_id,))
        nodes_data = cursor.fetchall()

        if not nodes_data:
            return None

        node_objects: Dict[int, Node] = {}
        root_node = None

        for db_id, parent_id, name, url, local_path in nodes_data:
            node = Node(name=name, url=url, local_path=local_path)
            node_objects[db_id] = node
            if parent_id is None:
                root_node = node

        if root_node is None:
            # This case should ideally not be reached if data exists
            return None

        for db_id, parent_id, _, _, _ in nodes_data:
            if parent_id and parent_id in node_objects:
                parent_node = node_objects[parent_id]
                child_node = node_objects[db_id]
                parent_node.children.append(child_node)

        cursor.execute("SELECT node_id, url, name, md5_hash, etag FROM files WHERE node_id IN ({seq})".format(
            seq=','.join(['?']*len(node_objects))), list(node_objects.keys()))
        
        for node_id, url, name, md5_hash, etag in cursor.fetchall():
            if node_id in node_objects:
                node_objects[node_id].files.append(File(url=url, name=name, md5_hash=md5_hash, etag=etag))

        return root_node

    # --- Change record methods ---
    def create_change_record(self, course_id: int, changes: List[str]) -> int:
        """Create a change record for a course with the given changes.

        change_no is computed as an RFC 3339 timestamp and must be unique per course.
        If collision occurs, increment by microseconds until unique.

        Returns the database id of the created change_record.
        """
        try:
            cursor = self.conn.cursor()

            # Count change types
            added_count = 0
            deleted_count = 0
            modified_count = 0
            
            for change in changes:
                if change.startswith("Added file:") or change.startswith("Added directory:"):
                    added_count += 1
                elif change.startswith("Deleted file:") or change.startswith("Deleted directory:"):
                    deleted_count += 1
                elif change.startswith("Modified file:"):
                    modified_count += 1

            # Generate change record message with all counts
            summary = f"+ {added_count} − {deleted_count} ~ {modified_count}"

            # Compute initial change_no based on current time
            base_dt = datetime.now(ZoneInfo("Europe/Athens"))
            change_no = base_dt.replace(tzinfo=None).isoformat()

            # Try inserting with collision handling on UNIQUE(course_id, change_no)
            microsecond_offset = 1
            while True:
                try:
                    cursor.execute(
                        "INSERT INTO change_records (course_id, change_no, message, changes_count, timestamp) VALUES (?, ?, ?, ?, datetime('now'))",
                        (course_id, change_no, summary, len(changes))
                    )
                    change_record_id = cursor.lastrowid
                    break
                except sqlite3.IntegrityError:
                    # collision on (course_id, change_no) - add microseconds and retry
                    next_dt = base_dt + timedelta(microseconds=microsecond_offset)
                    change_no = next_dt.replace(tzinfo=None).isoformat()
                    microsecond_offset += 1

            # Add each change to the change record
            for change in changes:
                # Parse change type and file path
                if change.startswith("Added file:"):
                    change_type = "added_file"
                    file_path = change.replace("Added file:", "").strip()
                elif change.startswith("Deleted file:"):
                    change_type = "deleted_file"
                    file_path = change.replace("Deleted file:", "").strip()
                elif change.startswith("Modified file:"):
                    change_type = "modified_file"
                    file_path = change.replace("Modified file:", "").strip()
                elif change.startswith("Added directory:"):
                    change_type = "added_directory"
                    file_path = change.replace("Added directory:", "").strip()
                elif change.startswith("Deleted directory:"):
                    change_type = "deleted_directory"
                    file_path = change.replace("Deleted directory:", "").strip()
                else:
                    change_type = "unknown"
                    file_path = change

                cursor.execute(
                    "INSERT INTO change_record_items (change_record_id, change_type, file_path) VALUES (?, ?, ?)",
                    (change_record_id, change_type, file_path)
                )

            logging.debug(f"Created change record {change_record_id} (change_no={change_no}) for course {course_id} with {len(changes)} changes")
            return change_record_id
        except Exception as e:
            logging.error(f"Failed to create change record for course {course_id}: {e}", exc_info=True)
            raise

    def get_change_record_by_course_and_no(self, course_id: int, change_no: str) -> Optional[Dict]:
        """Retrieve a single change record by course_id and change_no."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT cr.id, cr.course_id, c.name, cr.change_no, cr.timestamp, cr.message, cr.changes_count 
                FROM change_records cr
                JOIN courses c ON cr.course_id = c.id
                WHERE cr.course_id = ? AND cr.change_no = ?
            """, (course_id, change_no))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "course_id": row[1],
                "course_name": row[2],
                "change_no": row[3],
                "timestamp": row[4],
                "message": row[5],
                "changes_count": row[6]
            }
        except Exception as e:
            logging.error(f"Failed to get change record for course {course_id} change_no {change_no}: {e}", exc_info=True)
            raise

    def log_changes(self, course_id: int, changes: List[str]):
        """Log file changes to the database (calls create_change_record)."""
        self.create_change_record(course_id, changes)

    def get_change_records(self, course_id: Optional[int] = None, limit: int = 100, 
                    start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """Get change records, optionally filtered by course and date range."""
        try:
            cursor = self.conn.cursor()
            
            # Build query dynamically based on filters
            query = """
                SELECT c.id, c.course_id, co.name, c.change_no, c.timestamp, c.message, c.changes_count
                FROM change_records c
                JOIN courses co ON c.course_id = co.id
                WHERE 1=1
            """
            params = []
            
            if course_id:
                query += " AND c.course_id = ?"
                params.append(course_id)
            
            if start_date:
                query += " AND DATE(c.timestamp) >= DATE(?)"
                params.append(start_date)
            
            if end_date:
                query += " AND DATE(c.timestamp) <= DATE(?)"
                params.append(end_date)
            
            query += " ORDER BY c.timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "course_id": row[1],
                    "course_name": row[2],
                    "change_no": row[3],
                    "timestamp": row[4],
                    "message": row[5],
                    "changes_count": row[6]
                }
                for row in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get change records: {e}", exc_info=True)
            raise

    def get_change_record_items(self, change_record_id: int) -> List[Dict]:
        """Get all changes for a specific change record."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, change_type, file_path
                FROM change_record_items
                WHERE change_record_id = ?
                ORDER BY id
            """, (change_record_id,))
            
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "change_type": row[1],
                    "file_path": row[2]
                }
                for row in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get change record items for change record {change_record_id}: {e}", exc_info=True)
            raise

    # --- Check logs and status methods ---
    def log_check_event(self, log_type: str, message: str, course_id: Optional[int] = None, status: str = "info"):
        """Log a check event (check start, check end, email sent, etc.)."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO check_logs (log_type, course_id, message, status)
                VALUES (?, ?, ?, ?)
            """, (log_type, course_id, message, status))
            self.conn.commit()
            logging.debug(f"Logged check event: {log_type} - {message}")
        except Exception as e:
            logging.error(f"Failed to log check event: {e}", exc_info=True)
            raise

    def get_check_logs(self, limit: int = 100, log_type: Optional[str] = None) -> List[Dict]:
        """Get check logs, optionally filtered by log_type."""
        try:
            cursor = self.conn.cursor()
            if log_type:
                cursor.execute("""
                    SELECT cl.id, cl.timestamp, cl.log_type, cl.course_id, c.name as course_name, cl.message, cl.status
                    FROM check_logs cl
                    LEFT JOIN courses c ON cl.course_id = c.id
                    WHERE cl.log_type = ?
                    ORDER BY cl.timestamp DESC
                    LIMIT ?
                """, (log_type, limit))
            else:
                cursor.execute("""
                    SELECT cl.id, cl.timestamp, cl.log_type, cl.course_id, c.name as course_name, cl.message, cl.status
                    FROM check_logs cl
                    LEFT JOIN courses c ON cl.course_id = c.id
                    ORDER BY cl.timestamp DESC
                    LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "timestamp": row[1],
                    "log_type": row[2],
                    "course_id": row[3],
                    "course_name": row[4],
                    "message": row[5],
                    "status": row[6]
                }
                for row in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get check logs: {e}", exc_info=True)
            raise

    def set_check_status(self, is_checking: bool, course_id: Optional[int] = None):
        """Set the global check status."""
        try:
            cursor = self.conn.cursor()
            if is_checking:
                cursor.execute("""
                    INSERT OR REPLACE INTO check_status (id, is_checking, started_at, current_course_id)
                    VALUES (1, 1, CURRENT_TIMESTAMP, ?)
                """, (course_id,))
            else:
                cursor.execute("""
                    INSERT OR REPLACE INTO check_status (id, is_checking, started_at, current_course_id)
                    VALUES (1, 0, NULL, NULL)
                """)
            self.conn.commit()
            logging.debug(f"Set check status: is_checking={is_checking}, course_id={course_id}")
        except Exception as e:
            logging.error(f"Failed to set check status: {e}", exc_info=True)
            raise

    def get_check_status(self) -> Dict:
        """Get the current check status."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT cs.is_checking, cs.started_at, cs.current_course_id, c.name as course_name
                FROM check_status cs
                LEFT JOIN courses c ON cs.current_course_id = c.id
                WHERE cs.id = 1
            """)
            row = cursor.fetchone()
            if row:
                return {
                    "is_checking": bool(row[0]),
                    "started_at": row[1],
                    "current_course_id": row[2],
                    "course_name": row[3]
                }
            else:
                return {
                    "is_checking": False,
                    "started_at": None,
                    "current_course_id": None,
                    "course_name": None
                }
        except Exception as e:
            logging.error(f"Failed to get check status: {e}", exc_info=True)
            raise

    def close(self):
        """Closes the database connection."""
        self.conn.close()