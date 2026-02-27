"""
Handles all database interactions for the e-class checker application.
"""
import json
import logging
import sqlite3
import os
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from app.services.tree_builder import Node, File

DB_FILE = os.getenv("DB_FILE", "eclass.db")
SCHEMA_VERSION = 7  # Current schema version

class DatabaseManager:
    """Manages all SQLite database operations."""

    def __init__(self, db_file=DB_FILE):
        try:
            self.conn = sqlite3.connect(db_file)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self._create_tables()
            self._run_migrations()
            logging.debug(f"Database connection established: {db_file}")
        except Exception as e:
            logging.error(f"Failed to initialize database: {e}", exc_info=True)
            raise

    def _create_tables(self):
        """Creates all necessary tables if they don't already exist."""
        cursor = self.conn.cursor()
        
        # Schema version tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
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
                webdav_folder TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                local_path TEXT NOT NULL,  -- WebDAV path (kept as local_path for backward compatibility)
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
            CREATE TABLE IF NOT EXISTS webhook_config (
                id INTEGER PRIMARY KEY,
                webhook_url TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS webdav_config (
                id INTEGER PRIMARY KEY,
                hostname TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                disable_check INTEGER DEFAULT 0,
                timeout INTEGER DEFAULT 30
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                announcement_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                description TEXT,
                pub_date DATETIME,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                UNIQUE(course_id, announcement_id)
            )
        """)
        self.conn.commit()

    def _get_schema_version(self) -> int:
        """Get current schema version from database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT version FROM schema_version WHERE id = 1")
            result = cursor.fetchone()
            return result[0] if result else 1  # Default to version 1 if not set
        except sqlite3.OperationalError:
            # Table doesn't exist yet, must be version 1
            return 1

    def _set_schema_version(self, version: int):
        """Update schema version in database."""
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO schema_version (id, version) VALUES (1, ?)", (version,))
        self.conn.commit()
        logging.info(f"Schema version updated to {version}")

    def _run_migrations(self):
        """Run database migrations to bring schema up to current version."""
        current_version = self._get_schema_version()
        
        if current_version == SCHEMA_VERSION:
            logging.debug(f"Database schema is up to date (version {SCHEMA_VERSION})")
            return
        
        logging.info(f"Running migrations from version {current_version} to {SCHEMA_VERSION}")
        
        # Run migrations sequentially
        if current_version < 2:
            self._migration_1_to_2()
            self._set_schema_version(2)
        
        if current_version < 3:
            self._migration_2_to_3()
            self._set_schema_version(3)
        
        if current_version < 4:
            self._migration_3_to_4()
            self._set_schema_version(4)
        
        if current_version < 5:
            self._migration_4_to_5()
            self._set_schema_version(5)
        
        if current_version < 6:
            self._migration_5_to_6()
            self._set_schema_version(6)
        
        if current_version < 7:
            self._migration_6_to_7()
            self._set_schema_version(7)
        
        logging.info("All migrations completed successfully")

    def _migration_1_to_2(self):
        """Migration: Add webdav_folder column to courses table."""
        logging.info("Running migration 1 -> 2: Adding webdav_folder column")
        cursor = self.conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(courses)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'webdav_folder' not in columns:
            cursor.execute("ALTER TABLE courses ADD COLUMN webdav_folder TEXT")
            self.conn.commit()
            logging.info("Added webdav_folder column to courses table")
        else:
            logging.debug("webdav_folder column already exists, skipping")

    def _migration_2_to_3(self):
        """Migration: Remove download_folder column, make webdav_folder required."""
        logging.info("Running migration 2 -> 3: Removing download_folder column")
        cursor = self.conn.cursor()
        
        # Check current table structure
        cursor.execute("PRAGMA table_info(courses)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'download_folder' in columns:
            # SQLite doesn't support DROP COLUMN, so we need to recreate the table
            logging.info("Recreating courses table without download_folder")
            
            # Copy data to temporary table, using webdav_folder if available, else download_folder
            cursor.execute("""
                CREATE TABLE courses_new (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    webdav_folder TEXT NOT NULL
                )
            """)
            
            cursor.execute("""
                INSERT INTO courses_new (id, name, webdav_folder)
                SELECT id, name, COALESCE(webdav_folder, download_folder)
                FROM courses
            """)
            
            # Drop old table and rename new one
            cursor.execute("DROP TABLE courses")
            cursor.execute("ALTER TABLE courses_new RENAME TO courses")
            
            self.conn.commit()
            logging.info("Successfully removed download_folder column from courses table")
        else:
            logging.debug("download_folder column does not exist, skipping")

    def _migration_3_to_4(self):
        """Migration: Add last_updated column to files table."""
        logging.info("Running migration 3 -> 4: Adding last_updated column to files table")
        cursor = self.conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(files)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'last_updated' not in columns:
            cursor.execute("ALTER TABLE files ADD COLUMN last_updated DATETIME")
            self.conn.commit()
            logging.info("Added last_updated column to files table")
        else:
            logging.debug("last_updated column already exists, skipping")

    def _migration_4_to_5(self):
        """Migration: Add announcements table."""
        logging.info("Running migration 4 -> 5: Adding announcements table")
        cursor = self.conn.cursor()
        
        # Check if table already exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='announcements'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE announcements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    course_id INTEGER NOT NULL,
                    announcement_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    description TEXT,
                    pub_date DATETIME,
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                    UNIQUE(course_id, announcement_id)
                )
            """)
            self.conn.commit()
            logging.info("Created announcements table")
        else:
            logging.debug("announcements table already exists, skipping")

    def _migration_5_to_6(self):
        """Migration: Remove base_path column from webdav_config table."""
        logging.info("Running migration 5 -> 6: Removing base_path column from webdav_config")
        cursor = self.conn.cursor()
        
        # Check if base_path column exists
        cursor.execute("PRAGMA table_info(webdav_config)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'base_path' in columns:
            # SQLite doesn't support DROP COLUMN, so we need to recreate the table
            logging.info("Recreating webdav_config table without base_path")
            
            # Copy data to temporary table
            cursor.execute("""
                CREATE TABLE webdav_config_new (
                    id INTEGER PRIMARY KEY,
                    hostname TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    disable_check INTEGER DEFAULT 0,
                    timeout INTEGER DEFAULT 30
                )
            """)
            
            cursor.execute("""
                INSERT INTO webdav_config_new (id, hostname, username, password, disable_check, timeout)
                SELECT id, hostname, username, password, disable_check, timeout
                FROM webdav_config
            """)
            
            # Drop old table and rename new one
            cursor.execute("DROP TABLE webdav_config")
            cursor.execute("ALTER TABLE webdav_config_new RENAME TO webdav_config")
            
            self.conn.commit()
            logging.info("Successfully removed base_path column from webdav_config table")
        else:
            logging.debug("base_path column does not exist, skipping")

    def _migration_6_to_7(self):
        """Migration: Add local_path column to files table."""
        logging.info("Running migration 6 -> 7: Adding local_path column to files table")
        cursor = self.conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(files)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'local_path' not in columns:
            cursor.execute("ALTER TABLE files ADD COLUMN local_path TEXT")
            self.conn.commit()
            logging.info("Added local_path column to files table")
        else:
            logging.debug("local_path column already exists, skipping")

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

    # --- Webhook configuration methods ---
    def save_webhook_config(self, webhook_url: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO webhook_config (id, webhook_url) VALUES (1, ?)
            """, (webhook_url,))
            self.conn.commit()
            logging.debug("Webhook configuration saved successfully")
        except Exception as e:
            logging.error(f"Failed to save webhook configuration: {e}", exc_info=True)
            raise

    def get_webhook_config(self) -> Optional[Dict[str, any]]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT webhook_url FROM webhook_config WHERE id = 1")
            row = cursor.fetchone()
            if row:
                return {'webhook_url': row[0]}
            return None
        except Exception as e:
            logging.error(f"Failed to get webhook configuration: {e}", exc_info=True)
            raise

    # --- WebDAV configuration methods ---
    def save_webdav_config(self, hostname: str, username: str, password: str, 
                           disable_check: bool = False, timeout: int = 30):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO webdav_config 
                (id, hostname, username, password, disable_check, timeout) 
                VALUES (1, ?, ?, ?, ?, ?)
            """, (hostname, username, password, int(disable_check), timeout))
            self.conn.commit()
            logging.debug("WebDAV configuration saved successfully")
        except Exception as e:
            logging.error(f"Failed to save WebDAV configuration: {e}", exc_info=True)
            raise

    def get_webdav_config(self) -> Optional[Dict[str, any]]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT hostname, username, password, disable_check, timeout 
                FROM webdav_config WHERE id = 1
            """)
            row = cursor.fetchone()
            if row:
                return {
                    'hostname': row[0],
                    'username': row[1],
                    'password': row[2],
                    'disable_check': bool(row[3]),
                    'timeout': row[4]
                }
            return None
        except Exception as e:
            logging.error(f"Failed to get WebDAV configuration: {e}", exc_info=True)
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
        """Serializes cookies with all their attributes for persistent storage."""
        cookies_list = []
        for cookie in cookie_jar:
            cookie_dict = {
                'name': cookie.name,
                'value': cookie.value,
                'domain': cookie.domain,
                'path': cookie.path,
                'secure': cookie.secure,
                'expires': cookie.expires,
                'rest': getattr(cookie, 'rest', {}),
            }
            cookies_list.append(cookie_dict)
        
        cookie_json = json.dumps(cookies_list)
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO app_data (key, value) VALUES ('session_cookie', ?)", (cookie_json,))
        self.conn.commit()

    def load_cookie_jar(self):
        """Loads and deserializes cookies from database."""
        from requests.cookies import RequestsCookieJar, create_cookie
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM app_data WHERE key = 'session_cookie'")
        row = cursor.fetchone()
        if row and row[0]:
            try:
                cookies_data = json.loads(row[0])
                if not isinstance(cookies_data, list):
                    return None
                
                cookie_jar = RequestsCookieJar()
                for cookie_dict in cookies_data:
                    try:
                        cookie = create_cookie(
                            name=cookie_dict['name'],
                            value=cookie_dict['value'],
                            domain=cookie_dict.get('domain', ''),
                            path=cookie_dict.get('path', '/'),
                            secure=cookie_dict.get('secure', False),
                            expires=cookie_dict.get('expires'),
                            rest=cookie_dict.get('rest', {})
                        )
                        cookie_jar.set_cookie(cookie)
                    except Exception:
                        pass
                
                return cookie_jar if len(cookie_jar) > 0 else None
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                return None
        return None

    # --- Course methods ---
    def save_course(self, course_id: int, name: str, webdav_folder: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO courses (id, name, webdav_folder) VALUES (?, ?, ?)", 
                          (course_id, name, webdav_folder))
            self.conn.commit()
            logging.debug(f"Saved course: {name} (ID: {course_id})")
        except Exception as e:
            logging.error(f"Failed to save course {name} (ID: {course_id}): {e}", exc_info=True)
            raise

    def get_courses(self) -> List[Dict]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, name, webdav_folder FROM courses")
            rows = cursor.fetchall()
            return [{"id": row[0], "name": row[1], "webdav_folder": row[2]} for row in rows]
        except Exception as e:
            logging.error(f"Failed to get courses: {e}", exc_info=True)
            raise

    def delete_course(self, course_id: int):
        """Delete a course and all its associated data."""
        try:
            cursor = self.conn.cursor()
            # Explicitly delete all child rows (in case foreign keys are not enforced)
            cursor.execute("DELETE FROM files WHERE node_id IN (SELECT id FROM nodes WHERE course_id = ?)", (course_id,))
            cursor.execute("DELETE FROM nodes WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM change_record_items WHERE change_record_id IN (SELECT id FROM change_records WHERE course_id = ?)", (course_id,))
            cursor.execute("DELETE FROM change_records WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM change_history WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM check_logs WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM courses WHERE id = ?", (course_id,))
            self.conn.commit()
            logging.debug(f"Deleted course ID: {course_id} and all associated data")
        except Exception as e:
            logging.error(f"Failed to delete course {course_id}: {e}", exc_info=True)
            raise

    def reset_course_data(self, course_id: int):
        """Reset all data for a course (tree, change history, and announcements) without deleting the course itself."""
        try:
            cursor = self.conn.cursor()
            # Delete files belonging to nodes of this course
            cursor.execute("DELETE FROM files WHERE node_id IN (SELECT id FROM nodes WHERE course_id = ?)", (course_id,))
            # Delete the file tree
            cursor.execute("DELETE FROM nodes WHERE course_id = ?", (course_id,))
            # Delete change history
            cursor.execute("DELETE FROM change_history WHERE course_id = ?", (course_id,))
            # Delete change record items before change records
            cursor.execute("DELETE FROM change_record_items WHERE change_record_id IN (SELECT id FROM change_records WHERE course_id = ?)", (course_id,))
            # Delete change records
            cursor.execute("DELETE FROM change_records WHERE course_id = ?", (course_id,))
            # Delete check logs for this course
            cursor.execute("DELETE FROM check_logs WHERE course_id = ?", (course_id,))
            # Delete announcements for this course
            cursor.execute("DELETE FROM announcements WHERE course_id = ?", (course_id,))
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
                    "INSERT INTO files (node_id, url, name, md5_hash, etag, last_updated, local_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (current_db_id, file.url, file.name, file.md5_hash, file.etag, file.last_updated, file.local_path)
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

        cursor.execute("SELECT node_id, url, name, md5_hash, etag, last_updated, local_path FROM files WHERE node_id IN ({seq})".format(
            seq=','.join(['?']*len(node_objects))), list(node_objects.keys()))
        
        for node_id, url, name, md5_hash, etag, last_updated, local_path in cursor.fetchall():
            if node_id in node_objects:
                node_objects[node_id].files.append(File(url=url, name=name, md5_hash=md5_hash, etag=etag, last_updated=last_updated, local_path=local_path))

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

            self.conn.commit()
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

    # --- Announcements methods ---
    def save_announcements(self, course_id: int, announcements: List[Dict]):
        """
        Save announcements for a course.
        
        Args:
            course_id: The course ID
            announcements: List of announcement dictionaries with keys:
                - announcement_id: Unique ID for the announcement
                - title: Announcement title
                - link: Link to the announcement
                - description: HTML description
                - pub_date: Publication date as datetime object
        """
        try:
            cursor = self.conn.cursor()
            for announcement in announcements:
                cursor.execute("""
                    INSERT OR REPLACE INTO announcements 
                    (course_id, announcement_id, title, link, description, pub_date, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    course_id,
                    announcement['announcement_id'],
                    announcement['title'],
                    announcement['link'],
                    announcement['description'],
                    announcement['pub_date'].isoformat() if announcement['pub_date'] else None
                ))
            self.conn.commit()
            logging.debug(f"Saved {len(announcements)} announcements for course {course_id}")
        except Exception as e:
            logging.error(f"Failed to save announcements for course {course_id}: {e}", exc_info=True)
            raise

    def get_announcements(self, course_id: Optional[int] = None, limit: int = 50) -> List[Dict]:
        """
        Get announcements for a course or all courses.
        
        Args:
            course_id: Optional course ID to filter by
            limit: Maximum number of announcements to return
        
        Returns:
            List of announcement dictionaries
        """
        try:
            cursor = self.conn.cursor()
            if course_id:
                cursor.execute("""
                    SELECT a.id, a.course_id, c.name as course_name, a.announcement_id,
                           a.title, a.link, a.description, a.pub_date, a.fetched_at
                    FROM announcements a
                    JOIN courses c ON a.course_id = c.id
                    WHERE a.course_id = ?
                    ORDER BY a.pub_date DESC
                    LIMIT ?
                """, (course_id, limit))
            else:
                cursor.execute("""
                    SELECT a.id, a.course_id, c.name as course_name, a.announcement_id,
                           a.title, a.link, a.description, a.pub_date, a.fetched_at
                    FROM announcements a
                    JOIN courses c ON a.course_id = c.id
                    ORDER BY a.pub_date DESC
                    LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "course_id": row[1],
                    "course_name": row[2],
                    "announcement_id": row[3],
                    "title": row[4],
                    "link": row[5],
                    "description": row[6],
                    "pub_date": row[7],
                    "fetched_at": row[8]
                }
                for row in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get announcements: {e}", exc_info=True)
            raise

    def get_latest_announcement_date(self, course_id: int) -> Optional[datetime]:
        """
        Get the publication date of the latest announcement for a course.
        
        Args:
            course_id: The course ID
        
        Returns:
            datetime object of the latest announcement, or None if no announcements exist
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT pub_date
                FROM announcements
                WHERE course_id = ?
                ORDER BY pub_date DESC
                LIMIT 1
            """, (course_id,))
            row = cursor.fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None
        except Exception as e:
            logging.error(f"Failed to get latest announcement date for course {course_id}: {e}", exc_info=True)
            raise

    def close(self):
        """Closes the database connection."""
        self.conn.close()