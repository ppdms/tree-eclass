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
from app.services.differ import ChangeItem

DB_FILE = os.getenv("DB_FILE", "eclass.db")
SCHEMA_VERSION = 16  # Current schema version

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
                redirect_url TEXT,
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
                display_name TEXT,
                redirect_url TEXT,
                diff_webdav_path TEXT,
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                version_webdav_path TEXT,
                change_type TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                display_name TEXT,
                redirect_url TEXT,
                diff_webdav_path TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_study (
                course_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (course_id, file_path),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS study_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                note TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS global_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_key TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                description TEXT,
                pub_date DATETIME,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(feed_key, announcement_id)
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

        if current_version < 8:
            self._migration_7_to_8()
            self._set_schema_version(8)

        if current_version < 9:
            self._migration_8_to_9()
            self._set_schema_version(9)

        if current_version < 10:
            self._migration_9_to_10()
            self._set_schema_version(10)

        if current_version < 11:
            self._migration_10_to_11()
            self._set_schema_version(11)

        if current_version < 12:
            self._migration_11_to_12()
            self._set_schema_version(12)

        if current_version < 13:
            self._migration_12_to_13()
            self._set_schema_version(13)

        if current_version < 14:
            self._migration_13_to_14()
            self._set_schema_version(14)

        if current_version < 15:
            self._migration_14_to_15()
            self._set_schema_version(15)

        if current_version < 16:
            self._migration_15_to_16()
            self._set_schema_version(16)

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

    def _migration_7_to_8(self):
        """Migration: Add display_name column to change_record_items."""
        logging.info("Running migration 7 -> 8: Adding display_name column to change_record_items")
        cursor = self.conn.cursor()

        cursor.execute("PRAGMA table_info(change_record_items)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'display_name' not in columns:
            cursor.execute("ALTER TABLE change_record_items ADD COLUMN display_name TEXT")
            self.conn.commit()
            logging.info("Added display_name column to change_record_items")
        else:
            logging.debug("display_name column already exists, skipping")

    def _migration_8_to_9(self):
        """Migration: Retroactively backfill file_path (actual filename) and display_name for old change_record_items."""
        logging.info("Running migration 8 -> 9: Backfilling actual file paths in change_record_items")
        cursor = self.conn.cursor()

        # Fetch old items where display_name was not yet stored (pre-fix records)
        cursor.execute("""
            SELECT cri.id, cri.file_path, cr.course_id
            FROM change_record_items cri
            JOIN change_records cr ON cri.change_record_id = cr.id
            WHERE cri.display_name IS NULL
            AND cri.change_type IN ('added_file', 'modified_file')
        """)
        items = cursor.fetchall()

        # Course webdav_folder lookup
        cursor.execute("SELECT id, webdav_folder FROM courses")
        courses = {row[0]: (row[1] or '').strip('/') for row in cursor.fetchall()}

        # Build (course_id, display_name) -> actual local_path index from current files table
        cursor.execute("""
            SELECT f.name, f.local_path, n.course_id
            FROM files f
            JOIN nodes n ON f.node_id = n.id
            WHERE f.local_path IS NOT NULL
        """)
        file_map = {}
        for name, local_path, course_id in cursor.fetchall():
            key = (course_id, name)
            if key not in file_map:
                file_map[key] = local_path

        fixed = 0
        for item_id, file_path, course_id in items:
            # The old file_path was built from display names; last segment is the display name
            display_name = file_path.split('/')[-1]
            actual_local_path = file_map.get((course_id, display_name))
            if actual_local_path:
                # Make path relative to course webdav_folder
                webdav_folder = courses.get(course_id, '')
                rel = actual_local_path.strip('/')
                if webdav_folder and rel.startswith(webdav_folder + '/'):
                    rel = rel[len(webdav_folder) + 1:]
                cursor.execute(
                    "UPDATE change_record_items SET file_path = ?, display_name = ? WHERE id = ?",
                    (rel, display_name, item_id)
                )
                fixed += 1

        self.conn.commit()
        logging.info(f"Migration 8 -> 9: fixed {fixed} / {len(items)} old change_record_items")

    def _migration_9_to_10(self):
        """Migration: Add redirect_url column to files and change_record_items tables."""
        logging.info("Running migration 9 -> 10: Adding redirect_url columns")
        cursor = self.conn.cursor()

        cursor.execute("PRAGMA table_info(files)")
        if 'redirect_url' not in [row[1] for row in cursor.fetchall()]:
            cursor.execute("ALTER TABLE files ADD COLUMN redirect_url TEXT")
            logging.info("Added redirect_url column to files table")

        cursor.execute("PRAGMA table_info(change_record_items)")
        if 'redirect_url' not in [row[1] for row in cursor.fetchall()]:
            cursor.execute("ALTER TABLE change_record_items ADD COLUMN redirect_url TEXT")
            logging.info("Added redirect_url column to change_record_items table")

        self.conn.commit()

    def _migration_10_to_11(self):
        """Migration: Add file_versions table for tracking modified/deleted file history."""
        logging.info("Running migration 10 -> 11: Adding file_versions table")
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_versions'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE file_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    course_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    version_webdav_path TEXT,
                    change_type TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    display_name TEXT,
                    redirect_url TEXT,
                    FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
                )
            """)
            self.conn.commit()
            logging.info("Created file_versions table")
        else:
            logging.debug("file_versions table already exists, skipping")

    def _migration_11_to_12(self):
        """Migration: Add diff_webdav_path column to file_versions table."""
        logging.info("Running migration 11 -> 12: Adding diff_webdav_path column to file_versions")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(file_versions)")
        if 'diff_webdav_path' not in [row[1] for row in cursor.fetchall()]:
            cursor.execute("ALTER TABLE file_versions ADD COLUMN diff_webdav_path TEXT")
            self.conn.commit()
            logging.info("Added diff_webdav_path column to file_versions table")
        else:
            logging.debug("diff_webdav_path column already exists, skipping")

    def _migration_12_to_13(self):
        """Migration: Add diff_webdav_path column to change_record_items table."""
        logging.info("Running migration 12 -> 13: Adding diff_webdav_path column to change_record_items")
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(change_record_items)")
        if 'diff_webdav_path' not in [row[1] for row in cursor.fetchall()]:
            cursor.execute("ALTER TABLE change_record_items ADD COLUMN diff_webdav_path TEXT")
            self.conn.commit()
            logging.info("Added diff_webdav_path column to change_record_items table")
        else:
            logging.debug("diff_webdav_path column already exists, skipping")

    def _migration_13_to_14(self):
        """Migration: Add file_study table for per-file comprehension level tracking."""
        logging.info("Running migration 13 -> 14: Adding file_study table")
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_study'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE file_study (
                    course_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    level INTEGER NOT NULL DEFAULT 0,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (course_id, file_path),
                    FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
                )
            """)
            self.conn.commit()
            logging.info("Created file_study table")
        else:
            logging.debug("file_study table already exists, skipping")

    def _migration_14_to_15(self):
        """Migration: Add study_sessions table for coarse study session logging."""
        logging.info("Running migration 14 -> 15: Adding study_sessions table")
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='study_sessions'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE study_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    course_id INTEGER NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    note TEXT,
                    FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
                )
            """)
            self.conn.commit()
            logging.info("Created study_sessions table")
        else:
            logging.debug("study_sessions table already exists, skipping")

    def _migration_15_to_16(self):
        """Migration: Add global_announcements table and global feed toggles in preferences."""
        logging.info("Running migration 15 -> 16: Adding global_announcements + feed toggles")
        cursor = self.conn.cursor()

        # Create global_announcements table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='global_announcements'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE global_announcements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    feed_key TEXT NOT NULL,
                    announcement_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    description TEXT,
                    pub_date DATETIME,
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(feed_key, announcement_id)
                )
            """)
            logging.info("Created global_announcements table")

        # Add 3 global feed toggle columns to preferences
        cursor.execute("PRAGMA table_info(preferences)")
        existing = {row[1] for row in cursor.fetchall()}
        for col, default in [
            ('global_feed_dept_enabled', 1),
            ('global_feed_undergrad_enabled', 0),
            ('global_feed_rector_enabled', 0),
        ]:
            if col not in existing:
                cursor.execute(f"ALTER TABLE preferences ADD COLUMN {col} INTEGER DEFAULT {default}")
                logging.info(f"Added {col} column to preferences")

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
                        notification_on_error: bool = True,
                        global_feed_dept_enabled: bool = True,
                        global_feed_undergrad_enabled: bool = False,
                        global_feed_rector_enabled: bool = False):
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO preferences 
                (id, check_interval_minutes, max_concurrent_downloads, request_timeout_seconds, 
                 retry_attempts, notification_enabled, notification_on_error,
                 global_feed_dept_enabled, global_feed_undergrad_enabled, global_feed_rector_enabled) 
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (check_interval_minutes, max_concurrent_downloads, request_timeout_seconds, 
                  retry_attempts, int(notification_enabled), int(notification_on_error),
                  int(global_feed_dept_enabled), int(global_feed_undergrad_enabled), int(global_feed_rector_enabled)))
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
                       retry_attempts, notification_enabled, notification_on_error,
                       global_feed_dept_enabled, global_feed_undergrad_enabled, global_feed_rector_enabled
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
                    'notification_on_error': bool(row[5]),
                    'global_feed_dept_enabled': bool(row[6]),
                    'global_feed_undergrad_enabled': bool(row[7]),
                    'global_feed_rector_enabled': bool(row[8]),
                }
            # Return defaults if no preferences set
            return {
                'check_interval_minutes': 60,
                'max_concurrent_downloads': 3,
                'request_timeout_seconds': 30,
                'retry_attempts': 3,
                'notification_enabled': True,
                'notification_on_error': True,
                'global_feed_dept_enabled': True,
                'global_feed_undergrad_enabled': False,
                'global_feed_rector_enabled': False,
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
            cursor.execute("DELETE FROM file_versions WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM file_study WHERE course_id = ?", (course_id,))
            cursor.execute("DELETE FROM study_sessions WHERE course_id = ?", (course_id,))
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
            # Delete file version history
            cursor.execute("DELETE FROM file_versions WHERE course_id = ?", (course_id,))
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
                    "INSERT INTO files (node_id, url, name, md5_hash, etag, last_updated, local_path, redirect_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (current_db_id, file.url, file.name, file.md5_hash, file.etag, file.last_updated, file.local_path, file.redirect_url)
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

        cursor.execute("SELECT node_id, url, name, md5_hash, etag, last_updated, local_path, redirect_url FROM files WHERE node_id IN ({seq})".format(
            seq=','.join(['?']*len(node_objects))), list(node_objects.keys()))
        
        for node_id, url, name, md5_hash, etag, last_updated, local_path, redirect_url in cursor.fetchall():
            if node_id in node_objects:
                node_objects[node_id].files.append(File(url=url, name=name, md5_hash=md5_hash, etag=etag, last_updated=last_updated, local_path=local_path, redirect_url=redirect_url))

        return root_node

    # --- Change record methods ---
    def create_change_record(self, course_id: int, changes: List[ChangeItem]) -> int:
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
                if change.change_type in ("added_file", "added_directory"):
                    added_count += 1
                elif change.change_type in ("deleted_file", "deleted_directory"):
                    deleted_count += 1
                elif change.change_type == "modified_file":
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
                cursor.execute(
                    "INSERT INTO change_record_items (change_record_id, change_type, file_path, display_name, redirect_url, diff_webdav_path) VALUES (?, ?, ?, ?, ?, ?)",
                    (change_record_id, change.change_type, change.file_path, change.display_name, change.redirect_url, getattr(change, 'diff_webdav_path', None))
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

    def log_changes(self, course_id: int, changes: List[ChangeItem]):
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
                SELECT id, change_type, file_path, display_name, redirect_url, diff_webdav_path
                FROM change_record_items
                WHERE change_record_id = ?
                ORDER BY id
            """, (change_record_id,))
            
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "change_type": row[1],
                    "file_path": row[2],
                    "display_name": row[3],
                    "redirect_url": row[4],
                    "diff_webdav_path": row[5],
                }
                for row in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get change record items for change record {change_record_id}: {e}", exc_info=True)
            raise

    def get_timeline_data(self, limit: int = 100, course_id: Optional[int] = None) -> List[Dict]:
        """Return a combined, time-sorted list of change records (with items) and announcements."""
        try:
            cursor = self.conn.cursor()

            # --- Change records ---
            if course_id is not None:
                cursor.execute("""
                    SELECT cr.id, cr.course_id, co.name, cr.change_no, cr.timestamp, cr.message
                    FROM change_records cr
                    JOIN courses co ON cr.course_id = co.id
                    WHERE cr.course_id = ?
                    ORDER BY cr.timestamp DESC
                    LIMIT ?
                """, (course_id, limit))
            else:
                cursor.execute("""
                    SELECT cr.id, cr.course_id, co.name, cr.change_no, cr.timestamp, cr.message
                    FROM change_records cr
                    JOIN courses co ON cr.course_id = co.id
                    ORDER BY cr.timestamp DESC
                    LIMIT ?
                """, (limit,))
            change_rows = cursor.fetchall()

            # Bulk-fetch items for all returned change records
            items_by_cr_id: Dict[int, List[Dict]] = {}
            if change_rows:
                cr_ids = [row[0] for row in change_rows]
                placeholders = ','.join('?' * len(cr_ids))
                cursor.execute(
                    f"SELECT change_record_id, change_type, file_path, display_name, redirect_url, diff_webdav_path "
                    f"FROM change_record_items WHERE change_record_id IN ({placeholders}) ORDER BY id",
                    cr_ids
                )
                for cr_id, change_type, file_path, display_name, redirect_url, diff_webdav_path in cursor.fetchall():
                    items_by_cr_id.setdefault(cr_id, []).append(
                        {"change_type": change_type, "file_path": file_path, "display_name": display_name, "redirect_url": redirect_url, "diff_webdav_path": diff_webdav_path}
                    )

            timeline: List[Dict] = []
            for cr_id, course_id, course_name, change_no, timestamp, message in change_rows:
                timeline.append({
                    "type": "change",
                    "sort_key": timestamp or "",
                    "timestamp": timestamp,
                    "course_id": course_id,
                    "course_name": course_name,
                    "change_no": change_no,
                    "message": message,
                    "id": cr_id,
                    "changes": items_by_cr_id.get(cr_id, []),
                })

            # --- Announcements ---
            if course_id is not None:
                cursor.execute("""
                    SELECT a.id, a.course_id, co.name, a.title, a.link, a.description, a.pub_date
                    FROM announcements a
                    JOIN courses co ON a.course_id = co.id
                    WHERE a.course_id = ?
                    ORDER BY a.pub_date DESC
                    LIMIT ?
                """, (course_id, limit))
            else:
                cursor.execute("""
                    SELECT a.id, a.course_id, co.name, a.title, a.link, a.description, a.pub_date
                    FROM announcements a
                    JOIN courses co ON a.course_id = co.id
                    ORDER BY a.pub_date DESC
                    LIMIT ?
                """, (limit,))
            for ann_id, course_id, course_name, title, link, description, pub_date in cursor.fetchall():
                timeline.append({
                    "type": "announcement",
                    "sort_key": pub_date or "",
                    "timestamp": pub_date,
                    "course_id": course_id,
                    "course_name": course_name,
                    "title": title,
                    "link": link,
                    "description": description,
                    "id": ann_id,
                })

            # Merge-sort descending by timestamp string (ISO 8601 sorts lexicographically)
            timeline.sort(key=lambda x: x["sort_key"], reverse=True)
            return timeline[:limit]
        except Exception as e:
            logging.error(f"Failed to get timeline data: {e}", exc_info=True)
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

    # --- File version methods ---
    def save_file_version(self, course_id: int, file_path: str, version_webdav_path: Optional[str],
                          change_type: str, display_name: Optional[str] = None,
                          redirect_url: Optional[str] = None, diff_webdav_path: Optional[str] = None):
        """Record a version of a file (modified or deleted)."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO file_versions (course_id, file_path, version_webdav_path, change_type, display_name, redirect_url, diff_webdav_path)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (course_id, file_path, version_webdav_path, change_type, display_name, redirect_url, diff_webdav_path))
            self.conn.commit()
            logging.debug(f"Saved file version: course={course_id} path={file_path} type={change_type}")
        except Exception as e:
            logging.error(f"Failed to save file version: {e}", exc_info=True)
            raise

    def get_file_versions(self, course_id: int, file_path: Optional[str] = None,
                          change_type: Optional[str] = None) -> List[Dict]:
        """Get file versions for a course, optionally filtered by path and/or change type."""
        try:
            cursor = self.conn.cursor()
            query = """
                SELECT id, course_id, file_path, version_webdav_path, change_type, timestamp, display_name, redirect_url, diff_webdav_path
                FROM file_versions WHERE course_id = ?
            """
            params: list = [course_id]
            if file_path is not None:
                query += " AND file_path = ?"
                params.append(file_path)
            if change_type is not None:
                query += " AND change_type = ?"
                params.append(change_type)
            query += " ORDER BY timestamp DESC"
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {
                    "id": r[0], "course_id": r[1], "file_path": r[2],
                    "version_webdav_path": r[3], "change_type": r[4],
                    "timestamp": r[5], "display_name": r[6], "redirect_url": r[7],
                    "diff_webdav_path": r[8],
                }
                for r in rows
            ]
        except Exception as e:
            logging.error(f"Failed to get file versions for course {course_id}: {e}", exc_info=True)
            raise

    def get_files_with_versions(self, course_id: int) -> set:
        """Return the set of relative file_paths that have at least one 'modified' version record."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT DISTINCT file_path FROM file_versions WHERE course_id = ? AND change_type = 'modified'",
                (course_id,)
            )
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Failed to get files with versions for course {course_id}: {e}", exc_info=True)
            raise

    def get_folders_with_deleted(self, course_id: int) -> set:
        """Return the set of relative folder paths (including root='') that contain deleted files."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT DISTINCT file_path FROM file_versions WHERE course_id = ? AND change_type = 'deleted'",
                (course_id,)
            )
            folders: set = set()
            for (file_path,) in cursor.fetchall():
                parts = file_path.split('/')
                for i in range(len(parts)):  # i=0 → "", i=1 → first dir, etc.
                    folders.add('/'.join(parts[:i]))
            return folders
        except Exception as e:
            logging.error(f"Failed to get folders with deleted files for course {course_id}: {e}", exc_info=True)
            raise

    # --- Study tracking methods ---

    def set_file_study_level(self, course_id: int, file_path: str, level: int):
        """Upsert the comprehension level (0-5) for a file. Level 5 means ignored."""
        level = max(0, min(5, level))
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO file_study (course_id, file_path, level, last_updated)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(course_id, file_path) DO UPDATE SET
                    level = excluded.level,
                    last_updated = excluded.last_updated
            """, (course_id, file_path, level))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to set study level for course {course_id} path {file_path}: {e}", exc_info=True)
            raise

    def get_file_study_levels(self, course_id: int) -> Dict[str, int]:
        """Return {file_path: level} for all tracked files in a course."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT file_path, level FROM file_study WHERE course_id = ?", (course_id,))
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Failed to get study levels for course {course_id}: {e}", exc_info=True)
            raise

    def get_course_study_summary(self, course_id: int) -> Dict:
        """Return study level distribution for a course.

        Returns: {total, by_level: {0..4: count}, completion_ratio: 0.0-1.0}
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM files f JOIN nodes n ON f.node_id = n.id WHERE n.course_id = ?",
                (course_id,)
            )
            total = cursor.fetchone()[0]

            cursor.execute(
                "SELECT level, COUNT(*) FROM file_study WHERE course_id = ? GROUP BY level",
                (course_id,)
            )
            by_level = {row[0]: row[1] for row in cursor.fetchall()}

            ignored_count = by_level.pop(5, 0)
            effective_total = max(0, total - ignored_count)

            # Files not in file_study are implicitly level 0
            studied_count = sum(by_level.values())
            by_level[0] = by_level.get(0, 0) + max(0, effective_total - studied_count)

            completion_ratio = (
                sum(by_level.get(i, 0) * i for i in range(5)) / (4 * effective_total)
                if effective_total > 0 else 1.0
            )
            return {
                "total": effective_total,
                "ignored": ignored_count,
                "by_level": {str(i): by_level.get(i, 0) for i in range(5)},
                "completion_ratio": completion_ratio,
            }
        except Exception as e:
            logging.error(f"Failed to get study summary for course {course_id}: {e}", exc_info=True)
            raise

    def add_study_session(self, course_id: int, note: Optional[str] = None) -> int:
        """Record a study session for a course. Returns the new session id."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO study_sessions (course_id, note) VALUES (?, ?)",
                (course_id, note)
            )
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logging.error(f"Failed to add study session for course {course_id}: {e}", exc_info=True)
            raise

    def get_study_sessions(self, course_id: Optional[int] = None, limit: int = 100) -> List[Dict]:
        """Get study sessions, optionally filtered by course, newest first."""
        try:
            cursor = self.conn.cursor()
            if course_id is not None:
                cursor.execute("""
                    SELECT ss.id, ss.course_id, c.name, ss.timestamp, ss.note
                    FROM study_sessions ss
                    JOIN courses c ON ss.course_id = c.id
                    WHERE ss.course_id = ?
                    ORDER BY ss.timestamp DESC LIMIT ?
                """, (course_id, limit))
            else:
                cursor.execute("""
                    SELECT ss.id, ss.course_id, c.name, ss.timestamp, ss.note
                    FROM study_sessions ss
                    JOIN courses c ON ss.course_id = c.id
                    ORDER BY ss.timestamp DESC LIMIT ?
                """, (limit,))
            return [
                {"id": r[0], "course_id": r[1], "course_name": r[2], "timestamp": r[3], "note": r[4]}
                for r in cursor.fetchall()
            ]
        except Exception as e:
            logging.error(f"Failed to get study sessions: {e}", exc_info=True)
            raise

    def get_study_inbox(self, limit: int = 50) -> List[Dict]:
        """Return priority-sorted list of unmastered files across all courses.

        Priority = file_age_days (capped at 90) * (1 - course_completion_ratio)
        Higher score = study this first.
        """
        try:
            cursor = self.conn.cursor()

            # All non-mastered files with their study levels
            cursor.execute("""
                SELECT
                    f.local_path,
                    f.name,
                    f.url,
                    f.redirect_url,
                    f.last_updated,
                    n.course_id,
                    c.name AS course_name,
                    c.webdav_folder,
                    COALESCE(fs.level, 0) AS level,
                    COALESCE(
                        (julianday('now') - julianday(f.last_updated)),
                        30
                    ) AS age_days
                FROM files f
                JOIN nodes n ON f.node_id = n.id
                JOIN courses c ON n.course_id = c.id
                LEFT JOIN file_study fs
                    ON fs.course_id = n.course_id AND fs.file_path = f.local_path
                WHERE COALESCE(fs.level, 0) < 4
                AND f.local_path IS NOT NULL
            """)
            rows = cursor.fetchall()

            # Completion ratio per course
            cursor.execute("""
                SELECT n.course_id,
                       COUNT(*) AS total_files,
                       SUM(COALESCE(fs.level, 0)) AS total_level_sum
                FROM files f
                JOIN nodes n ON f.node_id = n.id
                LEFT JOIN file_study fs
                    ON fs.course_id = n.course_id AND fs.file_path = f.local_path
                GROUP BY n.course_id
            """)
            course_stats: Dict[int, float] = {}
            for cid, total_files, total_level_sum in cursor.fetchall():
                course_stats[cid] = (
                    (total_level_sum or 0) / (4 * total_files)
                    if total_files > 0 else 1.0
                )

            results = []
            for row in rows:
                local_path, name, url, redirect_url, last_updated, course_id, \
                    course_name, webdav_folder, level, age_days = row
                completion_ratio = course_stats.get(course_id, 0.0)
                age_capped = min(float(age_days), 90.0)
                priority = age_capped * (1.0 - completion_ratio)
                results.append({
                    "file_path": local_path,
                    "file_name": name,
                    "url": url,
                    "redirect_url": redirect_url,
                    "last_updated": last_updated,
                    "course_id": course_id,
                    "course_name": course_name,
                    "webdav_folder": webdav_folder,
                    "level": level,
                    "priority": priority,
                })

            results.sort(key=lambda x: x["priority"], reverse=True)
            return results[:limit]
        except Exception as e:
            logging.error(f"Failed to get study inbox: {e}", exc_info=True)
            raise

    # --- Global feed announcement methods ---

    def save_global_announcements(self, feed_key: str, announcements: List[Dict]):
        """Save announcements from a global feed (upsert by feed_key + announcement_id)."""
        try:
            cursor = self.conn.cursor()
            for ann in announcements:
                if not ann.get('announcement_id'):
                    continue
                cursor.execute("""
                    INSERT OR REPLACE INTO global_announcements
                    (feed_key, announcement_id, title, link, description, pub_date, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    feed_key,
                    ann['announcement_id'],
                    ann['title'],
                    ann['link'],
                    ann.get('description', ''),
                    ann['pub_date'].isoformat() if ann.get('pub_date') else None,
                ))
            self.conn.commit()
            logging.debug(f"Saved {len(announcements)} global announcements for feed '{feed_key}'")
        except Exception as e:
            logging.error(f"Failed to save global announcements for feed {feed_key}: {e}", exc_info=True)
            raise

    def get_global_announcements(self, feed_key: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get global feed announcements, optionally filtered by feed_key, newest first."""
        try:
            cursor = self.conn.cursor()
            if feed_key:
                cursor.execute("""
                    SELECT id, feed_key, announcement_id, title, link, description, pub_date, fetched_at
                    FROM global_announcements WHERE feed_key = ?
                    ORDER BY pub_date DESC LIMIT ?
                """, (feed_key, limit))
            else:
                cursor.execute("""
                    SELECT id, feed_key, announcement_id, title, link, description, pub_date, fetched_at
                    FROM global_announcements
                    ORDER BY pub_date DESC LIMIT ?
                """, (limit,))
            return [
                {
                    "id": r[0], "feed_key": r[1], "announcement_id": r[2],
                    "title": r[3], "link": r[4], "description": r[5],
                    "pub_date": r[6], "fetched_at": r[7],
                }
                for r in cursor.fetchall()
            ]
        except Exception as e:
            logging.error(f"Failed to get global announcements: {e}", exc_info=True)
            raise

    def get_latest_global_announcement_date(self, feed_key: str) -> Optional[datetime]:
        """Return the pub_date of the newest stored announcement for a feed, or None."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT pub_date FROM global_announcements
                WHERE feed_key = ? ORDER BY pub_date DESC LIMIT 1
            """, (feed_key,))
            row = cursor.fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None
        except Exception as e:
            logging.error(f"Failed to get latest global announcement date for {feed_key}: {e}", exc_info=True)
            raise

    def close(self):
        """Closes the database connection."""
        self.conn.close()