"""
Main entry point for the tree-eclass checker application.
Orchestrates the scraping, diffing, and notification process.
"""
import logging
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from app.services import differ, tree_builder
from app.services.announcements_scraper import AnnouncementsScraper, GLOBAL_FEEDS
from app.services.persistence import DatabaseManager
from app.services.scraper import Scraper, COURSE_URL_TEMPLATE
from app.services.webdav_uploader import WebDAVUploader


# Tree printing functions
def print_tree(root_node, course_name: str):
    """Prints the entire tree structure starting from the root node."""
    print(f'\033]8;;{root_node.url}\007{course_name}\033]8;;\007')
    _print_children_recursive(root_node, "")


def _print_children_recursive(parent_node, indent: str):
    """Recursively prints the children of a node."""
    directories = parent_node.children
    files = parent_node.files

    total_children = len(directories) + len(files)
    children_processed = 0

    # Print directories first
    for i, directory in enumerate(directories):
        children_processed += 1
        is_last = (children_processed == total_children)
        connector = "└── " if is_last else "├── "
        print(f'{indent}{connector}\033]8;;{directory.url}\007{directory.name}\033]8;;\007')
        next_indent = indent + ("    " if is_last else "│   ")
        _print_children_recursive(directory, next_indent)

    # Then print files
    for i, file in enumerate(files):
        children_processed += 1
        is_last = (children_processed == total_children)
        connector = "└── " if is_last else "├── "
        print(f'{indent}{connector}\033]8;;{file.url}\007{file.name}\033]8;;\007')


# Webhook notification function
def send_webhook(changes_by_course: dict, announcements_by_course: dict, db_manager: DatabaseManager):
    """Sends a Discord webhook notification with the detected changes and announcements."""
    webhook_config = db_manager.get_webhook_config()

    if not webhook_config:
        logging.warning("Webhook configuration not found in database. Notification not sent.")
        print("Webhook not configured. Please configure webhook settings in the web interface.")
        return

    webhook_url = webhook_config['webhook_url']

    # Build messages per course, splitting into multiple if over 2000 chars
    messages = []

    # Handle file changes
    for course_name, changes in changes_by_course.items():
        if not changes:
            continue

        # Get the latest change record for timestamp/summary
        courses = db_manager.get_courses()
        course = next((c for c in courses if c['name'] == course_name), None)
        if not course:
            continue

        change_records = db_manager.get_change_records(course_id=course['id'], limit=1)

        formatted_date = ""
        summary = ""
        if change_records:
            record = change_records[0]
            try:
                dt = datetime.fromisoformat(record['change_no'])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo('Europe/Athens'))
                formatted_date = dt.strftime('%B %d, %Y at %H:%M:%S')
            except Exception:
                formatted_date = record['change_no']
            summary = record.get('message', '')

        # Build header
        header_lines = [f"**Change Detected \u2014 {course_name}**\n"]
        if formatted_date:
            header_lines.append(f"**Date:** {formatted_date}")
        if summary:
            header_lines.append(f"**Changes:** `{summary}`")
        header_lines.append("")
        header = "\n".join(header_lines)

        # Split change lines into messages that fit within 2000 chars
        # Each message needs: header (first only) + "```\n" + lines + "\n```"
        code_open = "```\n"
        code_close = "\n```"
        current_is_first = True

        change_lines = [f"\u2022 {change}" for change in changes]
        batch = []
        batch_len = 0

        for line in change_lines:
            prefix = header if current_is_first else ""
            overhead = len(prefix) + len(code_open) + len(code_close)
            line_addition = len(line) + (1 if batch else 0)  # +1 for newline separator

            # If a single line alone exceeds the limit, truncate it
            if overhead + line_addition > 2000:
                line = line[:2000 - overhead - 5] + "…"
                line_addition = len(line) + (1 if batch else 0)

            if batch and overhead + batch_len + line_addition > 2000:
                # Flush current batch
                body = prefix + code_open + "\n".join(batch) + code_close
                messages.append(body)
                current_is_first = False
                batch = [line]
                batch_len = len(line)
            else:
                batch.append(line)
                batch_len += line_addition

        # Flush remaining
        if batch:
            prefix = header if current_is_first else ""
            body = prefix + code_open + "\n".join(batch) + code_close
            messages.append(body)

    # Handle announcements
    for course_name, announcements in announcements_by_course.items():
        if not announcements:
            continue

        # Build announcement message
        header_lines = [f"**📢 New Announcements \u2014 {course_name}**\n"]
        header_lines.append(f"**Count:** {len(announcements)} new announcement(s)")
        header_lines.append("")
        header = "\n".join(header_lines)

        announcement_lines = []
        for announcement in announcements:
            title = announcement['title']
            link = announcement['link']
            description = announcement.get('description', '')
            pub_date = ""
            if announcement.get('pub_date'):
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(announcement['pub_date']) if isinstance(announcement['pub_date'], str) else announcement['pub_date']
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ZoneInfo('UTC'))
                    dt = dt.astimezone(ZoneInfo('Europe/Athens'))
                    pub_date = dt.strftime('%b %d, %Y at %H:%M')
                except Exception:
                    pass
            
            # Convert HTML description to Markdown
            if description:
                from markdownify import markdownify as md
                description_md = md(description, heading_style="ATX", strip=['script', 'style'])
                # Clean up excessive whitespace
                description_md = ' '.join(description_md.split())
            else:
                description_md = ""
            
            # Format: • Title (Date) - Link + Description
            line = f"\u2022 **{title}**"
            if pub_date:
                line += f" ({pub_date})"
            line += f"\n  {link}"
            if description_md:
                line += f"\n  {description_md}"
            announcement_lines.append(line)

        # Split into messages that fit within 2000 chars
        current_is_first = True
        batch = []
        batch_len = 0

        for line in announcement_lines:
            prefix = header if current_is_first else ""
            overhead = len(prefix) + 10  # Some margin
            line_addition = len(line) + (2 if batch else 0)  # +2 for double newline separator

            # If a single line alone exceeds the limit, truncate it
            if overhead + len(line) + (2 if batch else 0) > 2000:
                line = line[:2000 - overhead - 5] + "…"
                line_addition = len(line) + (2 if batch else 0)

            if batch and overhead + batch_len + line_addition > 2000:
                # Flush current batch
                body = prefix + "\n\n".join(batch)
                messages.append(body)
                current_is_first = False
                batch = [line]
                batch_len = len(line)
            else:
                batch.append(line)
                batch_len += line_addition

        # Flush remaining
        if batch:
            prefix = header if current_is_first else ""
            body = prefix + "\n\n".join(batch)
            messages.append(body)

    if not messages:
        logging.info("No changes or announcements to send via webhook")
        return

    print(f"\n=== Webhook ===")
    print(f"URL: {webhook_url[:60]}...")
    print(f"Messages: {len(messages)}")
    print("===============\n")

    try:
        for i, content in enumerate(messages):
            payload = {"username": "tree-eclass", "content": content}
            resp = requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            if i < len(messages) - 1:
                time.sleep(0.5)  # rate limit courtesy

        logging.info(f"Webhook notification sent successfully ({len(messages)} message(s))")
        print(f"\u2713 Webhook sent successfully ({len(messages)} message(s))")

    except requests.RequestException as e:
        logging.error(f"Failed to send webhook: {e}", exc_info=True)
        print(f"Failed to send webhook: {e}", file=sys.stderr)


def _flatten_files(node) -> dict:
    """Return a flat dict of local_path -> File for all files in the tree."""
    result = {}
    for file in node.files:
        if file.local_path:
            result['/' + file.local_path.strip('/')] = file
    for child in node.children:
        result.update(_flatten_files(child))
    return result


def process_course(db_manager: DatabaseManager, course: dict, scraper_instance: Scraper) -> tuple[list, list, bool]:
    """
    Process a single course: check for changes, update tree, and log changes.
    
    Returns:
        tuple: (list of file changes, list of new announcements, success flag)
    """
    course_id = course['id']
    course_name = course['name']
    webdav_folder = course['webdav_folder']
    url = COURSE_URL_TEMPLATE.format(course_id)
    
    try:
        logging.info(f"Processing course '{course_name}' (ID: {course_id}) at URL: {url}")
        logging.info(f"Using WebDAV folder: {webdav_folder}")
        
        # Load previous tree from database
        try:
            old_root = db_manager.load_tree(course_id)
        except Exception as e:
            logging.error(f"Failed to load tree for course {course_name}: {e}", exc_info=True)
            old_root = None
        
        # Build new tree
        try:
            new_root = tree_builder.build_tree(scraper_instance, url, webdav_folder, course_name, course_id, old_root)
        except Exception as e:
            logging.error(f"Failed to build tree for course {course_name}: {e}", exc_info=True)
            return [], [], False
        
        # Compare trees
        changes = []
        try:
            changes = differ.diff_trees(old_root, new_root)
            if changes:
                # Log changes to database
                try:
                    db_manager.log_changes(course_id, changes)
                except Exception as e:
                    logging.error(f"Failed to log changes for course {course_name}: {e}", exc_info=True)

                # Archive deleted files: move from live WebDAV path to .versions/_deleted/
                if old_root and scraper_instance.webdav_uploader:
                    old_files_by_path = _flatten_files(old_root)
                    wf = '/' + webdav_folder.strip('/')
                    for change in changes:
                        if change.change_type == 'deleted_file':
                            full_path = '/' + (wf.rstrip('/') + '/' + change.file_path.lstrip('/')).strip('/')
                            dst_path = f"{wf}/.versions/_deleted/{change.file_path.lstrip('/')}"
                            old_file = old_files_by_path.get(full_path)
                            moved = scraper_instance.webdav_uploader.move_file(full_path, dst_path)
                            try:
                                db_manager.save_file_version(
                                    course_id=course_id,
                                    file_path=change.file_path,
                                    version_webdav_path=dst_path if moved else None,
                                    change_type='deleted',
                                    display_name=change.display_name or change.file_path.split('/')[-1],
                                    redirect_url=old_file.redirect_url if old_file else None,
                                )
                            except Exception as e:
                                logging.warning(f"Failed to save deleted version record for {change.file_path}: {e}")

                for change in changes:
                    print(f"{change} (Course: {course_name})")
        except Exception as e:
            logging.error(f"Failed to diff trees for course {course_name}: {e}", exc_info=True)
        
        # Fetch announcements and detect new ones
        new_announcements = []
        try:
            # Get the latest announcement date from database
            latest_date = db_manager.get_latest_announcement_date(course_id)
            
            announcements_scraper = AnnouncementsScraper(scraper_instance.session)
            announcements = announcements_scraper.fetch_announcements(course_id)
            
            if announcements:
                # Filter for new announcements
                for announcement in announcements:
                    if latest_date is None:
                        # No previous announcements, all are new
                        new_announcements.append(announcement)
                    elif announcement.get('pub_date'):
                        # Compare publication dates
                        ann_date = announcement['pub_date']
                        if isinstance(ann_date, str):
                            from email.utils import parsedate_to_datetime
                            ann_date = parsedate_to_datetime(ann_date)
                        
                        # Check if this announcement is newer
                        if ann_date and ann_date > latest_date:
                            new_announcements.append(announcement)
                
                # Save all announcements to database
                db_manager.save_announcements(course_id, announcements)
                
                if new_announcements:
                    logging.info(f"Found {len(new_announcements)} new announcement(s) for course {course_name}")
                    for announcement in new_announcements:
                        print(f"📢 New Announcement: {announcement['title']} (Course: {course_name})")
                else:
                    logging.info(f"Fetched {len(announcements)} announcements, but none are new for course {course_name}")
            else:
                logging.info(f"No announcements found for course {course_name}")
        except Exception as e:
            logging.error(f"Failed to fetch announcements for course {course_name}: {e}", exc_info=True)
        
        # Print and save the new tree
        try:
            print_tree(new_root, course_name)
        except Exception as e:
            logging.error(f"Failed to print tree for course {course_name}: {e}", exc_info=True)
        
        try:
            db_manager.save_tree(course_id, new_root)
            logging.info(f"Successfully saved tree for course {course_name}")
        except Exception as e:
            logging.error(f"Failed to save tree for course {course_name}: {e}", exc_info=True)
            return changes, new_announcements, False
        
        return changes, new_announcements, True
        
    except Exception as e:
        logging.error(f"Failed to process course {course_name} (ID: {course_id}): {e}", exc_info=True)
        return [], [], False


def run_checker(db_manager: DatabaseManager):
    """Main logic for checking courses for updates."""
    try:
        # Set check status to active
        db_manager.set_check_status(True)
        db_manager.log_check_event("check_start", "Starting check for all courses", status="info")
        
        courses = db_manager.get_courses()
        if not courses:
            logging.warning("No courses found in the database. Nothing to do.")
            db_manager.log_check_event("check_end", "No courses found", status="warning")
            db_manager.set_check_status(False)
            return

        # Initialize WebDAV uploader (required)
        webdav_config = db_manager.get_webdav_config()
        if not webdav_config:
            raise RuntimeError("WebDAV must be configured to check courses")
        
        try:
            webdav_uploader = WebDAVUploader(webdav_config)
            if not webdav_uploader.test_connection():
                raise RuntimeError("WebDAV connection test failed")
            logging.info("WebDAV connection established")
        except Exception as e:
            logging.error(f"Failed to initialize WebDAV uploader: {e}", exc_info=True)
            raise RuntimeError(f"WebDAV is required but not available: {e}")

        scraper_instance = Scraper(db_manager, webdav_uploader)
        all_changes = {}
        all_announcements = {}

        for course in courses:
            print("")
            db_manager.set_check_status(True, course['id'])
            db_manager.log_check_event("course_check_start", f"Checking course: {course['name']}", course_id=course['id'], status="info")
            changes, new_announcements, success = process_course(db_manager, course, scraper_instance)
            if success:
                if changes:
                    all_changes[course['name']] = changes
                if new_announcements:
                    all_announcements[course['name']] = new_announcements
                
                total_updates = len(changes) + len(new_announcements)
                if total_updates > 0:
                    db_manager.log_check_event("course_check_complete", f"Found {len(changes)} file change(s) and {len(new_announcements)} new announcement(s)", course_id=course['id'], status="success")
                else:
                    db_manager.log_check_event("course_check_complete", "No changes detected", course_id=course['id'], status="info")
            else:
                db_manager.log_check_event("course_check_complete", "Check failed", course_id=course['id'], status="error")

        # Check global feeds
        preferences = db_manager.get_preferences()
        ann_scraper = AnnouncementsScraper(scraper_instance.session)

        for feed_key, feed_info in GLOBAL_FEEDS.items():
            enabled_pref = f"global_feed_{feed_key}_enabled"
            if not preferences.get(enabled_pref, False):
                logging.debug(f"Global feed '{feed_key}' is disabled, skipping")
                continue

            db_manager.log_check_event("global_feed_check", f"Checking global feed: {feed_info['name']}", status="info")
            try:
                announcements = ann_scraper.fetch_global_feed(feed_key)
                if announcements:
                    latest = db_manager.get_latest_global_announcement_date(feed_key)
                    new_for_feed = []
                    for ann in announcements:
                        if latest is None:
                            new_for_feed.append(ann)
                        elif ann.get('pub_date') and ann['pub_date'] > latest:
                            new_for_feed.append(ann)
                    db_manager.save_global_announcements(feed_key, announcements)
                    if new_for_feed:
                        all_announcements[feed_info['name']] = new_for_feed
                        logging.info(f"Found {len(new_for_feed)} new announcement(s) for global feed '{feed_key}'")
                        for ann in new_for_feed:
                            print(f"\U0001f4e2 New Global Announcement [{feed_info['name']}]: {ann['title']}")
            except Exception as e:
                logging.error(f"Failed to process global feed '{feed_key}': {e}", exc_info=True)

        if all_changes or all_announcements:
            try:
                changes_count = len(all_changes)
                announcements_count = len(all_announcements)
                print(f"\nAttempting to send webhook with updates from {max(changes_count, announcements_count)} course(s).")
                send_webhook(all_changes, all_announcements, db_manager)
                db_manager.log_check_event("webhook_sent", f"Webhook sent for {changes_count} course(s) with file changes and {announcements_count} course(s) with new announcements", status="success")
            except Exception as e:
                logging.error(f"Failed to send webhook notification: {e}", exc_info=True)
                db_manager.log_check_event("webhook_failed", f"Failed to send webhook: {str(e)}", status="error")
        print("")
        
        db_manager.log_check_event("check_end", f"Check completed. {len(all_changes)} course(s) with changes", status="success")
        db_manager.set_check_status(False)
        
    except Exception as e:
        logging.error(f"Critical error in run_checker: {e}", exc_info=True)
        db_manager.log_check_event("check_error", f"Critical error: {str(e)}", status="error")
        db_manager.set_check_status(False)


def check_single_course(db_manager: DatabaseManager, course_id: int) -> dict:
    """Check a single course for updates and return the result."""
    try:
        # Set check status to active
        db_manager.set_check_status(True, course_id)
        
        # Get course info
        courses = db_manager.get_courses()
        course = next((c for c in courses if c['id'] == course_id), None)
        
        if not course:
            db_manager.set_check_status(False)
            return {
                'success': False,
                'error': f'Course ID {course_id} not found'
            }
        
        course_name = course['name']
        logging.info(f"Checking course '{course_name}' (ID: {course_id})")
        db_manager.log_check_event("course_check_start", f"Checking course: {course_name}", course_id=course_id, status="info")
        
        # Initialize WebDAV uploader (required)
        webdav_config = db_manager.get_webdav_config()
        if not webdav_config:
            db_manager.log_check_event("course_check_error", "WebDAV must be configured", course_id=course_id, status="error")
            db_manager.set_check_status(False)
            return {"success": False, "error": "WebDAV must be configured to check courses"}
        
        try:
            webdav_uploader = WebDAVUploader(webdav_config)
            if not webdav_uploader.test_connection():
                raise RuntimeError("WebDAV connection test failed")
            logging.info("WebDAV connection established")
        except Exception as e:
            logging.error(f"Failed to initialize WebDAV uploader: {e}", exc_info=True)
            db_manager.log_check_event("course_check_error", f"WebDAV connection failed: {e}", course_id=course_id, status="error")
            db_manager.set_check_status(False)
            return {"success": False, "error": f"WebDAV is required but not available: {e}"}
        
        scraper_instance = Scraper(db_manager, webdav_uploader)
        
        # Process the course using shared logic
        changes, new_announcements, success = process_course(db_manager, course, scraper_instance)
        
        if not success:
            db_manager.log_check_event("course_check_error", f"Failed to process course: {course_name}", course_id=course_id, status="error")
            db_manager.set_check_status(False)
            return {
                'success': False,
                'error': 'Failed to process course'
            }
        
        # Send webhook if changes or announcements detected
        if changes or new_announcements:
            try:
                all_changes = {course_name: changes} if changes else {}
                all_announcements = {course_name: new_announcements} if new_announcements else {}
                send_webhook(all_changes, all_announcements, db_manager)
                db_manager.log_check_event("webhook_sent", f"Webhook sent for course: {course_name}", course_id=course_id, status="success")
            except Exception as e:
                logging.error(f"Failed to send webhook notification: {e}", exc_info=True)
                db_manager.log_check_event("webhook_failed", f"Failed to send webhook: {str(e)}", course_id=course_id, status="error")
            
            db_manager.log_check_event("course_check_complete", f"Found {len(changes)} file change(s) and {len(new_announcements)} new announcement(s)", course_id=course_id, status="success")
        else:
            db_manager.log_check_event("course_check_complete", "No changes detected", course_id=course_id, status="info")
        
        db_manager.set_check_status(False)
        
        total_updates = len(changes) + len(new_announcements)
        return {
            'success': True,
            'changes_detected': total_updates > 0,
            'changes_count': len(changes),
            'announcements_count': len(new_announcements),
            'message': f'Detected {len(changes)} file change(s) and {len(new_announcements)} new announcement(s)' if total_updates > 0 else 'No changes detected'
        }
        
    except Exception as e:
        logging.error(f"Critical error checking course {course_id}: {e}", exc_info=True)
        db_manager.log_check_event("course_check_error", f"Critical error: {str(e)}", course_id=course_id, status="error")
        db_manager.set_check_status(False)
        return {
            'success': False,
            'error': str(e)
        }


