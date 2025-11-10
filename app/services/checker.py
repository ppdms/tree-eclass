"""
Main entry point for the tree-eclass checker application.
Orchestrates the scraping, diffing, and notification process.
"""
import logging
import os
import shutil
import smtplib
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.services import differ, tree_builder
from app.services.persistence import DatabaseManager
from app.services.scraper import Scraper, COURSE_URL_TEMPLATE
from app.services.tree_renderer import render_tree_for_email


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


# Email notification function
def send_email(changes_by_course: dict, db_manager: DatabaseManager):
    """Constructs and sends an HTML email with the detected changes using SMTP."""
    # Get email configuration from database
    email_config = db_manager.get_email_config()
    
    if not email_config:
        logging.warning("Email configuration not found in database. Email not sent.")
        print("Email configuration not set. Please configure email settings in the web interface.")
        return
    
    # Process each course's changes and get the latest change record
    email_data = []
    for course_name, changes in changes_by_course.items():
        if not changes:
            continue
        
        # Get course info to find the latest change record
        courses = db_manager.get_courses()
        course = next((c for c in courses if c['name'] == course_name), None)
        if not course:
            continue
        
        # Get the most recent change record for this course
        change_records = db_manager.get_change_records(course_id=course['id'], limit=1)
        if not change_records:
            continue
        
        change_record = change_records[0]
        
        # Parse changes to build list for tree renderer
        tree_changes = []
        for change in changes:
            change_type = None
            path = change
            
            if change.startswith("Added file:"):
                change_type = "added_file"
                path = change.replace("Added file:", "").strip()
            elif change.startswith("Deleted file:"):
                change_type = "deleted_file"
                path = change.replace("Deleted file:", "").strip()
            elif change.startswith("Modified file:"):
                change_type = "modified_file"
                path = change.replace("Modified file:", "").strip()
            elif change.startswith("Added directory:"):
                change_type = "added_directory"
                path = change.replace("Added directory:", "").strip()
            elif change.startswith("Deleted directory:"):
                change_type = "deleted_directory"
                path = change.replace("Deleted directory:", "").strip()
            else:
                change_type = "unknown"
            
            tree_changes.append({
                'change_type': change_type,
                'file_path': path
            })
        
        email_data.append({
            'course': course,
            'change_record': change_record,
            'tree_changes': tree_changes
        })
    
    if not email_data:
        logging.info("No change records found for email")
        return
    
    # Use the first course's data for the subject (or combine if multiple)
    if len(email_data) == 1:
        first_record = email_data[0]['change_record']
        first_course = email_data[0]['course']
        subject = f"Change Detected: {first_course['name']} ({first_record['change_no']})"
    else:
        subject = f"Changes Detected in {len(email_data)} Courses"
    
    # Build HTML email
    html_content = build_html_email(email_data)
    
    # Build plain text fallback
    plain_content = build_plain_email(changes_by_course)
    
    print("\n=== Email Content ===")
    print(f"To: {email_config['to_email']}")
    print(f"From: {email_config['from_email']}")
    print(f"Subject: {subject}")
    print("===================\n")
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = email_config['from_email']
        msg['To'] = email_config['to_email']
        msg['Subject'] = subject
        
        # Attach both plain text and HTML versions
        msg.attach(MIMEText(plain_content, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))
        
        # Connect to SMTP server and send
        if email_config['use_tls']:
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            server.starttls()
        else:
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
        
        if email_config['smtp_username'] and email_config['smtp_password']:
            server.login(email_config['smtp_username'], email_config['smtp_password'])
        
        server.send_message(msg)
        server.quit()
        
        logging.info("Email notification sent successfully")
        print("✓ Email sent successfully")
        
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error while sending email: {e}", exc_info=True)
        print(f"Failed to send email (SMTP error): {e}", file=sys.stderr)
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
        print(f"Failed to send email: {e}", file=sys.stderr)


def build_plain_email(changes_by_course: dict) -> str:
    """Build plain text email content."""
    content = "File system changes detected:\n\n"
    
    for course_name, changes in changes_by_course.items():
        if not changes:
            continue
        
        content += f"=== Course: {course_name} ===\n"
        for change in changes:
            content += f"- {change}\n"
        content += "\n"
    
    return content


def build_html_email(email_data: list) -> str:
    """Build modern HTML email content with styling."""
    
    # Start HTML with inline CSS
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
            background-color: #f8fafc;
            color: #1e293b;
            line-height: 1.5;
            font-size: 14px;
            margin: 0;
            padding: 20px;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }
        .header {
            background: #2563eb;
            color: white;
            padding: 24px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 24px;
            font-weight: 600;
        }
        .content {
            padding: 24px;
        }
        .course-section {
            margin-bottom: 32px;
            border-bottom: 1px solid #e2e8f0;
            padding-bottom: 24px;
        }
        .course-section:last-child {
            border-bottom: none;
            margin-bottom: 0;
        }
        .meta-row {
            display: flex;
            gap: 12px;
            margin-bottom: 8px;
            font-size: 14px;
        }
        .meta-label {
            font-weight: 600;
            color: #64748b;
            min-width: 80px;
        }
        .meta-value {
            color: #1e293b;
        }
        .meta-value a {
            color: #2563eb;
            text-decoration: none;
        }
        .meta-value a:hover {
            text-decoration: underline;
        }
        .changes-summary {
            display: inline-flex;
            gap: 8px;
            font-family: 'Roboto Mono', monospace;
            font-size: 13px;
            align-items: center;
        }
        .change-count {
            font-weight: 600;
            white-space: nowrap;
        }
        .change-count.added {
            color: #16a34a;
        }
        .change-count.deleted {
            color: #dc2626;
        }
        .change-count.modified {
            color: #ea580c;
        }
        .legend {
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 12px 16px;
            margin: 16px 0;
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            font-size: 13px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
            white-space: nowrap;
        }
        .legend-item strong {
            font-family: 'Roboto Mono', monospace;
            font-size: 14px;
        }
        .legend-item.added { color: #16a34a; }
        .legend-item.deleted { color: #dc2626; }
        .legend-item.modified { color: #ea580c; }
        .legend-item.mixed { color: #2563eb; }
        .tree-list {
            list-style: none;
            padding: 0;
            margin: 16px 0;
            font-family: 'Roboto Mono', monospace;
            font-size: 13px;
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 16px;
        }
        .tree-item {
            padding: 4px 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .tree-item .symbol {
            font-weight: bold;
            min-width: 16px;
        }
        .tree-item .icon {
            font-size: 16px;
        }
        .tree-item .path {
            flex: 1;
        }
        .footer {
            background: #f8fafc;
            border-top: 1px solid #e2e8f0;
            padding: 16px 24px;
            text-align: center;
            font-size: 12px;
            color: #64748b;
        }
        h2 {
            margin: 16px 0 12px 0;
            font-size: 18px;
            font-weight: 600;
            color: #1e293b;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📝 Change Detected - tree-eclass</h1>
        </div>
        <div class="content">
"""
    
    # Add each course section
    for data in email_data:
        course = data['course']
        change_record = data['change_record']
        tree_changes = data['tree_changes']
        
        # Format timestamp
        try:
            dt = datetime.fromisoformat(change_record['change_no'])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo('Europe/Athens'))
            formatted_date = dt.strftime('%B %d, %Y at %H:%M:%S')
        except:
            formatted_date = change_record['change_no']
        
        # Parse change counts from message
        message = change_record.get('message', '')
        import re
        match = re.match(r'\+\s*(\d+)\s*−\s*(\d+)\s*~\s*(\d+)', message)
        if match:
            added, deleted, modified = match.groups()
            # Format with right-aligned numbers (pad to 2 digits) and double space between categories
            added_str = added.rjust(2)
            deleted_str = deleted.rjust(2)
            modified_str = modified.rjust(2)
            changes_html = f'<span class="change-count added">+{added_str}</span>&nbsp;&nbsp;<span class="change-count deleted">−{deleted_str}</span>&nbsp;&nbsp;<span class="change-count modified">~{modified_str}</span>'
        else:
            changes_html = message
        
        # Build course section
        html += f"""
            <div class="course-section">
                <div class="meta-row">
                    <span class="meta-label">Course:</span>
                    <span class="meta-value"><strong>{course['name']}</strong></span>
                </div>
                <div class="meta-row">
                    <span class="meta-label">Date:</span>
                    <span class="meta-value">{formatted_date}</span>
                </div>
                <div class="meta-row">
                    <span class="meta-label">Changes:</span>
                    <span class="meta-value changes-summary">{changes_html}</span>
                </div>
                
                <h2>Changes</h2>
                
                <div class="legend">
                    <div class="legend-item added"><strong>+</strong>&nbsp;Added&nbsp;&nbsp;</div>
                    <div class="legend-item deleted"><strong>−</strong>&nbsp;Deleted&nbsp;&nbsp;</div>
                    <div class="legend-item modified"><strong>~</strong>&nbsp;Modified&nbsp;&nbsp;</div>
                    <div class="legend-item mixed"><strong>±</strong>&nbsp;Mixed</div>
                </div>
                
"""
        
        # Use shared tree renderer for email
        html += render_tree_for_email(tree_changes)
        
        html += """            </div>
"""
    
    # Close HTML
    html += """        </div>
        <div class="footer">
            <p>This is an automated notification from tree-eclass Manager</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html


def process_course(db_manager: DatabaseManager, course: dict, scraper_instance: Scraper) -> tuple[list, bool]:
    """
    Process a single course: check for changes, update tree, and log changes.
    
    Returns:
        tuple: (list of changes, success flag)
    """
    course_id = course['id']
    course_name = course['name']
    download_folder = course['download_folder']
    url = COURSE_URL_TEMPLATE.format(course_id)
    
    try:
        logging.info(f"Processing course '{course_name}' (ID: {course_id}) at URL: {url}")
        
        # Clean download folder
        if os.path.exists(download_folder):
            try:
                shutil.rmtree(download_folder)
            except Exception as e:
                logging.error(f"Failed to clean download folder {download_folder}: {e}", exc_info=True)
        
        # Load previous tree from database
        try:
            old_root = db_manager.load_tree(course_id)
        except Exception as e:
            logging.error(f"Failed to load tree for course {course_name}: {e}", exc_info=True)
            old_root = None
        
        # Build new tree
        try:
            new_root = tree_builder.build_tree(scraper_instance, url, download_folder, course_name, old_root)
        except Exception as e:
            logging.error(f"Failed to build tree for course {course_name}: {e}", exc_info=True)
            return [], False
        
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
                
                for change in changes:
                    print(f"{change} (Course: {course_name})")
        except Exception as e:
            logging.error(f"Failed to diff trees for course {course_name}: {e}", exc_info=True)
        
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
            return changes, False
        
        return changes, True
        
    except Exception as e:
        logging.error(f"Failed to process course {course_name} (ID: {course_id}): {e}", exc_info=True)
        return [], False


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

        scraper_instance = Scraper(db_manager)
        all_changes = {}

        for course in courses:
            print("")
            db_manager.set_check_status(True, course['id'])
            db_manager.log_check_event("course_check_start", f"Checking course: {course['name']}", course_id=course['id'], status="info")
            changes, success = process_course(db_manager, course, scraper_instance)
            if success and changes:
                all_changes[course['name']] = changes
                db_manager.log_check_event("course_check_complete", f"Found {len(changes)} change(s)", course_id=course['id'], status="success")
            else:
                db_manager.log_check_event("course_check_complete", "No changes detected", course_id=course['id'], status="info")

        if all_changes:
            try:
                print(f"\nAttempting to send email with changes from {len(all_changes)} course(s).")
                send_email(all_changes, db_manager)
                db_manager.log_check_event("email_sent", f"Email sent for {len(all_changes)} course(s)", status="success")
            except Exception as e:
                logging.error(f"Failed to send email notification: {e}", exc_info=True)
                db_manager.log_check_event("email_failed", f"Failed to send email: {str(e)}", status="error")
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
        
        scraper_instance = Scraper(db_manager)
        
        # Process the course using shared logic
        changes, success = process_course(db_manager, course, scraper_instance)
        
        if not success:
            db_manager.log_check_event("course_check_error", f"Failed to process course: {course_name}", course_id=course_id, status="error")
            db_manager.set_check_status(False)
            return {
                'success': False,
                'error': 'Failed to process course'
            }
        
        # Send email if changes detected
        if changes:
            try:
                send_email({course_name: changes}, db_manager)
                db_manager.log_check_event("email_sent", f"Email sent for course: {course_name}", course_id=course_id, status="success")
            except Exception as e:
                logging.error(f"Failed to send email notification: {e}", exc_info=True)
                db_manager.log_check_event("email_failed", f"Failed to send email: {str(e)}", course_id=course_id, status="error")
            
            db_manager.log_check_event("course_check_complete", f"Found {len(changes)} change(s)", course_id=course_id, status="success")
        else:
            db_manager.log_check_event("course_check_complete", "No changes detected", course_id=course_id, status="info")
        
        db_manager.set_check_status(False)
        
        return {
            'success': True,
            'changes_detected': len(changes) > 0,
            'changes_count': len(changes),
            'message': f'Detected {len(changes)} change(s)' if changes else 'No changes detected'
        }
        
    except Exception as e:
        logging.error(f"Critical error checking course {course_id}: {e}", exc_info=True)
        db_manager.log_check_event("course_check_error", f"Critical error: {str(e)}", course_id=course_id, status="error")
        db_manager.set_check_status(False)
        return {
            'success': False,
            'error': str(e)
        }


def main():
    """Sets up and runs the scheduled checker."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    try:
        db_manager = DatabaseManager()
        
        def scheduled_run():
            run_checker(db_manager)
            # Schedule the next run
            threading.Timer(3600, scheduled_run).start()

        print("tree-eclass checker started. Scheduled tasks running in background.")
        scheduled_run()

    except Exception as e:
        logging.critical(f"A critical error occurred during setup: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()