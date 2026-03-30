"""
FastAPI web application for managing tree-eClass.
Provides UI for viewing and managing courses, credentials, and file history.
"""
import logging
import asyncio
import mimetypes
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from app.services.persistence import DatabaseManager
from app.services.tree_builder import Node, File
from app.services.checker import run_checker, check_single_course, print_tree
from app.services.webdav_uploader import WebDAVUploader
from app.services.announcements_scraper import GLOBAL_FEEDS

# Configure logging for systemd
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = FastAPI(title="tree-eClass Manager", version="1.0.0")

# Global lock to prevent parallel checks (threading.Lock works across threads and event loops)
check_lock = threading.Lock()

# Scheduled checker stop event
_scheduler_stop = threading.Event()

# Thread pool executor for running blocking operations
executor = ThreadPoolExecutor(max_workers=1)

# Setup templates and static files
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Add custom Jinja2 filter for timezone conversion
def format_datetime_athens(timestamp_str: str) -> str:
    """Convert UTC timestamp string to Europe/Athens timezone and format it."""
    if not timestamp_str:
        return ""
    try:
        # Parse the timestamp (SQLite returns it as a string)
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        # Convert to Athens timezone
        athens_dt = dt.astimezone(ZoneInfo("Europe/Athens"))
        # Format: "Nov 8, 2025 14:30:45"
        return athens_dt.strftime("%b %d, %Y %H:%M:%S")
    except Exception as e:
        logging.warning(f"Failed to convert timestamp {timestamp_str}: {e}")
        return timestamp_str

def format_change_counts(message: str) -> str:
    """Format change counts with colors: + N − N ~ N"""
    # Parse the message format: "+ 3 − 1 ~ 2"
    match = re.match(r'\+\s*(\d+)\s*−\s*(\d+)\s*~\s*(\d+)', message)
    if not match:
        return message
    
    added, deleted, modified = match.groups()
    # Format with right-aligned numbers (pad to 2 digits) and double space between categories
    added_str = added.rjust(2)
    deleted_str = deleted.rjust(2)
    modified_str = modified.rjust(2)
    html = f'<span class="change-count added">+{added_str}</span>'
    html += f'<span class="change-count deleted">−{deleted_str}</span>'
    html += f'<span class="change-count modified">~{modified_str}</span>'
    return html

def format_change_no_date(change_no: str) -> str:
    """Format change_no (RFC 3339 timestamp) to human-readable full detail."""
    try:
        # Parse the ISO format timestamp
        dt = datetime.fromisoformat(change_no)
        # Format: "November 9, 2025 at 23:52:11.078040"
        date_part = dt.strftime("%B %d, %Y")
        time_part = dt.strftime("%H:%M:%S")
        microseconds = f".{dt.microsecond:06d}"
        return f"{date_part} at {time_part}{microseconds}"
    except Exception as e:
        logging.warning(f"Failed to parse change_no {change_no}: {e}")
        return change_no

def format_timestamp(timestamp_str: str) -> str:
    """Format ISO timestamp to concise date format for file tree display."""
    if not timestamp_str:
        return ""
    try:
        # Parse the timestamp (ISO 8601 format)
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        # Convert to Athens timezone for display
        athens_dt = dt.astimezone(ZoneInfo("Europe/Athens"))
        # Format: "Feb 24, 2026"
        return athens_dt.strftime("%b %d, %Y")
    except Exception as e:
        logging.warning(f"Failed to format timestamp {timestamp_str}: {e}")
        return ""

templates.env.filters["athens_time"] = format_datetime_athens
templates.env.filters["change_counts"] = format_change_counts
templates.env.filters["change_no_date"] = format_change_no_date
templates.env.filters["format_timestamp"] = format_timestamp

def _get_nav_courses():
    try:
        return db_manager.get_courses()
    except Exception:
        return []

def _get_semester_progress():
    """Return semester progress as an integer percentage (0-100), or None."""
    try:
        prefs = db_manager.get_preferences()
        start_str = prefs.get('semester_start')
        end_str = prefs.get('semester_end')
        if not start_str or not end_str:
            return None
        from datetime import date
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
        today = date.today()
        if today < start or today > end:
            return None
        total = (end - start).days
        if total <= 0:
            return None
        elapsed = (today - start).days
        return min(100, max(0, round(elapsed * 100 / total)))
    except Exception:
        return None

templates.env.globals["nav_courses"] = _get_nav_courses
templates.env.globals["semester_progress"] = _get_semester_progress

# Create static directory if it doesn't exist
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Initialize database
db_manager = DatabaseManager()


# ===== HELPER FUNCTIONS =====

def node_to_dict(node: Node, parent_path: str = "") -> dict:
    """Convert a Node to a dictionary for JSON serialization."""
    current_path = f"{parent_path}/{node.name}" if parent_path else node.name
    return {
        "name": node.name,
        "url": node.url,
        "path": current_path,
        "files": [
            {
                "name": f.name,
                "url": f.url,
                "md5_hash": f.md5_hash,
                "etag": f.etag,
                "local_path": f.local_path,
                "redirect_url": f.redirect_url,
            }
            for f in node.files
        ],
        "children": [node_to_dict(child, current_path) for child in node.children]
    }


# ===== ROUTES =====

@app.get("/", response_class=HTMLResponse)
async def index(request: Request,
                view: Optional[str] = None,
                course_id: Optional[str] = None,
                start_date: Optional[str] = None,
                end_date: Optional[str] = None):
    """Main page - unified activity timeline, with optional change-history filter view."""
    try:
        active_view = view if view in ('timeline', 'changes') else 'timeline'
        courses = db_manager.get_courses()

        timeline = []
        changes_data = []
        change_records = []
        selected_course = None

        course_id_int = None
        if course_id and course_id.strip():
            try:
                course_id_int = int(course_id)
            except ValueError:
                pass

        if active_view == 'timeline':
            timeline = db_manager.get_timeline_data(limit=100)
            courses_by_id = {c['id']: c for c in courses}
            changes_data = [
                {
                    "id": item["id"],
                    "changes": item["changes"],
                    "webdav_folder": courses_by_id.get(item["course_id"], {}).get("webdav_folder", ""),
                }
                for item in timeline
                if item["type"] == "change"
            ]
        elif active_view == 'changes':
            change_records = db_manager.get_change_records(
                course_id=course_id_int,
                start_date=start_date,
                end_date=end_date,
            )
            if course_id_int:
                selected_course = next((c for c in courses if c['id'] == course_id_int), None)

        return templates.TemplateResponse("timeline.html", {
            "request": request,
            "active_view": active_view,
            "courses": courses,
            "timeline": timeline,
            "changes_data": changes_data,
            "change_records": change_records,
            "selected_course": selected_course,
            "start_date": start_date or "",
            "end_date": end_date or "",
        })
    except Exception as e:
        logging.error(f"Error loading page: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== COURSE ROUTES =====

@app.post("/api/courses/reorder")
async def reorder_courses(request: Request):
    """Save a new sort order for courses. Body: [id, id, ...] in desired order."""
    try:
        body = await request.json()
        if not isinstance(body, list):
            raise HTTPException(status_code=422, detail="Expected a list of course IDs")
        db_manager.reorder_courses([int(i) for i in body])
        return JSONResponse(content={"status": "ok"})
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error reordering courses: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/courses", response_class=HTMLResponse)
async def list_courses(request: Request):
    """List all courses."""
    try:
        courses = db_manager.get_courses()
        study_summaries = {c['id']: db_manager.get_course_study_summary(c['id']) for c in courses}
        return templates.TemplateResponse("courses.html", {
            "request": request,
            "courses": courses,
            "study_summaries": study_summaries,
        })
    except Exception as e:
        logging.error(f"Error listing courses: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/courses/{course_id}", response_class=HTMLResponse)
async def view_course(request: Request, course_id: int):
    """View a specific course with its file tree."""
    try:
        courses = db_manager.get_courses()
        course = next((c for c in courses if c['id'] == course_id), None)

        if not course:
            logging.warning(f"Course {course_id} not found")
            raise HTTPException(status_code=404, detail="Course not found")

        tree = db_manager.load_tree(course_id)
        timeline = db_manager.get_timeline_data(limit=50, course_id=course_id)
        changes_data = [
            {
                "id": item["id"],
                "changes": item["changes"],
                "webdav_folder": course["webdav_folder"],
            }
            for item in timeline
            if item["type"] == "change"
        ]

        # Load version metadata for the file tree UI
        files_with_versions = db_manager.get_files_with_versions(course_id)
        folders_with_deleted = db_manager.get_folders_with_deleted(course_id)
        study_levels = db_manager.get_file_study_levels(course_id)

        return templates.TemplateResponse("course_detail.html", {
            "request": request,
            "course": course,
            "tree": tree,
            "timeline": timeline,
            "changes_data": changes_data,
            "files_with_versions": files_with_versions,
            "folders_with_deleted": folders_with_deleted,
            "study_levels": study_levels,
        })
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error viewing course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/courses/add")
async def add_course(
    course_id: int = Form(...),
    name: str = Form(...),
    webdav_folder: str = Form(...)
):
    """Add a new course."""
    try:
        db_manager.save_course(course_id, name, webdav_folder)
        logging.info(f"Added course: {name} (ID: {course_id})")
        return RedirectResponse(url="/courses", status_code=303)
    except Exception as e:
        logging.error(f"Error adding course {name} (ID: {course_id}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/courses/{course_id}/delete")
async def delete_course(course_id: int):
    """Delete a course."""
    try:
        db_manager.delete_course(course_id)
        logging.info(f"Deleted course ID: {course_id}")
        return RedirectResponse(url="/courses", status_code=303)
    except Exception as e:
        logging.error(f"Error deleting course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/courses/{course_id}/reset")
async def reset_course(course_id: int):
    """Reset all data for a course (tree, change history, and announcements)."""
    try:
        db_manager.reset_course_data(course_id)
        logging.info(f"Reset data for course ID {course_id}")
        return RedirectResponse(url=f"/courses/{course_id}", status_code=303)
    except Exception as e:
        logging.error(f"Error resetting course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/courses/{course_id}/update")
async def update_course(
    course_id: int,
    name: str = Form(...),
    webdav_folder: str = Form(...)
):
    """Update a course."""
    try:
        db_manager.save_course(course_id, name, webdav_folder)
        logging.info(f"Updated course: {name} (ID: {course_id})")
        return RedirectResponse(url=f"/courses/{course_id}", status_code=303)
    except Exception as e:
        logging.error(f"Error updating course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/{file_path:path}")
async def serve_file(file_path: str):
    """Serve a file from WebDAV storage."""
    try:
        # Get WebDAV configuration
        webdav_config = db_manager.get_webdav_config()
        if not webdav_config:
            raise HTTPException(status_code=503, detail="WebDAV not configured")
        
        # Initialize WebDAV uploader
        webdav_uploader = WebDAVUploader(webdav_config)
        if not webdav_uploader.is_configured():
            raise HTTPException(status_code=503, detail="WebDAV not configured")
        
        # Ensure file path starts with /
        if not file_path.startswith('/'):
            file_path = '/' + file_path
        
        # Download file from WebDAV
        file_data = webdav_uploader.download_file(file_path)
        if file_data is None:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Determine content type from file extension
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type is None:
            content_type = 'application/octet-stream'
        
        # Extract filename from path for Content-Disposition
        filename = file_path.split('/')[-1]
        
        # Encode filename for Content-Disposition header (RFC 2231/5987)
        # Use filename* parameter for proper UTF-8 encoding of non-ASCII characters
        if filename.isascii():
            disposition = f'inline; filename="{filename}"'
        else:
            # Use RFC 2231/5987 encoding for non-ASCII filenames
            encoded_filename = quote(filename)
            # Provide both parameters: simple ASCII fallback and UTF-8 encoded version
            disposition = f'inline; filename="{filename.encode("ascii", "ignore").decode("ascii")}"; filename*=UTF-8\'\'\'{encoded_filename}'
        
        return Response(
            content=file_data,
            media_type=content_type,
            headers={
                'Content-Disposition': disposition
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error serving file {file_path}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/courses/{course_id}/check")
async def check_course(course_id: int, background_tasks: BackgroundTasks):
    """Check a single course for updates."""
    # Check if a check is already running
    if check_lock.locked():
        check_status = db_manager.get_check_status()
        return JSONResponse(
            status_code=409,
            content={
                "detail": "A check is already in progress",
                "is_checking": True,
                "current_course": check_status.get('course_name')
            }
        )

    try:
        # Get course info first to validate it exists
        courses = db_manager.get_courses()
        course = next((c for c in courses if c['id'] == course_id), None)

        if not course:
            return JSONResponse(
                status_code=404,
                content={"detail": f"Course ID {course_id} not found"}
            )

        # Run the check in a background task
        def check_task():
            task_db_manager = DatabaseManager()
            try:
                with check_lock:
                    check_single_course(task_db_manager, course_id)
            except Exception as e:
                logging.error(f"Error in background course check task: {e}", exc_info=True)
            finally:
                task_db_manager.close()

        background_tasks.add_task(check_task)
        
        return JSONResponse(content={
            "status": "started",
            "message": f"Check started for {course['name']}"
        })
        
    except Exception as e:
        logging.error(f"Error checking course {course_id}: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )




@app.get("/api/announcements")
async def api_announcements(course_id: Optional[int] = None, limit: int = 50):
    """API endpoint to get announcements as JSON."""
    try:
        announcements = db_manager.get_announcements(course_id=course_id, limit=limit)
        return JSONResponse(content={"announcements": announcements})
    except Exception as e:
        logging.error(f"Error getting announcements: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )


# ===== EXERCISES ROUTES =====

_GREEK_MONTHS = {
    'Ιανουαρίου': 1, 'Φεβρουαρίου': 2, 'Μαρτίου': 3, 'Απριλίου': 4,
    'Μαΐου': 5, 'Ιουνίου': 6, 'Ιουλίου': 7, 'Αυγούστου': 8,
    'Σεπτεμβρίου': 9, 'Οκτωβρίου': 10, 'Νοεμβρίου': 11, 'Δεκεμβρίου': 12,
}

def _parse_greek_deadline(deadline_str: str):
    """Parse a Greek deadline string to a timezone-aware datetime, or None."""
    if not deadline_str:
        return None
    try:
        import re as _re
        from datetime import datetime as _dt, timedelta as _td

        cleaned = " ".join(deadline_str.split())

        # Relative forms seen in eClass list, e.g. "αύριο - 11:55 μ.μ."
        rel = _re.search(
            r'(σήμερα|αύριο|μεθαύριο)\s*-\s*(\d+):(\d+)\s*(μ\.μ\.|π\.μ\.)',
            cleaned,
            flags=_re.IGNORECASE,
        )
        if rel:
            rel_word, hour, minute, period = rel.groups()
            now = _dt.now(tz=ZoneInfo('Europe/Athens'))
            base_date = now.date()
            rel_word = rel_word.lower()
            if rel_word == 'αύριο':
                base_date = base_date + _td(days=1)
            elif rel_word == 'μεθαύριο':
                base_date = base_date + _td(days=2)

            hour, minute = int(hour), int(minute)
            if period == 'μ.μ.' and hour != 12:
                hour += 12
            elif period == 'π.μ.' and hour == 12:
                hour = 0

            return _dt(
                base_date.year,
                base_date.month,
                base_date.day,
                hour,
                minute,
                tzinfo=ZoneInfo('Europe/Athens'),
            )

        # e.g. "Τετάρτη, 25 Μαρτίου 2026 - 11:55 μ.μ."
        m = _re.search(r'(\d+)\s+(\S+)\s+(\d{4})\s*-\s*(\d+):(\d+)\s*(μ\.μ\.|π\.μ\.)', cleaned)
        if not m:
            return None
        day, month_gr, year, hour, minute, period = m.groups()
        month = _GREEK_MONTHS.get(month_gr)
        if not month:
            return None
        hour, minute = int(hour), int(minute)
        if period == 'μ.μ.' and hour != 12:
            hour += 12
        elif period == 'π.μ.' and hour == 12:
            hour = 0
        return _dt(int(year), month, int(day), hour, minute,
                   tzinfo=ZoneInfo('Europe/Athens'))
    except Exception:
        return None

def _deadline_urgency(deadline_dt, submitted: bool):
    """Return (urgency_class, time_label) for a deadline datetime."""
    from datetime import datetime as _dt, timedelta as _td
    if submitted:
        return 'ex-submitted', None
    if deadline_dt is None:
        return 'ex-unknown', None
    now = _dt.now(tz=ZoneInfo('Europe/Athens'))
    delta = deadline_dt - now
    total_seconds = delta.total_seconds()
    if total_seconds < 0:
        return 'ex-overdue', 'Overdue'
    days = int(total_seconds // 86400)
    hours = int((total_seconds % 86400) // 3600)
    minutes = int((total_seconds % 3600) // 60)
    if total_seconds < 86400:           # < 1 day
        urgency = 'ex-critical'
        label = f'{hours}h {minutes}m left'
    elif total_seconds < 3 * 86400:     # < 3 days
        urgency = 'ex-warning'
        label = f'{days}d {hours}h left'
    elif total_seconds < 7 * 86400:     # < 7 days
        urgency = 'ex-moderate'
        label = f'{days}d {hours}h left'
    else:
        urgency = 'ex-ok'
        label = f'{days}d left'
    return urgency, label


@app.get("/exercises", response_class=HTMLResponse)
async def list_exercises(request: Request, course_id: Optional[int] = None):
    """List exercises sorted by deadline urgency."""
    try:
        courses = db_manager.get_courses()
        exercises = db_manager.get_exercises(course_id=course_id, limit=500)

        # Annotate each exercise with parsed deadline and urgency
        for ex in exercises:
            dl_dt = _parse_greek_deadline(ex.get('deadline', ''))
            submitted = ex.get('submission_status') == 'submitted'
            urgency, time_label = _deadline_urgency(dl_dt, submitted)
            ex['_deadline_dt'] = dl_dt
            ex['_urgency'] = urgency
            ex['_time_label'] = time_label
            ex['_deadline_short'] = dl_dt.strftime('%-d %b · %H:%M') if dl_dt else ''

        # Sort: overdue first, then by closest deadline; submitted last
        urgency_order = {'ex-overdue': 0, 'ex-critical': 1, 'ex-warning': 2,
                         'ex-moderate': 3, 'ex-ok': 4, 'ex-submitted': 5, 'ex-unknown': 6}

        def _sort_key(ex):
            u = urgency_order.get(ex['_urgency'], 9)
            dt = ex['_deadline_dt']
            if dt is None:
                return (u, datetime.max.replace(tzinfo=ZoneInfo('Europe/Athens')))
            return (u, dt)

        exercises.sort(key=_sort_key)

        return templates.TemplateResponse("exercises.html", {
            "request": request,
            "exercises": exercises,
            "courses": courses,
            "selected_course_id": course_id,
        })
    except Exception as e:
        logging.error(f"Error listing exercises: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/exercises/{course_id}/{exercise_id}/ignore")
async def ignore_exercise(course_id: int, exercise_id: str):
    """Mark an exercise as ignored; it will no longer appear in the UI."""
    try:
        db_manager.ignore_exercise(course_id, exercise_id)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error ignoring exercise {exercise_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== SETTINGS ROUTES =====

@app.get("/settings", response_class=HTMLResponse)
async def view_settings(request: Request):
    """View and edit settings (credentials, email, and preferences)."""
    try:
        credentials = db_manager.get_credentials()
        # Scrub credentials password payload
        safe_credentials = (credentials[0], "") if credentials else None
        
        webhook_config = db_manager.get_webhook_config()
        # Scrub webhook URL payload
        if webhook_config and "webhook_url" in webhook_config:
            webhook_config["webhook_url"] = ""

        webdav_config = db_manager.get_webdav_config()
        # Scrub webdav password payload
        if webdav_config and "password" in webdav_config:
            webdav_config["password"] = ""

        preferences = db_manager.get_preferences()
        return templates.TemplateResponse("settings.html", {
            "request": request,
            "credentials": safe_credentials,
            "webhook_config": webhook_config,
            "webdav_config": webdav_config,
            "preferences": preferences,
            "global_feeds": GLOBAL_FEEDS,
        })
    except Exception as e:
        logging.error(f"Error viewing settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/settings/credentials")
async def update_credentials(
    username: str = Form(...),
    password: Optional[str] = Form(None)
):
    """Update credentials."""
    try:
        final_password = password
        if not final_password:
            existing = db_manager.get_credentials()
            final_password = existing[1] if existing else ""
            
        db_manager.save_credentials(username, final_password)
        logging.info(f"Updated credentials for user: {username}")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating credentials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/settings/webhook")
async def update_webhook_config(
    webhook_url: Optional[str] = Form(None)
):
    """Update webhook configuration."""
    try:
        final_url = webhook_url
        if not final_url:
            existing = db_manager.get_webhook_config()
            final_url = existing['webhook_url'] if existing else ""

        db_manager.save_webhook_config(webhook_url=final_url)
        logging.info("Updated webhook configuration")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating webhook configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/settings/webdav")
async def update_webdav_config(
    webdav_hostname: str = Form(...),
    webdav_username: str = Form(...),
    webdav_password: Optional[str] = Form(None),
    webdav_disable_check: Optional[bool] = Form(False)
):
    """Update WebDAV configuration."""
    try:
        final_password = webdav_password
        if not final_password:
            existing = db_manager.get_webdav_config()
            final_password = existing['password'] if existing else ""
            
        db_manager.save_webdav_config(
            hostname=webdav_hostname,
            username=webdav_username,
            password=final_password,
            disable_check=webdav_disable_check,
            timeout=30
        )
        logging.info("Updated WebDAV configuration")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating WebDAV configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/settings/preferences")
async def update_preferences(
    check_interval_minutes: int = Form(60),
    max_concurrent_downloads: int = Form(3),
    request_timeout_seconds: int = Form(30),
    retry_attempts: int = Form(3),
    notification_enabled: Optional[bool] = Form(False),
    notification_on_error: Optional[bool] = Form(False),
    global_feed_dept_enabled: Optional[bool] = Form(False),
    global_feed_undergrad_enabled: Optional[bool] = Form(False),
    global_feed_rector_enabled: Optional[bool] = Form(False),
    semester_start: Optional[str] = Form(None),
    semester_end: Optional[str] = Form(None),
):
    """Update application preferences."""
    try:
        db_manager.save_preferences(
            check_interval_minutes=check_interval_minutes,
            max_concurrent_downloads=max_concurrent_downloads,
            request_timeout_seconds=request_timeout_seconds,
            retry_attempts=retry_attempts,
            notification_enabled=notification_enabled,
            notification_on_error=notification_on_error,
            global_feed_dept_enabled=global_feed_dept_enabled,
            global_feed_undergrad_enabled=global_feed_undergrad_enabled,
            global_feed_rector_enabled=global_feed_rector_enabled,
            semester_start=semester_start or None,
            semester_end=semester_end or None,
        )
        logging.info("Updated preferences")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating preferences: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/courses/{course_id}/changes/{change_no}", response_class=HTMLResponse)
async def view_change_record(request: Request, course_id: int, change_no: str):
    """View details of a specific change identified by course and per-course change_no."""
    try:
        change_record = db_manager.get_change_record_by_course_and_no(course_id, change_no)

        if not change_record:
            logging.warning(f"Change record change_no={change_no} for course {course_id} not found")
            raise HTTPException(status_code=404, detail="Change record not found")

        # Fetch items by change_record id
        changes = db_manager.get_change_record_items(change_record['id'])

        course = next((c for c in db_manager.get_courses() if c['id'] == course_id), None)
        webdav_folder = course['webdav_folder'] if course else ''

        return templates.TemplateResponse("change_detail.html", {
            "request": request,
            "change_record": change_record,
            "changes": changes,
            "webdav_folder": webdav_folder,
        })
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error viewing change record course={course_id} change_no={change_no}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== CHECKER ROUTES =====

def run_checker_in_thread():
    """Wrapper function to run checker in a thread with its own DB connection."""
    # Create database manager in this thread
    thread_db_manager = DatabaseManager()
    try:
        run_checker(thread_db_manager)
    except Exception as e:
        logging.error(f"Error in checker thread: {e}", exc_info=True)
        # Make sure status is cleared on error
        try:
            thread_db_manager.set_check_status(False)
        except:
            pass
    finally:
        thread_db_manager.close()

def run_checker_task():
    """Background task to run the checker in a thread with lock protection."""
    try:
        with check_lock:
            logging.info("Starting manual check task")
            run_checker_in_thread()
            logging.info("Manual check task completed")
    except Exception as e:
        logging.error(f"Error in background checker task: {e}", exc_info=True)


def _scheduled_checker_loop():
    """Background thread that runs the checker on a schedule."""
    while not _scheduler_stop.is_set():
        # Read interval from preferences each cycle so changes take effect
        try:
            sched_db = DatabaseManager()
            prefs = sched_db.get_preferences()
            sched_db.close()
            interval_minutes = prefs.get('check_interval_minutes', 60)
        except Exception:
            interval_minutes = 60

        interval_seconds = max(interval_minutes, 1) * 60
        logging.info(f"Scheduled check will run in {interval_minutes} minute(s)")

        # Wait for the interval, checking the stop event periodically
        if _scheduler_stop.wait(timeout=interval_seconds):
            break  # stop event was set

        # Run the check (skip if a manual check is already running)
        if check_lock.acquire(blocking=False):
            try:
                logging.info("Starting scheduled check")
                run_checker_in_thread()
                logging.info("Scheduled check completed")
            finally:
                check_lock.release()
        else:
            logging.info("Scheduled check skipped — a check is already in progress")


@app.on_event("startup")
def start_scheduled_checker():
    """Start the scheduled checker thread when the app starts."""
    _scheduler_stop.clear()
    t = threading.Thread(target=_scheduled_checker_loop, daemon=True, name="scheduled-checker")
    t.start()
    logging.info("Scheduled checker started")


@app.on_event("shutdown")
def stop_scheduled_checker():
    """Stop the scheduled checker thread when the app shuts down."""
    _scheduler_stop.set()
    logging.info("Scheduled checker stopped")

@app.post("/run-check")
async def run_check(background_tasks: BackgroundTasks):
    """Manually trigger a check for all courses."""
    # Check if a check is already running
    if check_lock.locked():
        check_status = db_manager.get_check_status()
        return JSONResponse(
            status_code=409,
            content={
                "detail": "A check is already in progress",
                "is_checking": True,
                "current_course": check_status.get('course_name')
            }
        )
    
    try:
        # Set the status immediately so the UI can reflect it
        db_manager.set_check_status(True)
        
        # Schedule the check task
        background_tasks.add_task(run_checker_task)
        logging.info("Manual check triggered via web interface")
        return JSONResponse({
            "status": "success",
            "message": "Check started in background"
        })
    except Exception as e:
        logging.error(f"Error triggering manual check: {e}", exc_info=True)
        # Clear status on error
        db_manager.set_check_status(False)
        raise HTTPException(status_code=500, detail=str(e))


# ===== API ROUTES (for AJAX calls) =====

@app.get("/api/courses/{course_id}/file-versions")
async def api_file_versions(course_id: int, file_path: str):
    """Return all archived versions of a specific file (modified history)."""
    try:
        versions = db_manager.get_file_versions(course_id, file_path=file_path, change_type='modified')
        return JSONResponse(content={"versions": versions})
    except Exception as e:
        logging.error(f"API error getting file versions for course {course_id} path {file_path}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/courses/{course_id}/deleted-files")
async def api_deleted_files(course_id: int, folder: Optional[str] = None):
    """Return all deleted files for a course, optionally filtered to a folder subtree."""
    try:
        deleted = db_manager.get_file_versions(course_id, change_type='deleted')
        if folder is not None:
            prefix = folder.rstrip('/') + '/' if folder else ''
            deleted = [
                d for d in deleted
                if (folder == '' or d['file_path'].startswith(prefix) or
                    d['file_path'] == folder)
            ]
        return JSONResponse(content={"deleted": deleted})
    except Exception as e:
        logging.error(f"API error getting deleted files for course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/courses")
async def api_list_courses():
    """API endpoint to get all courses."""
    try:
        return db_manager.get_courses()
    except Exception as e:
        logging.error(f"API error listing courses: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/courses/{course_id}/tree")
async def api_get_tree(course_id: int):
    """API endpoint to get course tree."""
    try:
        tree = db_manager.load_tree(course_id)
        if not tree:
            return None
        return node_to_dict(tree)
    except Exception as e:
        logging.error(f"API error getting tree for course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def api_stats():
    """API endpoint to get system statistics."""
    try:
        courses = db_manager.get_courses()
        # Count total change records instead of individual changes
        change_records = db_manager.get_change_records(limit=10000)
        
        return {
            "total_courses": len(courses),
            "total_changes": len(change_records),
            "credentials_configured": db_manager.get_credentials() is not None
        }
    except Exception as e:
        logging.error(f"API error getting stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/check-status")
async def api_check_status():
    """API endpoint to get the current check status."""
    try:
        status = db_manager.get_check_status()
        return status
    except Exception as e:
        logging.error(f"API error getting check status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ===== STUDY TRACKING ROUTES =====

@app.get("/study", response_class=HTMLResponse)
async def study_inbox(request: Request):
    """Study inbox: priority-sorted list of unmastered files + session log."""
    try:
        courses = db_manager.get_courses()
        inbox = db_manager.get_study_inbox(limit=60)
        return templates.TemplateResponse("study.html", {
            "request": request,
            "inbox": inbox,
            "courses": courses,
        })
    except Exception as e:
        logging.error(f"Error loading study inbox: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/study/inbox")
async def api_study_inbox(limit: int = 50):
    """Return priority-sorted unmastered files as JSON."""
    try:
        inbox = db_manager.get_study_inbox(limit=limit)
        return JSONResponse(content={"inbox": inbox})
    except Exception as e:
        logging.error(f"API error getting study inbox: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/courses/{course_id}/files/study-level")
async def set_study_level(course_id: int, request: Request):
    """Set the comprehension level (0-4) for a file."""
    try:
        body = await request.json()
        file_path = body.get("file_path")
        level = body.get("level")
        if file_path is None or level is None:
            raise HTTPException(status_code=422, detail="file_path and level are required")
        level = int(level)
        if level < 0 or level > 5:
            raise HTTPException(status_code=422, detail="level must be between 0 and 5")
        db_manager.set_file_study_level(course_id, file_path, level)
        return JSONResponse(content={"status": "ok", "level": level})
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"API error setting study level course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    uvicorn.run(
        "app.web.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

