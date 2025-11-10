"""
FastAPI web application for managing tree-eclass.
Provides UI for viewing and managing courses, credentials, and file history.
"""
import logging
import asyncio
from datetime import datetime
from typing import List, Optional
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from app.services.persistence import DatabaseManager
from app.services.tree_builder import Node, File
from app.services.checker import run_checker, check_single_course, print_tree

# Configure logging for systemd
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = FastAPI(title="tree-eclass Manager", version="1.0.0")

# Global lock to prevent parallel checks
check_lock = asyncio.Lock()

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
    import re
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

templates.env.filters["athens_time"] = format_datetime_athens
templates.env.filters["change_counts"] = format_change_counts
templates.env.filters["change_no_date"] = format_change_no_date

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
                "etag": f.etag
            }
            for f in node.files
        ],
        "children": [node_to_dict(child, current_path) for child in node.children]
    }


# ===== ROUTES =====

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, course_id: Optional[str] = None):
    """Main page - shows change history."""
    return await view_history(request, course_id)


# ===== COURSE ROUTES =====

@app.get("/courses", response_class=HTMLResponse)
async def list_courses(request: Request):
    """List all courses."""
    try:
        courses = db_manager.get_courses()
        return templates.TemplateResponse("courses.html", {
            "request": request,
            "courses": courses
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
        
        return templates.TemplateResponse("course_detail.html", {
            "request": request,
            "course": course,
            "tree": tree
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
    download_folder: str = Form(...)
):
    """Add a new course."""
    try:
        db_manager.save_course(course_id, name, download_folder)
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
    """Reset all data for a course (tree and change history)."""
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
    download_folder: str = Form(...)
):
    """Update a course."""
    try:
        db_manager.save_course(course_id, name, download_folder)
        logging.info(f"Updated course: {name} (ID: {course_id})")
        return RedirectResponse(url=f"/courses/{course_id}", status_code=303)
    except Exception as e:
        logging.error(f"Error updating course {course_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
                async def run_with_lock():
                    async with check_lock:
                        check_single_course(task_db_manager, course_id)
                
                import asyncio
                asyncio.run(run_with_lock())
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


# ===== LOGS ROUTES =====

@app.get("/logs", response_class=HTMLResponse)
async def view_logs(request: Request, limit: int = 100):
    """View activity logs (checks, emails, etc.)."""
    try:
        logs = db_manager.get_check_logs(limit=limit)
        return templates.TemplateResponse("logs.html", {
            "request": request,
            "logs": logs
        })
    except Exception as e:
        logging.error(f"Error viewing logs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== SETTINGS ROUTES =====

@app.get("/settings", response_class=HTMLResponse)
async def view_settings(request: Request):
    """View and edit settings (credentials, email, and preferences)."""
    try:
        credentials = db_manager.get_credentials()
        email_config = db_manager.get_email_config()
        preferences = db_manager.get_preferences()
        return templates.TemplateResponse("settings.html", {
            "request": request,
            "credentials": credentials,
            "email_config": email_config,
            "preferences": preferences
        })
    except Exception as e:
        logging.error(f"Error viewing settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/settings/credentials")
async def update_credentials(
    username: str = Form(...),
    password: str = Form(...)
):
    """Update credentials."""
    try:
        db_manager.save_credentials(username, password)
        logging.info(f"Updated credentials for user: {username}")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating credentials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/settings/email")
async def update_email_config(
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    smtp_username: str = Form(""),
    smtp_password: str = Form(""),
    from_email: str = Form(...),
    to_email: str = Form(...),
    use_tls: bool = Form(True)
):
    """Update email configuration."""
    try:
        db_manager.save_email_config(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            from_email=from_email,
            to_email=to_email,
            use_tls=use_tls
        )
        logging.info("Updated email configuration")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating email configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/settings/preferences")
async def update_preferences(
    check_interval_minutes: int = Form(60),
    max_concurrent_downloads: int = Form(3),
    request_timeout_seconds: int = Form(30),
    retry_attempts: int = Form(3),
    notification_enabled: bool = Form(True),
    notification_on_error: bool = Form(True)
):
    """Update application preferences."""
    try:
        db_manager.save_preferences(
            check_interval_minutes=check_interval_minutes,
            max_concurrent_downloads=max_concurrent_downloads,
            request_timeout_seconds=request_timeout_seconds,
            retry_attempts=retry_attempts,
            notification_enabled=notification_enabled,
            notification_on_error=notification_on_error
        )
        logging.info("Updated preferences")
        return RedirectResponse(url="/settings", status_code=303)
    except Exception as e:
        logging.error(f"Error updating preferences: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ===== CHANGE HISTORY ROUTES =====

@app.get("/history", response_class=HTMLResponse)
async def view_history(request: Request, course_id: Optional[str] = None, 
                       start_date: Optional[str] = None, end_date: Optional[str] = None):
    """View change history."""
    try:
        # Convert course_id to int if provided and not empty
        course_id_int = None
        if course_id and course_id.strip():
            try:
                course_id_int = int(course_id)
            except ValueError:
                pass
        
        courses = db_manager.get_courses()
        
        # Get change records with filters
        change_records = db_manager.get_change_records(
            course_id=course_id_int,
            start_date=start_date,
            end_date=end_date
        )
        
        course = None
        if course_id_int:
            course = next((c for c in courses if c['id'] == course_id_int), None)
        
        return templates.TemplateResponse("history.html", {
            "request": request,
            "change_records": change_records,
            "courses": courses,
            "selected_course": course,
            "start_date": start_date or "",
            "end_date": end_date or ""
        })
    except Exception as e:
        logging.error(f"Error viewing history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


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

        return templates.TemplateResponse("change_detail.html", {
            "request": request,
            "change_record": change_record,
            "changes": changes
        })
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error viewing change record course={course_id} change_no={change_no}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== CHECKER ROUTES =====

async def run_checker_task_async():
    """Run checker with lock to prevent parallel execution."""
    # Create a new database manager instance for this task
    task_db_manager = DatabaseManager()
    try:
        # Acquire the lock
        async with check_lock:
            logging.info("Starting manual check task")
            run_checker(task_db_manager)
            logging.info("Manual check task completed")
    except Exception as e:
        logging.error(f"Error in background checker task: {e}", exc_info=True)
        # Make sure status is cleared on error
        try:
            task_db_manager.set_check_status(False)
        except:
            pass
    finally:
        task_db_manager.close()

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
        # Schedule the check task
        background_tasks.add_task(run_checker_task_async)
        logging.info("Manual check triggered via web interface")
        return JSONResponse({
            "status": "success",
            "message": "Check started in background"
        })
    except Exception as e:
        logging.error(f"Error triggering manual check: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ===== API ROUTES (for AJAX calls) =====

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


if __name__ == "__main__":
    uvicorn.run(
        "webapp:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

