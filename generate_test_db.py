#!/usr/bin/env python3
"""
Generate a test database with realistic data for testing all facets of the tree-eclass app.
This script will replace eclass.db with a new database containing:
- Credentials
- Multiple courses with hierarchical file structures
- Change records with various types of modifications
- Email configuration
- Application preferences

Uses the actual persistence.py functions to ensure schema compatibility.
"""

import sys
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent))

# Import persistence and data models without triggering package-level imports
import importlib
persistence_spec = importlib.import_module('app.services.persistence')
tree_builder_spec = importlib.import_module('app.services.tree_builder')
DatabaseManager = persistence_spec.DatabaseManager
Node = tree_builder_spec.Node
File = tree_builder_spec.File

# Database path
DB_PATH = Path(__file__).parent / "eclass.db"

# Sample data
COURSES = [
    {"code": "ALGO", "name": "Algorithms", "url": "https://eclass.example.edu/courses/ALGO123"},
    {"code": "DB", "name": "Databases", "url": "https://eclass.example.edu/courses/DB456"},
    {"code": "ML", "name": "Machine Learning", "url": "https://eclass.example.edu/courses/ML789"},
    {"code": "NET", "name": "Computer Networks", "url": "https://eclass.example.edu/courses/NET012"},
]

FILE_TYPES = [
    ("pdf", ["lecture", "slides", "notes", "exercises", "solutions", "exam", "midterm"]),
    ("zip", ["code", "assignment", "project", "lab"]),
    ("txt", ["readme", "instructions", "changelog"]),
    ("docx", ["syllabus", "guidelines", "requirements"]),
    ("py", ["example", "solution", "template"]),
    ("ipynb", ["notebook", "tutorial", "demo"]),
]

FOLDERS = [
    "Lectures",
    "Exercises",
    "Assignments",
    "Exams",
    "Old Exams",
    "Projects",
    "Labs",
    "Tutorials",
    "Resources",
    "Supplementary Material",
]

CHANGE_MESSAGES = [
    "Added new lecture materials for week {week}",
    "Updated exercise solutions",
    "Posted exam information and guidelines",
    "Added practice problems and solutions",
    "Updated course schedule",
    "Posted lab assignment #{num}",
    "Added supplementary reading materials",
    "Updated project requirements",
    "Posted midterm exam results",
    "Added final exam review materials",
]


def insert_credentials(db: DatabaseManager):
    """Insert test credentials."""
    db.save_credentials("test_user", "test_password")
    print("✓ Added credentials")


def insert_courses(db: DatabaseManager):
    """Insert test courses."""
    course_ids = []
    
    for i, course in enumerate(COURSES, start=1):
        download_folder = f"prod/classes/{course['code'].lower()}/2025"
        db.save_course(i, f"{course['name']} ({course['code']})", download_folder)
        course_ids.append(i)
    
    print(f"✓ Added {len(COURSES)} courses")
    return course_ids


def generate_file_tree(db: DatabaseManager, course_id: int, course_code: str):
    """Generate a hierarchical file tree for a course using Node and File dataclasses."""
    
    # Create root node
    root_name = f"{course_code} 2025"
    root_path = f"prod/classes/{course_code.lower()}/2025"
    root_url = f"https://eclass.example.edu/courses/{course_code}"
    root_node = Node(name=root_name, url=root_url, local_path=root_path)
    
    # Track all file paths for change generation
    all_file_paths = []
    
    # Create folder nodes as children
    for folder in random.sample(FOLDERS, k=random.randint(4, 7)):
        folder_path = f"{root_path}/{folder}"
        folder_url = f"{root_url}/{folder}"
        folder_node = Node(name=folder, url=folder_url, local_path=folder_path)
        
        # Create subfolders (sometimes)
        if random.random() > 0.6:
            for week in range(1, random.randint(2, 6)):
                subfolder = f"Week {week}"
                subfolder_path = f"{folder_path}/{subfolder}"
                subfolder_url = f"{folder_url}/{subfolder}"
                subfolder_node = Node(name=subfolder, url=subfolder_url, local_path=subfolder_path)
                
                # Add files to subfolder
                for _ in range(random.randint(2, 5)):
                    ext, prefixes = random.choice(FILE_TYPES)
                    filename = f"{random.choice(prefixes)}_{week}.{ext}"
                    file_url = f"{subfolder_url}/{filename}"
                    file = File(
                        url=file_url,
                        name=filename,
                        md5_hash=f"md5_{random.randint(100000, 999999)}",
                        etag=f"etag_{random.randint(100000, 999999)}"
                    )
                    subfolder_node.files.append(file)
                    all_file_paths.append(f"{folder}/{subfolder}/{filename}")
                
                folder_node.children.append(subfolder_node)
        
        # Add files directly to folder
        for _ in range(random.randint(3, 8)):
            ext, prefixes = random.choice(FILE_TYPES)
            filename = f"{random.choice(prefixes)}_{random.randint(1, 20)}.{ext}"
            file_url = f"{folder_url}/{filename}"
            file = File(
                url=file_url,
                name=filename,
                md5_hash=f"md5_{random.randint(100000, 999999)}",
                etag=f"etag_{random.randint(100000, 999999)}"
            )
            folder_node.files.append(file)
            all_file_paths.append(f"{folder}/{filename}")
        
        root_node.children.append(folder_node)
    
    # Save the tree structure
    db.save_tree(course_id, root_node)
    
    # Count total nodes for reporting
    def count_nodes(node: Node):
        total = 1  # Count this node
        total += len(node.files)
        for child in node.children:
            total += count_nodes(child)
        return total
    
    total_nodes = count_nodes(root_node)
    return total_nodes, all_file_paths


def generate_change_records(db: DatabaseManager, course_id: int, course_code: str, file_paths):
    """Generate change records with various types of changes."""
    
    # Generate 5-15 change records per course
    num_records = random.randint(5, 15)
    
    for i in range(num_records):
        # Generate change items as strings (the format expected by create_change_record)
        num_changes = random.randint(1, 10)
        changes = []
        
        for _ in range(num_changes):
            change_type = random.choice(["added", "modified", "deleted"])
            file_path = random.choice(file_paths)
            
            if change_type == "added":
                changes.append(f"Added file: {file_path}")
            elif change_type == "modified":
                changes.append(f"Modified file: {file_path}")
            elif change_type == "deleted":
                changes.append(f"Deleted file: {file_path}")
        
        # Create change record (it will auto-generate the message)
        db.create_change_record(course_id, changes)
    
    print(f"✓ Added {num_records} change records for {course_code}")


def insert_email_config(db: DatabaseManager):
    """Insert test email configuration."""
    db.save_email_config(
        smtp_server="smtp.gmail.com",
        smtp_port=587,
        smtp_username="test@example.com",
        smtp_password="test_password",
        from_email="noreply@tree-eclass.com",
        to_email="admin@example.com",
        use_tls=True
    )
    print("✓ Added email configuration")


def insert_preferences(db: DatabaseManager):
    """Insert test preferences."""
    db.save_preferences(
        check_interval_minutes=30,
        max_concurrent_downloads=5,
        request_timeout_seconds=45,
        retry_attempts=5,
        notification_enabled=True,
        notification_on_error=True
    )
    print("✓ Added preferences")


def main():
    """Main function to generate test database."""
    print("🌲 Generating test database for tree-eclass...")
    print()
    
    # Remove existing database
    if DB_PATH.exists():
        os.remove(DB_PATH)
        print(f"✓ Removed existing database at {DB_PATH}")
    
    # Create new database using DatabaseManager
    db = DatabaseManager(str(DB_PATH))
    print(f"✓ Created new database at {DB_PATH}")
    print()
    
    try:
        print("✓ Database tables created automatically")
        print()
        
        # Insert data using persistence functions
        insert_credentials(db)
        course_ids = insert_courses(db)
        print()
        
        # Generate file trees and changes for each course
        for course_id, course in zip(course_ids, COURSES):
            print(f"Generating data for {course['name']} ({course['code']})...")
            node_count, file_paths = generate_file_tree(db, course_id, course['code'])
            print(f"  ✓ Generated {node_count} nodes")
            
            generate_change_records(db, course_id, course['code'], file_paths)
        
        print()
        insert_email_config(db)
        insert_preferences(db)
        
        # Get statistics
        courses = db.get_courses()
        change_records = db.get_change_records()
        
        print()
        print("✅ Test database generated successfully!")
        print()
        print(f"Database location: {DB_PATH}")
        print()
        print("Summary:")
        print(f"  • {len(courses)} courses")
        print(f"  • {len(change_records)} change records")
        print()
        print("You can now run the application with: python3 run.py")
        
    except Exception as e:
        print(f"❌ Error generating database: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
