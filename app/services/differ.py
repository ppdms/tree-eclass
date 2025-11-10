"""
Contains the logic for comparing two course content trees and reporting the differences.
"""
import os
from typing import List, Optional

from app.services.tree_builder import Node, File

def _report_all_added(node: Node, base_path: str) -> List[str]:
    """Recursively generates change messages for a newly added directory tree."""
    changes = []
    # The node itself is the directory
    dir_path = os.path.join(base_path, node.name)
    changes.append(f"Added directory: {dir_path}")

    for file in node.files:
        file_path = os.path.join(dir_path, file.name)
        changes.append(f"Added file: {file_path}")

    for child in node.children:
        changes.extend(_report_all_added(child, dir_path))
    
    return changes

def diff_trees(previous: Optional[Node], latest: Node) -> List[str]:
    """Compares two Node trees and returns a list of change messages."""
    changes = []

    # If there was no previous tree, report everything in the latest tree as added.
    if previous is None:
        # The root itself is not reported as "added", just its contents.
        for file in latest.files:
            changes.append(f"Added file: {os.path.join(latest.name, file.name)}")
        for child in latest.children:
            changes.extend(_report_all_added(child, latest.name))
        return changes

    # --- Directory Diffing ---
    old_dirs = {d.name: d for d in previous.children}
    new_dirs = {d.name: d for d in latest.children}

    # Check for deleted directories
    for dir_name in old_dirs:
        if dir_name not in new_dirs:
            deleted_dir_path = os.path.join(latest.local_path, dir_name)
            changes.append(f"Deleted directory: {deleted_dir_path}")

    # Check for added and modified directories
    for dir_name, new_dir_node in new_dirs.items():
        if dir_name not in old_dirs:
            # This uses the helper to add the entire new subdirectory tree
            changes.extend(_report_all_added(new_dir_node, latest.local_path))
        else:
            # If the directory exists in both, recurse
            old_dir_node = old_dirs[dir_name]
            changes.extend(diff_trees(old_dir_node, new_dir_node))

    # --- File Diffing ---
    old_files = {f.url: f for f in previous.files}
    new_files = {f.url: f for f in latest.files}

    # Check for deleted files
    for file_url, old_file in old_files.items():
        if file_url not in new_files:
            deleted_file_path = os.path.join(latest.local_path, old_file.name)
            changes.append(f"Deleted file: {deleted_file_path}")

    # Check for added and updated files
    for file_url, new_file in new_files.items():
        full_file_path = os.path.join(latest.local_path, new_file.name)
        if file_url not in old_files:
            changes.append(f"Added file: {full_file_path}")
        else:
            old_file = old_files[file_url]
            # Check for updates based on MD5 hash
            if old_file.md5_hash != new_file.md5_hash:
                changes.append(f"Modified file: {full_file_path}")
    
    return changes