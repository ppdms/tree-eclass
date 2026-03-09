"""
Contains the logic for comparing two course content trees and reporting the differences.
"""
import os
from dataclasses import dataclass
from typing import List, Optional

from app.services.tree_builder import Node, File


@dataclass
class ChangeItem:
    """Represents a single detected change."""
    change_type: str   # "added_file", "deleted_file", "modified_file", "added_directory", "deleted_directory"
    file_path: str     # actual relative path using the real filesystem filename
    display_name: Optional[str] = None  # human-readable display name (if different from filename)
    redirect_url: Optional[str] = None  # set for external links (e.g. SharePoint recordings)

    def __str__(self) -> str:
        prefix_map = {
            "added_file": "Added file",
            "deleted_file": "Deleted file",
            "modified_file": "Modified file",
            "added_directory": "Added directory",
            "deleted_directory": "Deleted directory",
        }
        prefix = prefix_map.get(self.change_type, self.change_type)
        return f"{prefix}: {self.file_path}"

def _make_relative_path(full_path: str, root_path: str) -> str:
    """Remove the root path prefix to create a relative path for display."""
    # Normalize paths
    full_path = full_path.strip('/')
    root_path = root_path.strip('/')
    
    if not root_path:
        return full_path
    
    # Remove root_path prefix if present
    if full_path.startswith(root_path + '/'):
        return full_path[len(root_path) + 1:]
    elif full_path == root_path:
        return ''
    
    return full_path

def _get_file_actual_path(file: File, fallback_dir: str) -> str:
    """Return the file's actual WebDAV path, falling back to dir+name if local_path is unset."""
    if file.local_path:
        return file.local_path
    return os.path.join(fallback_dir, file.name)


def _report_all_added(node: Node, base_path: str, root_path: str) -> List[ChangeItem]:
    """Recursively generates ChangeItems for a newly added directory tree."""
    changes = []
    # The node itself is the directory
    dir_path = os.path.join(base_path, node.name)
    relative_dir_path = _make_relative_path(dir_path, root_path)
    if relative_dir_path:  # Only report if not empty (not the root itself)
        changes.append(ChangeItem("added_directory", relative_dir_path))

    for file in node.files:
        actual_path = _get_file_actual_path(file, dir_path)
        relative_file_path = _make_relative_path(actual_path, root_path)
        changes.append(ChangeItem("added_file", relative_file_path, file.name, file.redirect_url))

    for child in node.children:
        changes.extend(_report_all_added(child, dir_path, root_path))
    
    return changes

def diff_trees(previous: Optional[Node], latest: Node, root_path: Optional[str] = None) -> List[ChangeItem]:
    """Compares two Node trees and returns a list of ChangeItems.
    
    Args:
        previous: The previous tree state (can be None for first check)
        latest: The current tree state
        root_path: The root path to strip from messages (computed from latest.local_path if None)
    """
    changes = []
    
    # Use the root node's local_path as the base to strip from all paths
    if root_path is None:
        root_path = latest.local_path

    # If there was no previous tree, report everything in the latest tree as added.
    if previous is None:
        # The root itself is not reported as "added", just its contents.
        for file in latest.files:
            actual_path = _get_file_actual_path(file, latest.local_path)
            relative_path = _make_relative_path(actual_path, root_path)
            changes.append(ChangeItem("added_file", relative_path, file.name))
        for child in latest.children:
            changes.extend(_report_all_added(child, latest.local_path, root_path))
        return changes

    # --- Directory Diffing ---
    old_dirs = {d.name: d for d in previous.children}
    new_dirs = {d.name: d for d in latest.children}

    # Check for deleted directories
    for dir_name in old_dirs:
        if dir_name not in new_dirs:
            deleted_dir_path = os.path.join(latest.local_path, dir_name)
            relative_path = _make_relative_path(deleted_dir_path, root_path)
            changes.append(ChangeItem("deleted_directory", relative_path))

    # Check for added and modified directories
    for dir_name, new_dir_node in new_dirs.items():
        if dir_name not in old_dirs:
            # This uses the helper to add the entire new subdirectory tree
            changes.extend(_report_all_added(new_dir_node, latest.local_path, root_path))
        else:
            # If the directory exists in both, recurse
            old_dir_node = old_dirs[dir_name]
            changes.extend(diff_trees(old_dir_node, new_dir_node, root_path))

    # --- File Diffing ---
    old_files = {f.url: f for f in previous.files}
    new_files = {f.url: f for f in latest.files}

    # Check for deleted files
    for file_url, old_file in old_files.items():
        if file_url not in new_files:
            actual_path = _get_file_actual_path(old_file, latest.local_path)
            relative_path = _make_relative_path(actual_path, root_path)
            changes.append(ChangeItem("deleted_file", relative_path, old_file.name))

    # Check for added and updated files
    for file_url, new_file in new_files.items():
        actual_path = _get_file_actual_path(new_file, latest.local_path)
        relative_path = _make_relative_path(actual_path, root_path)
        if file_url not in old_files:
            changes.append(ChangeItem("added_file", relative_path, new_file.name, new_file.redirect_url))
        else:
            old_file = old_files[file_url]
            # Check for updates based on MD5 hash
            if old_file.md5_hash != new_file.md5_hash:
                changes.append(ChangeItem("modified_file", relative_path, new_file.name, new_file.redirect_url))
    
    return changes