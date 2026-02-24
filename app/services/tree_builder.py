"""
Builds the course content tree by scraping the e-class website.
"""
import logging
import os
from typing import Optional
from dataclasses import dataclass, field
from typing import List
from datetime import datetime, timezone

from app.services.scraper import Scraper, compute_md5


# Data models
@dataclass
class File:
    """Represents a file in the course content tree."""
    url: str
    name: str
    md5_hash: Optional[str] = None
    etag: Optional[str] = None
    last_updated: Optional[str] = None  # ISO 8601 timestamp when file was last added/modified


@dataclass
class Node:
    """Represents a directory (a node) in the course content tree."""
    name: str
    url: str
    local_path: str  # Note: This is actually the WebDAV path, kept as local_path for DB compatibility
    children: List['Node'] = field(default_factory=list)
    files: List[File] = field(default_factory=list)

def build_tree(scraper: Scraper, url: str, webdav_path: str, name: str, old_root: Optional[Node]) -> Node:
    """Recursively builds the Node tree for a given course URL.
    
    Args:
        scraper: The scraper instance with WebDAV configured
        url: The course URL to scrape
        webdav_path: The WebDAV destination path for this node
        name: The name of this node
        old_root: Previous tree state for comparison
        
    Returns:
        Root Node of the built tree
    """
    logging.info(f"Building tree for URL: {url}")
    
    # WebDAV is required
    if not scraper.webdav_uploader or not scraper.webdav_uploader.is_configured():
        raise RuntimeError("WebDAV must be configured to build tree")

    # Create the node for the current directory
    current_node = Node(name=name, url=url, local_path=webdav_path)

    # Get links from the scraper
    file_urls, dir_urls, file_names, dir_names = scraper.get_links(url)

    # Create a map of old files for efficient lookup
    old_files_map = {f.url: f for f in old_root.files} if old_root else {}

    # Process files
    for i, file_url in enumerate(file_urls):
        file_name = file_names[i]
        old_file = old_files_map.get(file_url)
        
        file_hash = None
        etag = None
        last_updated = None

        if "google" in file_url:
            # Google Drive files: always download and compute hash
            downloaded_path, file_hash = scraper.download_file(file_url, webdav_path)
            last_updated = datetime.now(timezone.utc).isoformat()
        else:
            etag = scraper.fetch_etag(file_url)
            # Download if new, or etag has changed
            if not old_file or not etag or old_file.etag != etag:
                downloaded_path, file_hash = scraper.download_file(file_url, webdav_path)
                last_updated = datetime.now(timezone.utc).isoformat()
            else:
                # Keep old hash, etag, and timestamp if file is not re-downloaded
                file_hash = old_file.md5_hash
                etag = old_file.etag
                last_updated = old_file.last_updated
        
        current_node.files.append(File(url=file_url, name=file_name, md5_hash=file_hash, etag=etag, last_updated=last_updated))

    # Create a map of old child directories for efficient lookup
    old_children_map = {c.name: c for c in old_root.children} if old_root else {}

    # Process directories recursively
    for i, dir_url in enumerate(dir_urls):
        dir_name = dir_names[i]
        child_local_path = os.path.join(webdav_path, dir_name)
        old_child_node = old_children_map.get(dir_name)
        
        child_node = build_tree(scraper, dir_url, child_local_path, dir_name, old_child_node)
        current_node.children.append(child_node)

    return current_node