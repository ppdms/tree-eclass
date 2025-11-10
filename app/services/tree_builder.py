"""
Builds the course content tree by scraping the e-class website.
"""
import logging
import os
from typing import Optional
from dataclasses import dataclass, field
from typing import List

from app.services.scraper import Scraper, compute_md5


# Data models
@dataclass
class File:
    """Represents a file in the course content tree."""
    url: str
    name: str
    md5_hash: Optional[str] = None
    etag: Optional[str] = None


@dataclass
class Node:
    """Represents a directory (a node) in the course content tree."""
    name: str
    url: str
    local_path: str
    children: List['Node'] = field(default_factory=list)
    files: List[File] = field(default_factory=list)

def build_tree(scraper: Scraper, url: str, local_path: str, name: str, old_root: Optional[Node]) -> Node:
    """Recursively builds the Node tree for a given course URL using the new data model."""
    logging.info(f"Building tree for URL: {url}")
    os.makedirs(local_path, exist_ok=True)

    # Create the node for the current directory
    current_node = Node(name=name, url=url, local_path=local_path)

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

        if "google" in file_url:
            downloaded_path = scraper.download_file(file_url, local_path)
            if downloaded_path:
                file_hash = compute_md5(downloaded_path)
        else:
            etag = scraper.fetch_etag(file_url)
            # Download if new, or etag has changed
            if not old_file or not etag or old_file.etag != etag:
                downloaded_path = scraper.download_file(file_url, local_path)
                if downloaded_path:
                    file_hash = compute_md5(downloaded_path)
            else:
                # Keep old hash and etag if file is not re-downloaded
                file_hash = old_file.md5_hash
                etag = old_file.etag
        
        current_node.files.append(File(url=file_url, name=file_name, md5_hash=file_hash, etag=etag))

    # Create a map of old child directories for efficient lookup
    old_children_map = {c.name: c for c in old_root.children} if old_root else {}

    # Process directories recursively
    for i, dir_url in enumerate(dir_urls):
        dir_name = dir_names[i]
        child_local_path = os.path.join(local_path, dir_name)
        old_child_node = old_children_map.get(dir_name)
        
        child_node = build_tree(scraper, dir_url, child_local_path, dir_name, old_child_node)
        current_node.children.append(child_node)

    return current_node