"""
Builds the course content tree by scraping the e-class website.
"""
import logging
import os
import subprocess
import tempfile
from typing import Optional
from dataclasses import dataclass, field
from typing import List
from datetime import datetime, timezone

from app.services.scraper import Scraper, compute_md5, compute_md5_from_bytes


# Data models
@dataclass
class File:
    """Represents a file in the course content tree."""
    url: str
    name: str
    md5_hash: Optional[str] = None
    etag: Optional[str] = None
    last_updated: Optional[str] = None  # ISO 8601 timestamp when file was last added/modified
    local_path: Optional[str] = None  # WebDAV path where the file is stored
    redirect_url: Optional[str] = None  # Set when the file is an external link (e.g. SharePoint)
    diff_webdav_path: Optional[str] = None  # Transient: WebDAV path of the diff PDF for this update cycle


@dataclass
class Node:
    """Represents a directory (a node) in the course content tree."""
    name: str
    url: str
    local_path: str  # Note: This is actually the WebDAV path, kept as local_path for DB compatibility
    children: List['Node'] = field(default_factory=list)
    files: List[File] = field(default_factory=list)

def _compute_version_path(file_local_path: str, root_webdav: str) -> str:
    """Compute the .versions archive path for a modified file.

    E.g. /Algorithms/Lecture Notes/lecture1.pdf
      → /Algorithms/.versions/Lecture Notes/lecture1.pdf/2026-03-09T143022.pdf
    """
    root = root_webdav.strip('/')
    full = file_local_path.strip('/')
    relative = full[len(root) + 1:] if full.startswith(root + '/') else full
    filename = os.path.basename(relative)
    _, ext = os.path.splitext(filename)
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')
    return f"/{root}/.versions/{relative}/{ts}{ext}"


def _archive_modified_version(scraper: 'Scraper', old_file: 'File', webdav_path: str,
                               root_webdav: str) -> Optional[str]:
    """Copy old file version to .versions/ before it gets overwritten.

    Returns the WebDAV path of the archived copy, or None if no copy was made
    (e.g. redirect-only files or copy failure).  The caller is responsible for
    calling save_file_version with the returned path.
    """
    if old_file.local_path:
        version_path = _compute_version_path(old_file.local_path, root_webdav)
        success = scraper.webdav_uploader.copy_file(old_file.local_path, version_path)
        return version_path if success else None
    return None


def _generate_pdf_diff(webdav_uploader, archived_version_path: str, new_webdav_path: str) -> Optional[str]:
    """Run diff-pdf on the archived old version vs the new version and upload the result.

    Returns the WebDAV path of the diff PDF, or None if generation failed or is
    not applicable (e.g. non-PDF files).
    """
    if not archived_version_path or not new_webdav_path:
        return None
    if not (archived_version_path.lower().endswith('.pdf') and new_webdav_path.lower().endswith('.pdf')):
        return None

    try:
        old_data = webdav_uploader.download_file(archived_version_path)
        new_data = webdav_uploader.download_file(new_webdav_path)
        if not old_data or not new_data:
            logging.warning(f"Could not download PDFs for diff: {archived_version_path}, {new_webdav_path}")
            return None

        with tempfile.TemporaryDirectory() as tmpdir:
            old_tmp = os.path.join(tmpdir, 'old.pdf')
            new_tmp = os.path.join(tmpdir, 'new.pdf')
            diff_tmp = os.path.join(tmpdir, 'diff.pdf')

            with open(old_tmp, 'wb') as f:
                f.write(old_data)
            with open(new_tmp, 'wb') as f:
                f.write(new_data)

            result = subprocess.run(
                ['diff-pdf', f'--output-diff={diff_tmp}', '--skip-identical', '--mark-differences',
                 old_tmp, new_tmp],
                capture_output=True, timeout=120,
            )

            # diff-pdf exits 0 (identical) or 1 (differences found) — both are success
            if result.returncode not in (0, 1):
                logging.warning(
                    f"diff-pdf exited {result.returncode} for {archived_version_path}: "
                    f"{result.stderr.decode(errors='replace')}"
                )
                return None

            if not os.path.exists(diff_tmp):
                return None

            with open(diff_tmp, 'rb') as f:
                diff_data = f.read()

        # Store the diff alongside the archived version
        old_base = os.path.splitext(os.path.basename(archived_version_path))[0]
        diff_dir = os.path.dirname(archived_version_path)
        diff_name = f"{old_base}_diff.pdf"
        diff_webdav_path = webdav_uploader.upload_file(diff_data, diff_dir, diff_name)
        logging.info(f"Generated PDF diff: {diff_webdav_path}")
        return diff_webdav_path

    except Exception as e:
        logging.warning(f"Failed to generate PDF diff for {archived_version_path}: {e}")
        return None


def build_tree(scraper: Scraper, url: str, webdav_path: str, name: str,
               course_id: int, old_root: Optional[Node],
               root_webdav: Optional[str] = None) -> Node:
    """Recursively builds the Node tree for a given course URL.
    
    Args:
        scraper: The scraper instance with WebDAV configured
        url: The course URL to scrape
        webdav_path: The WebDAV destination path for this node
        name: The name of this node
        course_id: Database course ID (used for version archiving)
        old_root: Previous tree state for comparison
        root_webdav: Root WebDAV folder for the course (computed on first call)
        
    Returns:
        Root Node of the built tree
    """
    logging.info(f"Building tree for URL: {url}")

    # Capture root WebDAV path on the first (non-recursive) call
    if root_webdav is None:
        root_webdav = webdav_path

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
        local_path = None
        actual_file_name = None
        redirect_url = None
        file_diff_webdav_path = None

        if "google" in file_url:
            # Google Drive files: always download and compute hash
            local_path, file_hash, actual_file_name, redirect_url = scraper.download_file(file_url, webdav_path)
            last_updated = datetime.now(timezone.utc).isoformat()
            # Use the actual downloaded file name if the scraped one is empty
            if not file_name or not file_name.strip():
                file_name = actual_file_name
        else:
            etag = scraper.fetch_etag(file_url)
            # Download if new, or etag has changed
            if not old_file or not etag or old_file.etag != etag:
                # Archive old version before it gets overwritten (only when file existed and etag changed)
                archived_version_path = None
                if old_file and etag and old_file.etag != etag:
                    archived_version_path = _archive_modified_version(scraper, old_file, webdav_path, root_webdav)
                local_path, file_hash, actual_file_name, redirect_url = scraper.download_file(file_url, webdav_path)
                if redirect_url:
                    # External link (e.g. SharePoint/Stream): use a stable hash derived from
                    # the redirect URL so change detection is based on the link, not HTML content.
                    file_hash = compute_md5_from_bytes(redirect_url.encode())
                    local_path = None
                last_updated = datetime.now(timezone.utc).isoformat()
                # Record old version in DB and (for PDFs) generate a visual diff
                if old_file and etag and old_file.etag != etag:
                    root = root_webdav.strip('/')
                    if old_file.local_path:
                        full = old_file.local_path.strip('/')
                        rel_path = full[len(root) + 1:] if full.startswith(root + '/') else full
                        diff_webdav_path = None
                        if archived_version_path and local_path:
                            diff_webdav_path = _generate_pdf_diff(
                                scraper.webdav_uploader, archived_version_path, local_path
                            )
                        try:
                            scraper.db_manager.save_file_version(
                                course_id=course_id,
                                file_path=rel_path,
                                version_webdav_path=archived_version_path,
                                change_type='modified',
                                display_name=old_file.name,
                                redirect_url=None,
                                diff_webdav_path=diff_webdav_path,
                            )
                            file_diff_webdav_path = diff_webdav_path
                        except Exception as e:
                            logging.warning(f"Failed to save file version record for {rel_path}: {e}")
                    elif old_file.redirect_url:
                        file_logical = os.path.join(webdav_path.strip('/'), old_file.name)
                        rel_path = file_logical[len(root) + 1:] if file_logical.startswith(root + '/') else file_logical
                        try:
                            scraper.db_manager.save_file_version(
                                course_id=course_id,
                                file_path=rel_path,
                                version_webdav_path=None,
                                change_type='modified',
                                display_name=old_file.name,
                                redirect_url=old_file.redirect_url,
                            )
                        except Exception as e:
                            logging.warning(f"Failed to save redirect version record for {rel_path}: {e}")
            else:
                # Keep old hash, etag, timestamp and local_path if file is not re-downloaded
                file_hash = old_file.md5_hash
                etag = old_file.etag
                last_updated = old_file.last_updated
                local_path = old_file.local_path
                redirect_url = old_file.redirect_url
        
        current_node.files.append(File(url=file_url, name=file_name, md5_hash=file_hash, etag=etag, last_updated=last_updated, local_path=local_path, redirect_url=redirect_url, diff_webdav_path=file_diff_webdav_path))

    # Create a map of old child directories for efficient lookup
    old_children_map = {c.name: c for c in old_root.children} if old_root else {}

    # Process directories recursively
    for i, dir_url in enumerate(dir_urls):
        dir_name = dir_names[i]
        child_local_path = os.path.join(webdav_path, dir_name)
        old_child_node = old_children_map.get(dir_name)
        
        child_node = build_tree(scraper, dir_url, child_local_path, dir_name, course_id, old_child_node, root_webdav=root_webdav)
        current_node.children.append(child_node)

    return current_node