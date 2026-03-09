"""
Handles all network interactions with the e-class website.
"""
import hashlib
import logging
import os
import re
import sys
from typing import Optional
from urllib.parse import urlparse, unquote, urljoin
import requests
from bs4 import BeautifulSoup

# Configuration
ECLASS_BASE_URL = "https://eclass.aueb.gr"
LOGIN_URL = f"{ECLASS_BASE_URL}/?login_page=1"
COURSE_URL_TEMPLATE = f"{ECLASS_BASE_URL}/modules/document/index.php?course=INF{{}}"


# Utility functions
def compute_md5(file_path: str) -> str:
    """Computes the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        raise RuntimeError(f"Failed to compute MD5 hash for file: {file_path}")


def compute_md5_from_bytes(data: bytes) -> str:
    """Computes the MD5 hash from bytes."""
    hash_md5 = hashlib.md5()
    hash_md5.update(data)
    return hash_md5.hexdigest()


def extract_file_name(file_url: str) -> str:
    """Extracts a filename from a URL."""
    path = urlparse(file_url).path
    return unquote(os.path.basename(path))


def extract_file_name_from_response(response) -> Optional[str]:
    """Extract the actual filename from a Content-Disposition response header.
    
    Handles the PHP server quirk where UTF-8 filenames are sent as
    latin-1 encoded bytes (RFC 6266 filename* parameter takes priority).
    Returns None if no usable filename is found.
    """
    cd = response.headers.get('Content-Disposition', '')
    if not cd:
        return None

    # Prefer RFC 5987 encoded filename* (e.g. filename*=UTF-8''foo%20bar.pdf)
    m = re.search(r"filename\*=UTF-8''([^;\s]+)", cd, re.IGNORECASE)
    if m:
        return unquote(m.group(1))

    # Fall back to plain filename="..."
    m = re.search(r'filename="([^"]+)"', cd)
    if not m:
        # Unquoted filename — stop only at ; or end of string.
        # Do NOT use \s here: UTF-8 continuation bytes decoded as latin-1
        # include U+0085/U+0089 etc., which Python's \s treats as whitespace.
        m = re.search(r"filename=([^;]+)", cd)
    if m:
        raw = m.group(1).strip()
        try:
            # PHP often sends UTF-8 bytes with each byte as a latin-1 character
            return raw.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            return raw

    return None


def download_google_drive_file(file_url: str, destination: str, webdav_uploader=None) -> tuple[str, Optional[str], str]:
    """
    Downloads a file from Google Drive and uploads to WebDAV.
    
    Returns:
        Tuple of (webdav_path, md5_hash, file_name)
    """
    from app.services import google_drive_downloader
    try:
        return google_drive_downloader.download_file(file_url, destination, webdav_uploader)
    except Exception as e:
        raise RuntimeError(f"Failed to download Google Drive file: {file_url} - {e}")

class Scraper:
    """Manages a session and scrapes content from the e-class website."""

    def __init__(self, db_manager, webdav_uploader=None):
        self.db_manager = db_manager
        self.session = requests.Session()
        self.webdav_uploader = webdav_uploader
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self._get_cookie()

    def _get_cookie(self):
        """Loads the session cookie from the database or updates it if necessary."""
        cookie_jar = self.db_manager.load_cookie_jar()
        if cookie_jar:
            self.session.cookies = cookie_jar
        else:
            logging.info("No cookie found in database, attempting to log in.")
            self._update_cookie()

    def _update_cookie(self):
        """Updates the session cookie by logging into the e-class website."""
        credentials = self.db_manager.get_credentials()
        if not credentials:
            raise RuntimeError("Credentials not found in the database.")
        
        username, password = credentials

        payload = {
            'uname': username,
            'pass': password,
            'submit': 'Είσοδος'  # "Login" in Greek
        }
        try:
            response = self.session.post(LOGIN_URL, data=payload, allow_redirects=True)
            response.raise_for_status()
            
            # Filter out cookies with empty domain
            from requests.cookies import RequestsCookieJar
            filtered_cookies = RequestsCookieJar()
            for cookie in self.session.cookies:
                if cookie.domain and cookie.domain.strip():
                    filtered_cookies.set_cookie(cookie)
            
            self.session.cookies = filtered_cookies
            
            # Verify login was successful
            phpsessid_found = any(cookie.name == 'PHPSESSID' for cookie in self.session.cookies)
            if not phpsessid_found:
                raise RuntimeError("Login failed: No PHPSESSID cookie received")
            
            if "login_page=1" in response.text and "uname" in response.text:
                raise RuntimeError("Login failed: Invalid credentials or login unsuccessful")
            
            self.db_manager.save_cookie_jar(self.session.cookies)
            logging.info("Successfully updated cookie and saved to database.")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error during login process: {e}")

    def get_links(self, url: str):
        """Parses a URL and returns lists of file and directory links."""
        files = []
        directories = []
        file_names = []
        directory_names = []

        try:
            response = self.session.get(url)
            response.raise_for_status()
            page_text = response.text

            # Check for login page and re-authenticate if needed
            if "login_page=1" in page_text and "uname" in page_text:
                logging.info(f"Page at {url} identified as: Login Page. Re-authenticating.")
                self._update_cookie()
                response = self.session.get(url)
                response.raise_for_status()
                page_text = response.text

                if "login_page=1" in page_text and "uname" in page_text:
                    logging.error(f"Authentication failed after retry for {url}.")
                    raise RuntimeError(f"Authentication failed, still on login page for URL: {url}")

            # Check for registration page
            if "Εγγραφή και είσοδος στο μάθημα" in page_text:
                logging.warning(f"Page at {url} identified as: Course Registration Page.")
                raise RuntimeError(f"User not registered for course at URL: {url}. Please register for the course on e-class.")
            
            soup = BeautifulSoup(page_text, 'html.parser')

            # Check for a valid documents page
            if "Έγγραφα" in page_text:
                logging.info(f"Page at {url} identified as: Documents Page.")
            else:
                logging.warning(f"Page at {url} identified as: Unidentified.")

            # Track which rows have Google Drive downloads to avoid duplicate processing
            google_drive_rows = set()
            
            # First pass: Handle Google Drive links in table rows (they have file names in neighboring cells)
            for row in soup.select("tr"):
                # Look for Google Drive download links in this row
                google_links = row.select('a[href*="drive.google.com"]')
                for google_link in google_links:
                    href = google_link.get('href')
                    
                    # Only accept actual Google Drive file links
                    if "/drive/folders/" in href or "accounts.google.com" in href or "support.google.com" in href:
                        logging.warning(f"Skipping non-file Google link: {href}")
                        continue
                    
                    if "drive.google.com/file/" in href or "drive.google.com/open" in href:
                        # Find the file name in the same row (look for a.fileURL element)
                        file_name_link = row.select_one('a.fileURL')
                        if file_name_link:
                            file_name = file_name_link.get_text(strip=True)
                            files.append(href)
                            file_names.append(file_name)
                            logging.info(f"Found Google Drive file: {file_name} -> {href}")
                            # Mark this row as processed to avoid duplicate entries
                            google_drive_rows.add(id(row))
                        else:
                            # Fallback: use any text we can find
                            files.append(href)
                            file_names.append("Unknown Google Drive File")
                            logging.warning(f"Google Drive link found but no file name available: {href}")
                    else:
                        logging.info(f"Skipping unrecognized Google link: {href}")

            # Second pass: Handle regular links (non-Google Drive)
            for link in soup.select("a[href]"):
                href = link.get('href')
                link_text = link.get_text(strip=True)

                # Skip Google Drive links (already handled above)
                if "google" in href:
                    continue
                
                # Skip fileURL links that are in rows with Google Drive downloads
                # (those were already processed with their Google Drive URLs)
                if 'fileURL' in link.get('class', []):
                    # Check if this link is in a row that has a Google Drive download
                    parent_row = link.find_parent('tr')
                    if parent_row and id(parent_row) in google_drive_rows:
                        continue

                if (
                    ("https://eclass.aueb.gr" + href) == url or
                    "Αποθήκευση" in link_text or  # "Save"
                    "Λήψη" in link_text or  # "Download"
                    "&sort" in href or
                    "modules/document/?course=" in href or
                    "modules/document/" not in href or
                    (len(href) > 9 and href.endswith("openDir=/")) or
                    ("modules/document/index.php?" in href and ("&openDir=/" not in href or "&openDir=%2F" in href))
                ):
                    continue

                if '.' in href[-6:] or '/file.php/' in href or '/modules/document/file.php' in href:
                    # Check if href is already a full URL before prepending base URL
                    if href.startswith('http://') or href.startswith('https://'):
                        files.append(href)
                    else:
                        files.append(ECLASS_BASE_URL + href)
                    file_names.append(link_text)
                elif "&download=/" not in href:
                    # Check if href is already a full URL
                    if href.startswith('http://') or href.startswith('https://'):
                        directories.append(href)
                    else:
                        directories.append(ECLASS_BASE_URL + href)
                    directory_names.append(link_text)

        except requests.exceptions.RequestException as e:
            raise RuntimeError(e)

        return files, directories, file_names, directory_names

    def fetch_etag(self, file_url: str) -> str | None:
        """Fetches the ETag header for a given file URL."""
        try:
            response = self.session.head(file_url)
            response.raise_for_status()
            return response.headers.get('ETag')
        except requests.exceptions.RequestException:
            return None

    def download_file(self, file_url: str, destination: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Downloads a file from a URL and uploads to WebDAV.
        
        Args:
            file_url: The URL of the file to download
            destination: The destination WebDAV path
        
        Returns:
            Tuple of (webdav_path, md5_hash, file_name, redirect_url)
            For Google Drive files, file_name is the actual downloaded name.
            For external redirects (e.g. SharePoint), redirect_url is set and the
            other three values are None (no content is downloaded or uploaded).
        """
        if "google" in file_url:
            webdav_path, md5_hash, file_name = download_google_drive_file(file_url, destination, self.webdav_uploader)
            return webdav_path, md5_hash, file_name, None

        original_netloc = urlparse(file_url).netloc
        try:
            response = self.session.get(file_url, stream=True, timeout=30)

            if response.status_code == 403:
                print(f"Access denied (403) for {file_url}, attempting cookie update and retry.", file=sys.stderr)
                self._update_cookie()
                response = self.session.get(file_url, stream=True, timeout=30)

            response.raise_for_status()

            # Check if we ended up on an external domain (e.g. SharePoint, OneDrive).
            # In that case we do NOT download HTML content — we just record the redirect URL.
            final_netloc = urlparse(response.url).netloc
            if final_netloc and final_netloc != original_netloc:
                # Find the first off-site Location header in the redirect chain.
                redirect_url = response.url  # safe fallback (final URL)
                for r in response.history:
                    loc = r.headers.get('Location', '')
                    if loc:
                        abs_loc = urljoin(r.url, loc)
                        if urlparse(abs_loc).netloc != original_netloc:
                            redirect_url = abs_loc
                            break
                response.close()
                logging.info(f"External redirect detected for {file_url} -> {redirect_url}")
                return None, None, None, redirect_url

            logging.debug(f"download_file headers for {file_url}: { {k: repr(v) for k, v in response.headers.items()} }")

            # Prefer the Content-Disposition filename (includes the real extension)
            # and fall back to extracting from the URL if unavailable.
            header_name = extract_file_name_from_response(response)
            file_name = header_name if header_name else extract_file_name(file_url)

            # WebDAV is required
            if not self.webdav_uploader or not self.webdav_uploader.is_configured():
                raise RuntimeError("WebDAV must be configured to download files")

            # Download to memory first, compute MD5, then upload
            file_data = b''
            for chunk in response.iter_content(chunk_size=8192):
                file_data += chunk

            md5_hash = compute_md5_from_bytes(file_data)
            webdav_path = self.webdav_uploader.upload_file(file_data, destination, file_name)

            return webdav_path, md5_hash, file_name, None

        except requests.exceptions.ConnectTimeout as e:
            # Timed out while following a redirect to an external host.
            # Treat the unreachable URL as an external redirect link rather than a hard failure.
            if e.request and urlparse(e.request.url).netloc != original_netloc:
                redirect_url = e.request.url
                logging.warning(f"Connection timeout for external redirect {file_url} -> {redirect_url}")
                return None, None, None, redirect_url
            raise RuntimeError(f"Failed to download file: {file_url} - {e}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to download file: {file_url} - {e}")

