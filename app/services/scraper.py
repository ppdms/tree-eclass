"""
Handles all network interactions with the e-class website.
"""
import hashlib
import logging
import os
import sys
from urllib.parse import urlparse, unquote
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


def extract_file_name(file_url: str) -> str:
    """Extracts a filename from a URL."""
    path = urlparse(file_url).path
    return unquote(os.path.basename(path))


def download_google_drive_file(file_url: str, destination: str) -> str:
    """Downloads a file from Google Drive."""
    from app.services import google_drive_downloader
    try:
        return google_drive_downloader.download_file(file_url, destination)
    except Exception as e:
        raise RuntimeError(f"Failed to download Google Drive file: {file_url} - {e}")

class Scraper:
    """Manages a session and scrapes content from the e-class website."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.session = requests.Session()
        self._get_cookie()

    def _get_cookie(self):
        """Loads the session cookie from the database or updates it if necessary."""
        cookie_jar = self.db_manager.load_cookie_jar()
        if cookie_jar:
            self.session.cookies.update(cookie_jar)
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
            response = self.session.post(LOGIN_URL, data=payload)
            response.raise_for_status()
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

            # 1. Check for login page
            if "Σύνδεση" in page_text:
                logging.info(f"Page at {url} identified as: Login Page. Re-authenticating.")
                self._update_cookie()
                response = self.session.get(url) # Retry once
                response.raise_for_status()
                page_text = response.text

                if "Σύνδεση" in page_text:
                    logging.error(f"Authentication failed after retry for {url}.")
                    raise RuntimeError(f"Authentication failed, still on login page for URL: {url}")

            # 2. Check for registration page
            if "Εγγραφή και είσοδος στο μάθημα" in page_text:
                logging.warning(f"Page at {url} identified as: Course Registration Page.")
                raise RuntimeError(f"User not registered for course at URL: {url}. Please register for the course on e-class.")
            
            soup = BeautifulSoup(page_text, 'html.parser')

            # 3. Check for a valid documents page
            # We assume it's a documents page if it contains the text "Έγγραφα" (Documents) 
            # and does not contain the registration text.
            if "Έγγραφα" in page_text:
                logging.info(f"Page at {url} identified as: Documents Page.")
            else:
                logging.warning(f"Page at {url} identified as: Unidentified.")


            for link in soup.select("a[href]"):
                href = link.get('href')
                link_text = link.get_text(strip=True)

                if (
                    ("https://eclass.aueb.gr" + href) == url or
                    "Αποθήκευση" in link_text or  # "Save"
                    "Λήψη" in link_text or  # "Download"
                    "&sort" in href or
                    "modules/document/?course=" in href or
                    ("google" not in href and "modules/document/" not in href) or
                    (len(href) > 9 and href.endswith("openDir=/")) or
                    ("modules/document/index.php?" in href and ("&openDir=/" not in href or "&openDir=%2F" in href))
                ):
                    continue

                if "google" in href:
                    # Only accept actual Google Drive file links, skip folders and other Google URLs
                    if "/drive/folders/" in href or "accounts.google.com" in href or "support.google.com" in href:
                        logging.warning(f"Skipping non-file Google link: {href}")
                    elif "drive.google.com/file/" in href or "drive.google.com/open" in href:
                        # Valid Google Drive file link
                        files.append(href)
                        file_names.append(link_text)
                    else:
                        # Log other Google links for debugging
                        logging.info(f"Skipping unrecognized Google link: {href}")
                elif '.' in href[-6:]:
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

    def download_file(self, file_url: str, destination: str) -> str:
        """Downloads a file from a URL to a destination path."""
        if "google" in file_url:
            return download_google_drive_file(file_url, destination)

        try:
            response = self.session.get(file_url, stream=True)
            response.raise_for_status()

            if response.status_code == 403:
                print(f"Access denied (403) for {file_url}, attempting cookie update and retry.", file=sys.stderr)
                self._update_cookie()
                response = self.session.get(file_url, stream=True)
                response.raise_for_status()

            file_name = extract_file_name(file_url)
            destination_file = os.path.join(destination, file_name)
            os.makedirs(destination, exist_ok=True)

            with open(destination_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return destination_file

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to download file: {file_url} - {e}")

