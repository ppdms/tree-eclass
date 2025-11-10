
import requests
import re
import os
import sys
from urllib.parse import unquote
from bs4 import BeautifulSoup

def download_file(file_url, destination_path):
    """
    Downloads a file from a Google Drive URL.
    """
    file_id = extract_file_id(file_url)
    resource_key = extract_resource_key(file_url)
    auth_user = extract_auth_user(file_url)

    download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download"
    if resource_key:
        download_url += f"&resourcekey={resource_key}"
    if auth_user:
        download_url += f"&authuser={auth_user}"

    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        
        file_name = get_file_name(r, file_url)
        destination_file_path = os.path.join(destination_path, file_name)

        os.makedirs(destination_path, exist_ok=True)

        with open(destination_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    return destination_file_path

def extract_file_id(url):
    """
    Extracts the file ID from a Google Drive URL.
    """
    # Try to match the file/d/ pattern
    match = re.search(r"https://drive.google.com/file/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    
    # Try to match the open?id= pattern
    match = re.search(r"id=([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
        
    return url

def extract_resource_key(url):
    """
    Extracts the resource key from a Google Drive URL.
    """
    match = re.search(r"resourcekey=([^&]+)", url)
    if match:
        return match.group(1)
    return None

def extract_auth_user(url):
    """
    Extracts the authuser from a Google Drive URL.
    """
    match = re.search(r"authuser=([^&]+)", url)
    if match:
        return unquote(match.group(1))
    return None

def get_file_name(response, file_url):
    """
    Gets the file name from the Content-Disposition header or by parsing the HTML title.
    """
    content_disposition = response.headers.get('content-disposition')
    if content_disposition and 'filename=' in content_disposition:
        filename = content_disposition.split('filename=')[1].strip().strip('"')
        return filename
    else:
        try:
            page_content = requests.get(file_url).text
            soup = BeautifulSoup(page_content, 'html.parser')
            title = soup.title.string
            if "- Google Drive" in title:
                title = title.replace("- Google Drive", "").strip()
            return title
        except Exception as e:
            print(f"Error fetching page to get file name: {e}", file=sys.stderr)
            return "downloaded_file"

def main():
    if len(sys.argv) != 3:
        print("Usage: python google_drive_downloader.py <fileUrl> <destinationPath>")
        return

    file_url = sys.argv[1]
    destination_path = sys.argv[2]

    try:
        saved_path = download_file(file_url, destination_path)
        print(f"File downloaded successfully to: {saved_path}")
    except Exception as e:
        print(f"Error downloading file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
