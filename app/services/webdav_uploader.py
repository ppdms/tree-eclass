"""
Handles uploading files to WebDAV destinations.
"""
import logging
import os
import tempfile
import urllib3
from typing import Optional
from webdav3.client import Client
from webdav3.exceptions import WebDavException


class WebDAVUploader:
    """Manages WebDAV uploads."""

    def __init__(self, webdav_config: Optional[dict] = None):
        """Initialize WebDAV client with configuration."""
        self.client = None
        self.base_path = ""
        
        if webdav_config:
            self.configure(webdav_config)

    def configure(self, webdav_config: dict):
        """Configure the WebDAV client with provided settings."""
        try:
            # Ensure hostname has trailing slash
            hostname = webdav_config.get('hostname', '').rstrip('/')
            if not hostname.endswith('/'):
                hostname += '/'
            
            options = {
                'webdav_hostname': hostname,
                'webdav_login': webdav_config.get('username'),
                'webdav_password': webdav_config.get('password'),
            }
            
            if webdav_config.get('timeout'):
                options['webdav_timeout'] = webdav_config.get('timeout')
            
            # Optional: configure chunk size for downloads (default is 65536)
            # This could be useful for large files
            if webdav_config.get('chunk_size'):
                options['chunk_size'] = webdav_config.get('chunk_size')
            
            self.client = Client(options)
            
            # SSL verification must be set on the client object, not in options
            if webdav_config.get('disable_check'):
                self.client.verify = False  # Disable SSL certificate verification
                # Suppress SSL warnings when verification is disabled
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                logging.warning("SSL verification is disabled for WebDAV")
            
            self.base_path = webdav_config.get('base_path', '').strip('/')
            
            logging.info(f"WebDAV client configured for {hostname}")
            logging.debug(f"WebDAV base_path: '{self.base_path}'")
        except Exception as e:
            logging.error(f"Failed to configure WebDAV client: {e}", exc_info=True)
            raise

    def is_configured(self) -> bool:
        """Check if WebDAV client is configured."""
        return self.client is not None

    def _resolve_remote_path(self, remote_path: str) -> str:
        """Resolve remote path based on prefix rules.
        
        Rules:
        - Paths starting with '/' are absolute (relative to WebDAV root)
        - Paths starting with '~' use base_path from settings (~ replaced with base_path)
        - Other paths are treated as absolute (same as starting with '/')
        
        Args:
            remote_path: The input path with optional ~ or / prefix
            
        Returns:
            Normalized absolute path for WebDAV
        """
        remote_path = remote_path.strip()
        
        if remote_path.startswith('~/'):
            # Replace ~ with base_path
            relative_part = remote_path[2:]  # Remove '~/'
            if self.base_path:
                parts = [self.base_path, relative_part]
                full_path = '/'.join(p.strip('/') for p in parts if p)
            else:
                full_path = relative_part.strip('/')
        elif remote_path.startswith('~'):
            # Just ~ means base_path
            if remote_path == '~' and self.base_path:
                full_path = self.base_path.strip('/')
            else:
                # Treat as literal path (edge case)
                full_path = remote_path.strip('/')
        else:
            # Absolute path or relative path (treat as absolute)
            full_path = remote_path.strip('/')
        
        # Ensure path starts with /
        return '/' + full_path if full_path else '/'

    def upload_file(self, file_data: bytes, remote_path: str, file_name: str) -> str:
        """
        Upload file data to WebDAV destination.
        
        Args:
            file_data: The file content as bytes
            remote_path: Path with optional ~ (for base_path) or / (absolute) prefix
            file_name: The name of the file
            
        Returns:
            The full remote path where the file was uploaded
        """
        if not self.is_configured():
            raise RuntimeError("WebDAV client is not configured")
        
        try:
            # Resolve remote path based on prefix rules
            full_remote_dir = self._resolve_remote_path(remote_path)
            full_remote_path = f"{full_remote_dir}/{file_name}".replace('//', '/')
            
            # Ensure directory exists
            self._ensure_directory(full_remote_dir)
            
            # Write file data to temporary file and upload
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(file_data)
                tmp_file_path = tmp_file.name
            
            try:
                self.client.upload_sync(remote_path=full_remote_path, local_path=tmp_file_path)
                logging.info(f"Uploaded file to WebDAV: {full_remote_path}")
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
            
            return full_remote_path
            
        except WebDavException as e:
            logging.error(f"WebDAV error uploading file: {e}", exc_info=True)
            raise RuntimeError(f"WebDAV error uploading file: {e}")
        except Exception as e:
            logging.error(f"Failed to upload file to WebDAV: {e}", exc_info=True)
            raise RuntimeError(f"Failed to upload file to WebDAV: {e}")

    def upload_stream(self, file_stream, remote_path: str, file_name: str) -> str:
        """
        Upload file from a stream to WebDAV destination.
        
        Args:
            file_stream: File-like object or iterator of chunks
            remote_path: Path with optional ~ (for base_path) or / (absolute) prefix
            file_name: The name of the file
            
        Returns:
            The full remote path where the file was uploaded
        """
        if not self.is_configured():
            raise RuntimeError("WebDAV client is not configured")
        
        try:
            # Resolve remote path based on prefix rules
            full_remote_dir = self._resolve_remote_path(remote_path)
            full_remote_path = f"{full_remote_dir}/{file_name}".replace('//', '/')
            
            # Ensure directory exists
            self._ensure_directory(full_remote_dir)
            
            # Write stream to temporary file and upload
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                if hasattr(file_stream, 'read'):
                    # File-like object
                    while True:
                        chunk = file_stream.read(8192)
                        if not chunk:
                            break
                        tmp_file.write(chunk)
                else:
                    # Iterator of chunks
                    for chunk in file_stream:
                        tmp_file.write(chunk)
                
                tmp_file_path = tmp_file.name
            
            try:
                self.client.upload_sync(remote_path=full_remote_path, local_path=tmp_file_path)
                logging.info(f"Uploaded file to WebDAV: {full_remote_path}")
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
            
            return full_remote_path
            
        except WebDavException as e:
            logging.error(f"WebDAV error uploading stream: {e}", exc_info=True)
            raise RuntimeError(f"WebDAV error uploading stream: {e}")
        except Exception as e:
            logging.error(f"Failed to upload stream to WebDAV: {e}", exc_info=True)
            raise RuntimeError(f"Failed to upload stream to WebDAV: {e}")

    def _ensure_directory(self, remote_dir: str):
        """Ensure that a directory exists on the WebDAV server."""
        if not remote_dir or remote_dir == '/':
            return
        
        try:
            # Check if directory exists
            if not self.client.check(remote_dir):
                # Create parent directories recursively
                parent_dir = os.path.dirname(remote_dir.rstrip('/'))
                if parent_dir and parent_dir != '/':
                    self._ensure_directory(parent_dir)
                
                # Create the directory
                self.client.mkdir(remote_dir)
                logging.debug(f"Created WebDAV directory: {remote_dir}")
        except WebDavException as e:
            logging.warning(f"WebDAV error ensuring directory {remote_dir}: {e}")
        except Exception as e:
            logging.warning(f"Error ensuring directory {remote_dir}: {e}")

    def test_connection(self) -> bool:
        """Test the WebDAV connection."""
        if not self.is_configured():
            return False
        
        try:
            # Try to check if root exists (simplest test)
            result = self.client.check('/')
            logging.info(f"WebDAV connection test successful (root check: {result})")
            
            # If base_path is set, ensure it exists
            if self.base_path:
                try:
                    if not self.client.check(self.base_path):
                        logging.info(f"Creating WebDAV base path: {self.base_path}")
                        self.client.mkdir(self.base_path)
                    else:
                        logging.info(f"WebDAV base path exists: {self.base_path}")
                except WebDavException as e:
                    logging.warning(f"Could not verify/create base path {self.base_path}: {e}")
            
            return True
        except WebDavException as e:
            logging.error(f"WebDAV connection test failed: {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"WebDAV connection test failed with unexpected error: {e}", exc_info=True)
            return False
