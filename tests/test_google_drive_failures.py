import unittest
from unittest.mock import MagicMock, patch

import requests

from app.services.google_drive_downloader import (
    GoogleDriveDownloadError,
    download_file,
)
from app.services.tree_builder import File, Node, build_tree


DRIVE_URL = "https://drive.google.com/open?id=missing-file"


class FakeUploader:
    def is_configured(self):
        return True


class FailingDriveScraper:
    def __init__(self, error):
        self.webdav_uploader = FakeUploader()
        self.error = error

    def get_links(self, url):
        return [DRIVE_URL], [], ["Old exam"], []

    def download_file(self, file_url, destination):
        raise self.error


class GoogleDriveDownloaderTests(unittest.TestCase):
    def test_http_error_is_identified_as_drive_download_failure(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with patch("app.services.google_drive_downloader.requests.get", return_value=response):
            with self.assertRaises(GoogleDriveDownloadError):
                download_file(DRIVE_URL, "/course", FakeUploader())


class GoogleDriveTreeFailureTests(unittest.TestCase):
    def test_unavailable_drive_file_preserves_previous_copy(self):
        old_file = File(
            url=DRIVE_URL,
            name="Old exam",
            md5_hash="abc123",
            etag="old-etag",
            last_updated="2026-01-02T03:04:05+00:00",
            local_path="/course/old-exam.pdf",
            redirect_url=None,
        )
        old_root = Node(
            name="Course",
            url="https://eclass.test/documents",
            local_path="/course",
            files=[old_file],
        )
        scraper = FailingDriveScraper(GoogleDriveDownloadError("404 Not Found"))

        with self.assertLogs(level="WARNING") as logs:
            new_root = build_tree(
                scraper,
                old_root.url,
                old_root.local_path,
                old_root.name,
                252,
                old_root,
            )

        self.assertIn("Skipping unavailable Google Drive file", "\n".join(logs.output))
        self.assertEqual(new_root.files[0], old_file)

    def test_new_unavailable_drive_file_does_not_abort_tree_build(self):
        scraper = FailingDriveScraper(GoogleDriveDownloadError("404 Not Found"))

        new_root = build_tree(
            scraper,
            "https://eclass.test/documents",
            "/course",
            "Course",
            252,
            None,
        )

        self.assertEqual(len(new_root.files), 1)
        self.assertEqual(new_root.files[0].url, DRIVE_URL)
        self.assertIsNone(new_root.files[0].local_path)
        self.assertIsNone(new_root.files[0].md5_hash)

    def test_non_download_failure_remains_course_fatal(self):
        scraper = FailingDriveScraper(RuntimeError("WebDAV upload failed"))

        with self.assertRaisesRegex(RuntimeError, "WebDAV upload failed"):
            build_tree(
                scraper,
                "https://eclass.test/documents",
                "/course",
                "Course",
                252,
                None,
            )


if __name__ == "__main__":
    unittest.main()
