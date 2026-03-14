"""
Handles scraping and parsing of course announcements from e-class RSS feeds.
"""
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


ECLASS_BASE_URL = "https://eclass.aueb.gr"
ANNOUNCEMENTS_URL_TEMPLATE = f"{ECLASS_BASE_URL}/modules/announcements/index.php?course=INF{{}}"

# Special feeds that are not tied to a monitored course.
# 'eclass_course_id' feeds are fetched via the authenticated e-class session;
# feeds without it are fetched as plain public URLs.
GLOBAL_FEEDS = {
    'dept': {
        'key': 'dept',
        'name': 'Ανακοινώσεις Τμήματος Πληροφορικής ΟΠΑ',
        'eclass_course_id': None,
        'page_url': 'https://eclass.aueb.gr/modules/announcements/index.php?course=INF417',
        'url': None,
    },
    'undergrad': {
        'key': 'undergrad',
        'name': 'Ανακοινώσεις Προπτυχιακών Τμημάτων',
        'eclass_course_id': None,
        'page_url': None,
        'url': 'https://aueb.gr/el/taxonomy/term/701/feed',
    },
    'rector': {
        'key': 'rector',
        'name': 'Ανακοινώσεις Πρυτανείας',
        'eclass_course_id': None,
        'page_url': None,
        'url': 'https://aueb.gr/el/taxonomy/term/700/feed',
    },
}


class AnnouncementsScraper:
    """Handles fetching and parsing course announcements."""
    
    def __init__(self, session: requests.Session):
        """
        Initialize the announcements scraper.
        
        Args:
            session: Authenticated requests session from the main Scraper
        """
        self.session = session
    
    def get_rss_url(self, course_id: int) -> Optional[str]:
        """
        Extract the RSS feed URL from a course's announcements page.
        
        Args:
            course_id: The course ID (e.g., 161 for INF161)
        
        Returns:
            The full RSS feed URL with token, or None if not found
        """
        announcements_url = ANNOUNCEMENTS_URL_TEMPLATE.format(course_id)
        
        try:
            response = self.session.get(announcements_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the RSS link
            # <a class="btn btn-default text-decoration-none tiny-icon-rss" href="/modules/announcements/rss.php?c=INF161&uid=...&token=...">
            rss_link = soup.find('a', class_='tiny-icon-rss')
            
            if rss_link and rss_link.get('href'):
                href = rss_link.get('href')
                # If it's a relative URL, make it absolute
                if href.startswith('/'):
                    return ECLASS_BASE_URL + href
                return href
            
            logging.warning(f"RSS feed link not found for course INF{course_id}")
            return None
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch announcements page for course INF{course_id}: {e}")
            return None
    
    def fetch_announcements(self, course_id: int) -> List[Dict]:
        """
        Fetch and parse announcements for a course.
        
        Args:
            course_id: The course ID (e.g., 161 for INF161)
        
        Returns:
            List of announcement dictionaries with keys:
            - announcement_id: Unique ID for the announcement
            - title: Announcement title
            - link: Link to the announcement detail page
            - description: HTML description/content
            - pub_date: Publication date as datetime object
        """
        rss_url = self.get_rss_url(course_id)
        
        if not rss_url:
            logging.warning(f"Cannot fetch announcements for course INF{course_id}: No RSS URL found")
            return []
        
        try:
            response = self.session.get(rss_url)
            response.raise_for_status()
            
            # Parse the RSS XML
            announcements = self._parse_rss(response.text)
            logging.info(f"Fetched {len(announcements)} announcements for course INF{course_id}")
            return announcements
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch RSS feed for course INF{course_id}: {e}")
            return []
        except ET.ParseError as e:
            logging.error(f"Failed to parse RSS XML for course INF{course_id}: {e}")
            return []
    
    def _parse_rss(self, rss_xml: str) -> List[Dict]:
        """
        Parse RSS XML and extract announcement items.
        
        Args:
            rss_xml: Raw RSS XML string
        
        Returns:
            List of parsed announcement dictionaries
        """
        announcements = []
        
        try:
            root = ET.fromstring(rss_xml.strip())
            
            # Navigate to channel items
            # <rss><channel><item>...</item></channel></rss>
            channel = root.find('channel')
            if channel is None:
                logging.warning("No channel element found in RSS feed")
                return []
            
            for item in channel.findall('item'):
                title = item.find('title')
                link = item.find('link')
                description = item.find('description')
                pub_date = item.find('pubDate')
                guid = item.find('guid')
                
                # Extract announcement ID from guid
                # Format: "Wed, 18 Feb 2026 17:48:07 +030098099"
                # The announcement ID is at the end after the timezone
                announcement_id = None
                if guid is not None and guid.text:
                    # Try to extract ID from guid text (format: datetime+timezone+id)
                    # Example: "Wed, 18 Feb 2026 17:48:07 +030098099"
                    guid_text = guid.text.strip()
                    # Split by space and take the last part which should contain the ID
                    parts = guid_text.rsplit(' ', 1)
                    if len(parts) == 2:
                        # Extract just the digits from the end
                        import re
                        match = re.search(r'(\d+)$', parts[1])
                        if match:
                            announcement_id = match.group(1)
                
                # If we couldn't extract from guid, try from the link
                # Format: https://eclass.aueb.gr/modules/announcements/index.php?an_id=98099&course=INF161
                if not announcement_id and link is not None and link.text:
                    import re
                    match = re.search(r'an_id=(\d+)', link.text)
                    if match:
                        announcement_id = match.group(1)
                
                # Parse the publication date
                # Format: "Wed, 18 Feb 2026 17:48:07 +0300"
                pub_date_obj = None
                if pub_date is not None and pub_date.text:
                    pub_date_obj = self._parse_rfc2822_date(pub_date.text.strip())
                
                announcement = {
                    'announcement_id': announcement_id,
                    'title': title.text.strip() if title is not None and title.text else 'Untitled',
                    'link': link.text.strip() if link is not None and link.text else '',
                    'description': description.text.strip() if description is not None and description.text else '',
                    'pub_date': pub_date_obj
                }
                
                announcements.append(announcement)
        
        except Exception as e:
            logging.error(f"Error parsing RSS feed: {e}", exc_info=True)
        
        return announcements
    
    def _parse_rfc2822_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse RFC 2822 date format used in RSS feeds.
        
        Args:
            date_str: Date string like "Wed, 18 Feb 2026 17:48:07 +0300"
        
        Returns:
            datetime object or None if parsing fails
        """
        try:
            # Python's datetime.strptime can handle RFC 2822 format
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            return dt
        except Exception as e:
            logging.warning(f"Failed to parse date '{date_str}': {e}")
            return None

    def fetch_global_feed(self, feed_key: str) -> List[Dict]:
        """Fetch announcements for a global (non-course) feed.

        Dispatches to the authenticated eclass path for feeds with an
        eclass_course_id, scrapes an eclass page_url for feeds whose RSS
        token must be extracted from a public page, or directly fetches
        the RSS URL for plain public feeds.

        Args:
            feed_key: One of the keys in GLOBAL_FEEDS.

        Returns:
            List of announcement dicts (same shape as fetch_announcements).
        """
        feed = GLOBAL_FEEDS.get(feed_key)
        if not feed:
            logging.warning(f"Unknown global feed key: {feed_key}")
            return []

        if feed['eclass_course_id'] is not None:
            # Reuse the existing authenticated fetch path
            return self.fetch_announcements(feed['eclass_course_id'])

        if feed.get('page_url'):
            # Public eclass page — scrape RSS URL without authentication
            return self._fetch_from_page_url(feed['page_url'], feed_key)

        # Plain public RSS URL — no authentication required
        return self._fetch_direct_rss(feed['url'], feed_key)

    def _fetch_from_page_url(self, page_url: str, feed_key: str) -> List[Dict]:
        """Fetch the RSS token URL from a public eclass announcements page, then fetch it."""
        try:
            resp = requests.get(page_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            rss_link = soup.find('a', class_='tiny-icon-rss')
            if not rss_link or not rss_link.get('href'):
                logging.warning(f"RSS link not found on page {page_url}")
                return []
            href = rss_link['href']
            rss_url = (ECLASS_BASE_URL + href) if href.startswith('/') else href
            return self._fetch_direct_rss(rss_url, feed_key)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch page_url for feed '{feed_key}': {e}")
            return []

    def _fetch_direct_rss(self, url: str, feed_key: str) -> List[Dict]:
        """Fetch and parse a plain public RSS feed URL without authentication."""
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            announcements = self._parse_rss(resp.text)

            # For public feeds the guid might be a full URL; use it as announcement_id
            for ann in announcements:
                if not ann.get('announcement_id') and ann.get('link'):
                    ann['announcement_id'] = ann['link']

            logging.info(f"Fetched {len(announcements)} announcements for global feed '{feed_key}'")
            return announcements
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch global feed '{feed_key}' from {url}: {e}")
            return []
        except Exception as e:
            logging.error(f"Failed to parse global feed '{feed_key}': {e}", exc_info=True)
            return []
