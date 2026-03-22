"""
Handles scraping and parsing of course exercises from e-class.
"""
import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup


ECLASS_BASE_URL = "https://eclass.aueb.gr"
EXERCISES_URL_TEMPLATE = f"{ECLASS_BASE_URL}/modules/work/index.php?course=INF{{}}"


class ExercisesScraper:
    """Handles fetching and parsing course exercises."""

    def __init__(self, session: requests.Session):
        self.session = session

    def fetch_exercises(self, course_id: int) -> List[Dict]:
        """
        Fetch and parse exercises for a course.
        Fetches the list page, then each exercise's detail page.

        Returns dicts with keys:
            exercise_id, title, link, deadline, submission_status, grade, work_type,
            description, start_date, max_grade,
            assignment_file_name, assignment_file_url,
            grade_comments, submission_date
        """
        url = EXERCISES_URL_TEMPLATE.format(course_id)
        try:
            response = self.session.get(url)
            response.raise_for_status()
            exercises = self._parse_list(response.text, course_id)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch exercises list for course INF{course_id}: {e}")
            return []

        for ex in exercises:
            try:
                detail = self._fetch_detail(ex['link'])
                # Grade from detail page takes priority if non-empty
                grade_from_detail = detail.pop('grade', '')
                if grade_from_detail:
                    ex['grade'] = grade_from_detail
                ex.update(detail)
            except Exception as e:
                logging.warning(f"Failed to fetch exercise detail for {ex['link']}: {e}")

        logging.info(f"Fetched {len(exercises)} exercises for course INF{course_id}")
        return exercises

    # ------------------------------------------------------------------ #
    #  List page parser
    # ------------------------------------------------------------------ #

    def _parse_list(self, html: str, course_id: int) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')

        table = soup.find('table', id=f'assignment_table_INF{course_id}')
        if not table:
            logging.warning(f"Exercise table not found for course INF{course_id}")
            return []

        tbody = table.find('tbody')
        if not tbody:
            return []

        exercises = []
        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 3:
                continue

            # --- Title & link ---
            title_cell = cells[0]
            link_tag = title_cell.find('a')
            if not link_tag:
                continue
            title = link_tag.get_text(strip=True)
            href = link_tag.get('href', '')
            link = (ECLASS_BASE_URL + href) if href.startswith('/') else href

            match = re.search(r'[?&]id=(\d+)', href)
            if not match:
                continue
            exercise_id = match.group(1)

            work_type = ""
            small = title_cell.find('small', class_='text-muted')
            if small:
                work_type = small.get_text(strip=True)

            # --- Deadline (first text node before the <div>) ---
            deadline_str = ""
            for node in cells[1].children:
                if hasattr(node, 'name') and node.name is not None:
                    break
                text = str(node).strip()
                if text:
                    deadline_str = text
                    break

            # --- Submission status ---
            status_icon = cells[2].find('i')
            if status_icon and 'fa-check' in status_icon.get('class', []):
                submission_status = 'submitted'
            else:
                submission_status = 'pending'

            # --- Grade from list (fallback) ---
            grade = ""
            if len(cells) >= 4:
                grade_text = cells[3].get_text(strip=True)
                if grade_text and grade_text != '-':
                    grade = grade_text

            exercises.append({
                'exercise_id': exercise_id,
                'title': title,
                'link': link,
                'deadline': deadline_str,
                'submission_status': submission_status,
                'grade': grade,
                'work_type': work_type,
                # Detail fields (populated by _fetch_detail)
                'description': '',
                'start_date': '',
                'max_grade': '',
                'assignment_file_name': '',
                'assignment_file_url': '',
                'grade_comments': '',
                'submission_date': '',
            })

        return exercises

    # ------------------------------------------------------------------ #
    #  Detail page fetcher / parser
    # ------------------------------------------------------------------ #

    def _fetch_detail(self, url: str) -> Dict:
        response = self.session.get(url)
        response.raise_for_status()
        return self._parse_detail_page(response.text)

    def _parse_detail_page(self, html: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        result = {
            'description': '',
            'start_date': '',
            'max_grade': '',
            'assignment_file_name': '',
            'assignment_file_url': '',
            'grade': '',
            'grade_comments': '',
            'submission_date': '',
        }

        for card in soup.find_all('div', class_='panelCard'):
            header = card.find('div', class_='card-header')
            body = card.find('div', class_='card-body')
            if not header or not body:
                continue
            h3 = header.find('h3')
            if not h3:
                continue
            section = h3.get_text(strip=True)

            # Build label → BeautifulSoup element dict for this card
            fields = {}
            for li in body.find_all('li', class_='list-group-item'):
                title_div = li.find('div', class_='title-default')
                value_div = li.find('div', class_='title-default-line-height')
                if not title_div or not value_div:
                    continue
                label = title_div.get_text(strip=True).rstrip(':')
                fields[label] = value_div

            if 'Στοιχεία εργασίας' in section:
                if 'Περιγραφή' in fields:
                    result['description'] = fields['Περιγραφή'].decode_contents().strip()
                if 'Ημερομηνία Έναρξης' in fields:
                    # First text node only (skip the "remaining time" <div>)
                    result['start_date'] = _first_text(fields['Ημερομηνία Έναρξης'])
                if 'Μέγιστη βαθμολογία' in fields:
                    result['max_grade'] = fields['Μέγιστη βαθμολογία'].get_text(strip=True)
                if 'Αρχείο' in fields:
                    a = fields['Αρχείο'].find('a')
                    if a:
                        result['assignment_file_name'] = a.get('title') or a.get_text(strip=True)
                        href = a.get('href', '')
                        result['assignment_file_url'] = (ECLASS_BASE_URL + href) if href.startswith('/') else href

            elif 'Στοιχεία υποβολής' in section:
                if 'Βαθμός' in fields:
                    grade_text = fields['Βαθμός'].get_text(strip=True)
                    if grade_text and grade_text != '-':
                        result['grade'] = grade_text
                if 'Σχόλια βαθμολογητή' in fields:
                    result['grade_comments'] = fields['Σχόλια βαθμολογητή'].get_text(strip=True)
                if 'Ημ/νία αποστολής' in fields:
                    result['submission_date'] = fields['Ημ/νία αποστολής'].get_text(strip=True)

        return result


def _first_text(tag) -> str:
    """Return the first direct text node of a tag, stripped."""
    for node in tag.children:
        if hasattr(node, 'name') and node.name is not None:
            break
        text = str(node).strip()
        if text:
            return text
    return tag.get_text(strip=True)
