"""
Service layer for tree-eclass.
Contains business logic for scraping, building, diffing, and persistence.
"""

from .scraper import Scraper
from .tree_builder import build_tree, Node, File
from .differ import diff_trees
from .persistence import DatabaseManager
from .checker import run_checker, print_tree, send_email

__all__ = [
    'Scraper',
    'build_tree',
    'diff_trees',
    'print_tree',
    'send_email',
    'DatabaseManager',
    'run_checker',
    'Node',
    'File'
]
