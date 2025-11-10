"""
Web application module.
FastAPI-based web interface for tree-eclass.
"""

from .app import app, db_manager

__all__ = ['app', 'db_manager']
