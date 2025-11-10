#!/usr/bin/env python3
"""
Entry point for running the tree-eclass web application.
"""

import sys
import uvicorn

from app.web.app import app

if __name__ == "__main__":
    uvicorn.run(
        "app.web.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False
    )
