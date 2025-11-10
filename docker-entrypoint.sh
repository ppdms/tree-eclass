#!/bin/bash
set -e

echo "Starting tree-eclass..."

# Ensure data directory exists
mkdir -p /data

# Check if database exists, if not, initialize
if [ ! -f "/data/eclass.db" ]; then
    echo "Initializing database..."
fi

# Start the application
exec python3 run.py
