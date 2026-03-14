#!/bin/bash
set -e

echo "Starting tree-eclass..."

# Ensure data directory exists
mkdir -p /data

# Start the application
exec python3 run.py
