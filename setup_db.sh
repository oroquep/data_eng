#!/bin/bash

# Go to the script's directory
cd "$(dirname "$0")"

# Manually add venv's Scripts folder to PATH (Windows-style virtualenv)
export PATH="./venv/Scripts:$PATH"

# Start the PostgreSQL container
docker-compose up -d

# Wait for the database to be ready
echo "Waiting for database to be ready..."
sleep 10

# Run DB init with venv Python
python init_db.py

echo "Database setup complete!"