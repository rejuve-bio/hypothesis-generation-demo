#!/bin/bash
# Entrypoint script for Flask application with database seeding

set -e

echo "=============================================="
echo "Starting Application Initialization"
echo "=============================================="

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to be ready..."
if [ -z "$MONGODB_URI" ]; then
    echo "ERROR: MONGODB_URI is not set. api-service cannot connect to MongoDB."
    exit 1
fi

max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    mongo_err=$(python3 -c "
from pymongo import MongoClient
import os
uri = os.environ.get('MONGODB_URI', '')
c = MongoClient(uri, serverSelectionTimeoutMS=5000)
c.admin.command('ping')
" 2>&1)
    mongo_rc=$?

    if [ "$mongo_rc" -eq 0 ]; then
        echo "✓ MongoDB is ready!"
        break
    fi

    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts - MongoDB not ready yet..."
    # Show first line of error so logs are actionable
    echo "   ↳ $(echo "$mongo_err" | head -1)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "MongoDB failed to become ready after $max_attempts attempts"
    echo "Last error:"
    echo "$mongo_err"
    echo "Continuing anyway - application will retry connections"
fi

echo ""
echo "=============================================="
echo "Running Database Seeding"
echo "=============================================="

# Run database seeding
python3 src/seed_database.py


# Execute the original command
exec "$@"
