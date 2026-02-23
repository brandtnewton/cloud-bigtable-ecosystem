#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
SERVER_BINARY=./cassandra-bigtable-proxy
SERVER_PORT=9042
TEST_COMMAND="go test ./testing/compliance/"

echo "🚀 Building the application..."
go build -o $SERVER_BINARY .

# Function to clean up the server process
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo "Shutting down server (PID: $SERVER_PID)..."
        kill $SERVER_PID
    fi
}

# Trap signals (like Ctrl+C or script failure) to ensure cleanup runs
trap cleanup EXIT

echo "🌐 Starting server..."
# Start server in the background and redirect logs
$SERVER_BINARY -f ~/cassandra-to-bigtable-proxy-config.yaml --log-level=debug 2>&1 &
SERVER_PID=$!

sleep 10

echo "✅ Server is up! Running integration tests..."
eval $TEST_COMMAND

echo "🎉 All tests passed!"
# The 'trap' will automatically call cleanup() here