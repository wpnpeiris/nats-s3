#!/bin/bash

##################################
# Benchmark Script for NATS-S3 Gateway
#
# This script sets up a NATS server and a NATS-S3 gateway,
# runs a benchmark using the 'warp' tool, and ensures cleanup
# of all resources upon completion or interruption.
#
# This scripts assumes that Docker, NATS server
# and warp (https://github.com/minio/warp) are installed
# and available in the system's PATH.
#
# Also, ensure that you have a valid 'credentials.json' file in your working directory.
# The Access Key and Secret Key in the credentials file should both be set to 'testtest'.
##################################

set -e  # Exit on error

# Cleanup function to be called on exit
cleanup() {
    echo "Cleaning up..."
    
    # Stop NATS-S3 container if running
    if [ ! -z "$NATS_S3_CONTAINER_ID" ]; then
        echo "Stopping NATS-S3 container..."
        docker stop "$NATS_S3_CONTAINER_ID" 2>/dev/null || true
        docker rm "$NATS_S3_CONTAINER_ID" 2>/dev/null || true
    fi
    
    # Stop NATS server if running
    if [ ! -z "$NATS_PID" ]; then
        echo "Stopping NATS server (PID: $NATS_PID)..."
        kill "$NATS_PID" 2>/dev/null || true
        wait "$NATS_PID" 2>/dev/null || true
    fi
    
    # Remove NATS JetStream data directory
    if [ -d "/tmp/nats/jetstream" ]; then
        echo "Removing NATS JetStream data directory..."
        rm -rf /tmp/nats/jetstream
    fi
    
    echo "Cleanup complete."
}

# Register cleanup function to run on script exit
trap cleanup EXIT INT TERM

echo "Starting NATS server..."
nats-server -js &
NATS_PID=$!
echo "NATS server started with PID: $NATS_PID"

# Wait for NATS to be ready
sleep 2

echo "Starting NATS-S3 gateway..."
NATS_S3_CONTAINER_ID=$(docker run -d --network host -p 5222:5222 \
  -v $(pwd)/credentials.json:/credentials.json \
  wpnpeiris/nats-s3:v0.3.8 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials /credentials.json)
echo "NATS-S3 gateway started with container ID: $NATS_S3_CONTAINER_ID"

# Wait for NATS-S3 to be ready
sleep 3

echo "Running benchmark..."
warp mixed --host=localhost:5222 \
    --access-key=testtest \
    --secret-key=testtest \
    --duration 20s \
    --obj.size 1M \
    --concurrent=1 \
    --objects=10 \
    --get-distrib 1 \
    --stat-distrib 1 \
    --put-distrib 2 \
    --delete-distrib 1

echo "Benchmark completed successfully!"
