#!/bin/bash

# On the remote host (dagster.ist-apps-cnt-sb.bu.edu)
# Run this script to start all gRPC servers

declare -A SERVERS=(
    ["ora"]=4000
    ["g"]=4001
    ["h"]=4002
    ["z"]=4003
    ["excl"]=4004
)

# Create logs directory
mkdir -p logs

for prefix in "${!SERVERS[@]}"; do
    port=${SERVERS[$prefix]}
    echo "Starting gRPC server for prefix '$prefix' on port $port..."
    
    # The entry point is specified HERE with --attribute
    GROUP_PREFIX=$prefix nohup dagster api grpc \
        --python-file src/fresh_start/definitions.py \
        --attribute get_defs \
        --host 0.0.0.0 \
        --port $port \
        > logs/grpc_${prefix}_${port}.log 2>&1 &
    
    sleep 2  # Give server time to start
    echo "Started gRPC server for $prefix (PID: $!)"
done

echo "All gRPC servers started. Logs in logs/ directory."
echo "To stop all: pkill -f 'dagster api grpc'"