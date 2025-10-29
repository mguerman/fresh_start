#!/bin/bash
# Stop all gRPC servers
pkill -f "dagster api grpc"
echo "All gRPC servers stopped"