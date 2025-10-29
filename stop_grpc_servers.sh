#!/bin/bash
# filepath: /home/mguerman/fresh_start/stop_grpc_servers.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

declare -A SERVERS=(
    ["ora"]=4000
    ["d"]=4001
    ["f"]=4002
    ["g"]=4003
    ["h"]=4004
    ["z"]=4005
    ["excl"]=4006
)

stop_server() {
    local prefix=$1
    local port=$2
    local pid_file="logs/grpc_${prefix}.pid"
    
    log_info "Stopping gRPC server for $prefix (port $port)..."
    
    # Try to stop using saved PID first
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            log_info "Stopping process $pid (from PID file)"
            kill $pid
            sleep 2
            
            # Force kill if needed
            if kill -0 $pid 2>/dev/null; then
                log_warn "Force killing process $pid"
                kill -9 $pid 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi
    
    # Kill any processes using the port
    if lsof -i :$port > /dev/null 2>&1; then
        log_warn "Force killing processes on port $port"
        lsof -ti :$port | xargs kill -9 2>/dev/null || true
    fi
    
    log_info "✅ Stopped $prefix server"
}

main() {
    log_info "Stopping all Dagster gRPC servers..."
    
    # Stop individual servers
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        stop_server $prefix $port
    done
    
    # Clean up any remaining dagster grpc processes
    if pgrep -f "dagster api grpc" > /dev/null; then
        log_warn "Cleaning up remaining Dagster gRPC processes..."
        pkill -f "dagster api grpc" || true
        sleep 2
        pkill -9 -f "dagster api grpc" 2>/dev/null || true
    fi
    
    log_info "✅ All gRPC servers stopped"
    
    # Clean up PID files and environment scripts
    rm -f logs/grpc_*.pid 2>/dev/null || true
    rm -f logs/env_*.sh 2>/dev/null || true
    
    # Show final status
    echo ""
    log_info "Final cleanup:"
    if pgrep -f "dagster api grpc" > /dev/null; then
        log_warn "Some Dagster processes may still be running:"
        ps aux | grep "dagster api grpc" | grep -v grep
    else
        log_info "No Dagster gRPC processes running"
    fi
    
    # Check ports
    local ports_still_used=()
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        if lsof -i :$port > /dev/null 2>&1; then
            ports_still_used+=($port)
        fi
    done
    
    if [ ${#ports_still_used[@]} -gt 0 ]; then
        log_warn "Ports still in use: ${ports_still_used[*]}"
    else
        log_info "All target ports are free"
    fi
}

main "$@"