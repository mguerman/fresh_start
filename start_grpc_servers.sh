#!/bin/bash
# filepath: /home/mguerman/fresh_start/start_grpc_servers.sh

set -e  # Exit on any error

# Configuration
DAGSTER_HOST="localhost"  # From your workspace.yaml
MODULE_NAME="src.fresh_start.definitions"

declare -A SERVERS=(
    ["ora"]=4000
    ["d"]=4001
    ["f"]=4002
    ["g"]=4003
    ["h"]=4004
    ["z"]=4005
    ["excl"]=4006
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Function to check if port is in use
check_port() {
    local port=$1
    if lsof -i :$port > /dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Function to kill existing dagster grpc processes
cleanup_existing() {
    log_info "Checking for existing Dagster gRPC processes..."
    
    # Kill existing dagster grpc processes
    if pgrep -f "dagster api grpc" > /dev/null; then
        log_warn "Found existing Dagster gRPC processes. Terminating..."
        pkill -f "dagster api grpc" || true
        sleep 3
        
        # Force kill if still running
        if pgrep -f "dagster api grpc" > /dev/null; then
            log_warn "Force killing stubborn processes..."
            pkill -9 -f "dagster api grpc" || true
            sleep 2
        fi
    fi
    
    # Check for processes still using our ports
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        if check_port $port; then
            log_warn "Port $port still in use. Trying to free it..."
            lsof -ti :$port | xargs kill -9 2>/dev/null || true
            sleep 1
        fi
    done
}

# Function to start a single gRPC server
start_server() {
    local prefix=$1
    local port=$2
    local log_file="logs/grpc_${prefix}_${port}.log"
    
    log_info "Starting gRPC server for prefix '$prefix' on port $port..."
    
    # Check if port is still in use
    if check_port $port; then
        log_error "Port $port is still in use. Cannot start $prefix server."
        return 1
    fi
    
    # Start server with environment variables loaded
    nohup bash -c "
        # Load .env file if it exists
        if [ -f .env ]; then
            set -o allexport
            source .env
            set +o allexport
        fi
        
        # Set group prefix
        export GROUP_PREFIX=$prefix
        
        # Start Dagster gRPC server
        dagster api grpc \\
            --module-name $MODULE_NAME \\
            --host $DAGSTER_HOST \\
            --port $port
    " > $log_file 2>&1 &
    
    local pid=$!
    
    # Wait time based on expected server size
    local wait_time=5  # Default
    case $prefix in
        "excl")
            wait_time=40  # excl has ~30k assets
            log_info "⏳ Waiting ${wait_time}s for large excl prefix..."
            ;;
        "h"|"d"|"f"|"g")
            wait_time=30  # h has ~15k assets
            log_info "⏳ Waiting ${wait_time}s for medium h prefix (14,946 assets)..."
            ;;
        "ora"|"z")
            wait_time=15   # Small prefixes
            log_info "⏳ Waiting ${wait_time}s for small $prefix prefix..."
            ;;
    esac
    
    # Wait with progress indication for large servers
    if [ $wait_time -gt 10 ]; then
        for ((i=1; i<=wait_time; i+=5)); do
            sleep 5
            if ! kill -0 $pid 2>/dev/null; then
                log_error "Process died during startup at ${i}s"
                return 1
            fi
            
            # Check if server is responding yet
            if check_port $port; then
                log_info "✅ Server started early at ${i}s"
                break
            fi
            
            if [ $i -lt $wait_time ]; then
                echo -n "."  # Progress indicator
            fi
        done
        echo ""  # New line after progress dots
    else
        sleep $wait_time
    fi
    
    # Verify the server actually started
    if ! kill -0 $pid 2>/dev/null; then
        log_error "Failed to start gRPC server for $prefix (process died)"
        log_error "Check log: $log_file"
        return 1
    fi
    
    # Verify port is now in use
    if ! check_port $port; then
        log_error "gRPC server for $prefix started but port $port not listening"
        log_error "Check log: $log_file"
        return 1
    fi
    
    log_info "✅ Started gRPC server for $prefix (PID: $pid, Port: $port)"
    echo $pid > "logs/grpc_${prefix}.pid"
    
    # Quick success check in log
    if grep -q "Started Dagster code server" $log_file 2>/dev/null; then
        log_info "🎉 $prefix server confirmed started"
    else
        log_warn "⚠️  $prefix server may still be loading definitions..."
    fi
    
    return 0
}

# Function to test server responsiveness
test_server() {
    local prefix=$1
    local port=$2
    
    log_info "Testing gRPC server responsiveness for $prefix on port $port..."
    
    # Simple connection test using Python
    python3 -c "
import socket
import sys
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex(('$DAGSTER_HOST', $port))
    sock.close()
    if result == 0:
        print('✅ Port $port is responding')
        sys.exit(0)
    else:
        print('❌ Port $port is not responding')
        sys.exit(1)
except Exception as e:
    print(f'❌ Connection test failed: {e}')
    sys.exit(1)
" && log_info "✅ $prefix server responding" || log_error "❌ $prefix server not responding"
}

# Main execution
main() {
    log_info "Starting Dagster gRPC servers setup..."
    
    # Create logs directory
    mkdir -p logs
    
    # Cleanup existing processes
    cleanup_existing
    
    # Start all servers
    local failed_servers=()
    local started_servers=()
    
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        
        if start_server $prefix $port; then
            started_servers+=($prefix)
        else
            failed_servers+=($prefix)
            log_error "Failed to start server for prefix: $prefix"
        fi
    done
    
    # Summary
    log_info "Server startup complete!"
    log_info "Started servers: ${started_servers[*]}"
    
    if [ ${#failed_servers[@]} -gt 0 ]; then
        log_error "Failed servers: ${failed_servers[*]}"
        log_error "Check individual log files in logs/ directory"
    fi
    
    # Test server responsiveness
    sleep 5
    log_info "Testing server responsiveness..."
    for prefix in "${started_servers[@]}"; do
        port=${SERVERS[$prefix]}
        test_server $prefix $port
    done
    
    # Display useful information
    echo ""
    log_info "=== Server Status ==="
    echo "Servers running: ${#started_servers[@]}/${#SERVERS[@]}"
    echo "Log files:"
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        echo "  $prefix: logs/grpc_${prefix}_${port}.log"
    done
    echo ""
    echo "To stop all servers: ./stop_grpc_servers.sh"
    echo "To check status: ./check_grpc_servers.sh"
    echo "To view logs: tail -f logs/grpc_<prefix>_<port>.log"
}

# Run main function
main "$@"