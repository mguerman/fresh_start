#!/bin/bash
# filepath: /home/mguerman/fresh_start/check_grpc_servers.sh

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
    ["g"]=4001
    ["h"]=4002
    ["z"]=4003
    ["excl"]=4004
)

DAGSTER_HOST="dagster.ist-apps-cnt-sb.bu.edu"

check_server() {
    local prefix=$1
    local port=$2
    local status="❌ STOPPED"
    
    # Check if port is listening
    if lsof -i :$port > /dev/null 2>&1; then
        status="✅ RUNNING"
    fi
    
    printf "%-8s Port %-4s %s\n" "$prefix" "$port" "$status"
}

show_processes() {
    log_info "Dagster gRPC processes:"
    if pgrep -f "dagster api grpc" > /dev/null; then
        ps aux | grep "dagster api grpc" | grep -v grep | while read line; do
            echo "  $line"
        done
    else
        echo "  No Dagster gRPC processes found"
    fi
}

show_port_usage() {
    log_info "Port usage:"
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        if lsof -i :$port > /dev/null 2>&1; then
            echo -n "  Port $port: "
            lsof -i :$port | tail -n +2 | head -1 | awk '{print $1 " (PID: " $2 ")"}'
        fi
    done
}

test_definitions() {
    log_info "Testing definition loading for each prefix:"
    
    for prefix in "${!SERVERS[@]}"; do
        echo -n "  $prefix: "
        
        # Test with environment variables loaded - FIXED QUOTES
        bash -c "
            # Load .env file if it exists
            if [ -f .env ]; then
                set -o allexport
                source .env
                set +o allexport
            fi
            
            # Set group prefix
            export GROUP_PREFIX=$prefix
            
            # Test definition loading with proper quote escaping
            python3 -c \"
import sys
sys.path.insert(0, '.')
import os
group_prefix = os.environ.get('GROUP_PREFIX', 'NOT_SET')
db_user = os.environ.get('DB_USER', 'NOT_SET')
print(f'GROUP_PREFIX: {group_prefix}')
if db_user != 'NOT_SET':
    print(f'DB_USER: {db_user[:3]}***')

try:
    from src.fresh_start.definitions import get_defs
    defs = get_defs()
    print(f'✅ {len(defs.assets)} assets, {len(defs.jobs)} jobs, {len(defs.schedules)} schedules')
except Exception as e:
    print(f'❌ {e}')
\"
        " 2>/dev/null || echo "❌ Failed to test definitions"
    done
}

check_logs() {
    log_info "Recent log activity:"
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        log_file="logs/grpc_${prefix}_${port}.log"
        
        if [ -f "$log_file" ]; then
            # Check for recent errors
            if tail -20 "$log_file" | grep -q -E "(ERROR|CRITICAL|Failed|Exception)" 2>/dev/null; then
                echo "  $prefix: ⚠️  Recent errors detected"
                echo "    Latest error:"
                tail -20 "$log_file" | grep -E "(ERROR|CRITICAL|Failed)" | tail -1 | sed 's/^/      /'
            else
                # Check if server finished loading
                if tail -10 "$log_file" | grep -q "Started Dagster code server" 2>/dev/null; then
                    echo "  $prefix: ✅ Definitions loaded successfully"
                elif tail -10 "$log_file" | grep -q "Building definitions" 2>/dev/null; then
                    echo "  $prefix: 🔄 Still loading definitions..."
                else
                    echo "  $prefix: ✅ Running normally"
                fi
            fi
        else
            echo "  $prefix: ❌ Log file missing"
        fi
    done
}

main() {
    echo "=== Dagster gRPC Server Status ==="
    echo ""
    
    echo "Server Status:"
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        check_server $prefix $port
    done
    echo ""
    
    show_processes
    echo ""
    
    show_port_usage
    echo ""
    
    test_definitions
    echo ""
    
    check_logs
    echo ""
    
    echo "Log files:"
    for prefix in "${!SERVERS[@]}"; do
        port=${SERVERS[$prefix]}
        log_file="logs/grpc_${prefix}_${port}.log"
        if [ -f "$log_file" ]; then
            size=$(du -h "$log_file" | cut -f1)
            echo "  $prefix: $log_file ($size)"
        else
            echo "  $prefix: $log_file (missing)"
        fi
    done
    
    echo ""
    echo "Commands:"
    echo "  View logs: tail -f logs/grpc_<prefix>_<port>.log"
    echo "  Stop all:  ./stop_grpc_servers.sh"
    echo "  Restart:   ./stop_grpc_servers.sh && ./start_grpc_servers.sh"
}

main "$@"