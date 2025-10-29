#!/bin/bash
# filepath: /home/mguerman/fresh_start/test_grpc_definitions.sh

# Test that each prefix loads the correct groups
declare -A SERVERS=(
    ["ora"]=4000
    ["g"]=4001
    ["h"]=4002
    ["z"]=4003
    ["excl"]=4004
)

echo "=== Testing Definition Loading by Prefix ==="
echo ""

for prefix in "${!SERVERS[@]}"; do
    echo "Testing prefix: $prefix"
    echo "----------------------------------------"
    
    GROUP_PREFIX=$prefix python3 -c "
import sys
sys.path.insert(0, '.')

try:
    from src.fresh_start.definitions import get_defs
    defs = get_defs()
    
    print(f'Assets: {len(defs.assets)}')
    print(f'Jobs: {len(defs.jobs)}')
    print(f'Schedules: {len(defs.schedules)}')
    
    # Show first few asset groups
    if defs.assets:
        groups = set()
        for asset in list(defs.assets)[:10]:  # Sample first 10 assets
            if hasattr(asset, 'group_names') and asset.group_names:
                groups.update(asset.group_names)
        print(f'Sample groups: {sorted(list(groups))[:5]}')
    
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
"
    echo ""
done