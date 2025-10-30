"""
Dagster definitions with optimized loading and minimal overhead.
Supports distributed gRPC servers with GROUP_PREFIX environment variable.
"""

from pathlib import Path
import os
import dagster as dg
import threading
import pickle
import hashlib
import logging

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Thread-safe YAML cache
_yaml_cache = None
_cache_lock = threading.Lock()

# Paths and configuration
BASE_DIR = Path(__file__).parent
YAML_PATH = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
LOGS_DIR = BASE_DIR.parent.parent / "logs"

# Performance modes
FAST_MODE = os.environ.get("DAGSTER_FAST_MODE", "false").lower() == "true"
ULTRA_FAST_MODE = os.environ.get("DAGSTER_ULTRA_FAST", "false").lower() == "true"

# ═══════════════════════════════════════════════════════════════════════════════
# YAML CACHING SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

def _get_cache_key():
    """Generate cache key based on YAML file content."""
    if YAML_PATH.exists():
        with open(YAML_PATH, 'rb') as f:
            content = f.read()
        return hashlib.md5(content).hexdigest()[:8]
    return "no_yaml"

def _cleanup_old_cache_files(current_cache_file):
    """Remove old cache files to prevent disk bloat."""
    try:
        for old_cache in LOGS_DIR.glob("yaml_cache_*.pkl"):
            if old_cache != current_cache_file:
                old_cache.unlink()
    except Exception:
        pass  # Ignore cleanup errors

def _load_all_yaml_data():
    """Load and cache ALL YAML data with thread-safe caching."""
    global _yaml_cache
    
    # Check in-memory cache first (fastest)
    if _yaml_cache is not None:
        return _yaml_cache
    
    with _cache_lock:
        # Double-check pattern inside lock
        if _yaml_cache is not None:
            return _yaml_cache
        
        # Ensure logs directory exists
        LOGS_DIR.mkdir(exist_ok=True)
        
        # Try to load from file cache
        cache_key = _get_cache_key()
        cache_file = LOGS_DIR / f"yaml_cache_{cache_key}.pkl"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    _yaml_cache = pickle.load(f)
                return _yaml_cache
            except Exception:
                # Cache file corrupted, will rebuild
                pass
        
        # Load fresh data from YAML
        all_groups = load_enabled_groups(YAML_PATH)
        
        # Save to file cache
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(all_groups, f)
            _cleanup_old_cache_files(cache_file)
        except Exception:
            pass  # Ignore cache save errors
        
        # Set in-memory cache
        _yaml_cache = all_groups
        return _yaml_cache

# ═══════════════════════════════════════════════════════════════════════════════
# GROUP FILTERING AND VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_and_filter_groups(all_groups, target_group=None, group_prefix=None):
    """Validate inputs and filter groups based on criteria."""
    if not all_groups:
        raise ValueError("No groups loaded from YAML file")
    
    if target_group:
        # Filter to specific group
        filtered = [g for g in all_groups if g.get('name') == target_group]
        
        if not filtered:
            available = [g.get('name') for g in all_groups[:10]]
            raise ValueError(f"Group '{target_group}' not found. Available: {available}")
        
        return filtered
    
    elif group_prefix:
        # Filter by prefix
        filtered = [g for g in all_groups if g.get('name', '').startswith(group_prefix)]
        
        if not filtered:
            available_prefixes = sorted(set(
                g.get('name', '')[:3] for g in all_groups if g.get('name')
            ))
            raise ValueError(f"No groups with prefix '{group_prefix}'. Available: {available_prefixes}")
        
        return filtered
    
    else:
        # Return all groups
        return all_groups

# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCE CREATION
# ═══════════════════════════════════════════════════════════════════════════════

def _create_shared_resources():
    """Create shared resources with proper error handling."""
    try:
        return {
            "postgres": PostgresResource(
                db_user=os.environ["DB_USER"],
                db_password=os.environ["DB_PASSWORD"],
                db_host=os.environ["DB_HOST"],
                db_port=os.environ["DB_PORT"],
                db_name=os.environ["DB_NAME"],
            ),
            "oracle": OracleResource(
                db_user=os.environ["ORACLE_DB_USER"],
                db_password=os.environ["ORACLE_DB_PASSWORD"],
                db_host=os.environ["ORACLE_DB_HOST"],
                db_port=os.environ["ORACLE_DB_PORT"],
                db_service=os.environ["ORACLE_DB_SERVICE"],
            ),
        }
    except KeyError as e:
        raise ValueError(f"Missing required environment variable: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _calculate_staggered_schedule(group_name, all_group_names, base_hour=2, stagger_minutes=1):
    """Calculate staggered schedule for a group based on its global position."""
    try:
        group_index = all_group_names.index(group_name)
    except ValueError:
        group_index = 0
    
    # Calculate total offset
    total_stagger_minutes = group_index * stagger_minutes
    
    # Calculate final time
    schedule_hour = base_hour + (total_stagger_minutes // 60)
    schedule_minute = total_stagger_minutes % 60
    
    # Handle day overflow
    if schedule_hour >= 24:
        schedule_hour = schedule_hour % 24
    
    return schedule_hour, schedule_minute

def _create_jobs_and_schedules(filtered_groups, all_groups):
    """Create jobs and schedules for the filtered groups."""
    if ULTRA_FAST_MODE:
        # Skip jobs and schedules in ultra-fast mode
        return [], []
    
    jobs = []
    schedules = []
    
    # Get all group names for staggered scheduling
    all_group_names = [g.get('name') for g in all_groups if g.get('name')]
    
    for group in filtered_groups:
        group_name = group.get("name")
        if not group_name:
            continue

        # Create job
        job_name = f"replication_job_{group_name}"
        try:
            job = dg.define_asset_job(
                name=job_name,
                selection=dg.AssetSelection.groups(group_name),
                description=f"Replication job for group {group_name}",
            )
            jobs.append(job)
        except Exception:
            continue  # Skip failed jobs silently in fast mode

        # Create staggered schedule
        schedule_hour, schedule_minute = _calculate_staggered_schedule(
            group_name, all_group_names
        )
        
        cron_schedule = f"{schedule_minute} {schedule_hour} * * *"
        
        try:
            schedule = dg.ScheduleDefinition(
                job_name=job_name,
                cron_schedule=cron_schedule,
                execution_timezone="America/New_York",
                description=f"Daily replication for {group_name} at {schedule_hour:02d}:{schedule_minute:02d}",
            )
            schedules.append(schedule)
        except Exception:
            continue  # Skip failed schedules silently in fast mode
    
    return jobs, schedules

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DEFINITION BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _get_definitions_for_location(target_group=None, group_prefix=None):
    """Build Dagster definitions with performance optimization."""
    
    try:
        # Load all groups from YAML (cached)
        all_groups = _load_all_yaml_data()
        
        # Validate and filter groups
        filtered_groups = _validate_and_filter_groups(all_groups, target_group, group_prefix)
        
        # Ultra-fast mode: minimal definitions
        if ULTRA_FAST_MODE:
            # Only create a minimal set of assets for testing
            if len(filtered_groups) > 3:
                filtered_groups = filtered_groups[:3]  # Limit to first 3 groups
            
            all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
            
            # Minimal resources
            resources = {
                "postgres": PostgresResource(
                    db_user=os.environ.get("DB_USER", "test"),
                    db_password=os.environ.get("DB_PASSWORD", "test"),
                    db_host=os.environ.get("DB_HOST", "localhost"),
                    db_port=os.environ.get("DB_PORT", "5432"),
                    db_name=os.environ.get("DB_NAME", "test"),
                )
            }
            
            return dg.Definitions(
                assets=all_assets,
                resources=resources,
                jobs=[],  # Skip jobs in ultra-fast mode
                schedules=[],  # Skip schedules in ultra-fast mode
            )
        
        # Normal mode: full definitions
        all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
        resources = _create_shared_resources()
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups)
        
        # Build final definitions
        definitions = dg.Definitions(
            assets=all_assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
        )
        
        # Minimal output
        if not FAST_MODE:
            print(f"✅ {len(all_assets)} assets loaded")
        
        return definitions
        
    except Exception as e:
        print(f"❌ Failed to build definitions: {e}")
        raise

# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_defs():
    """
    Factory function to get Dagster definitions.
    
    Uses GROUP_PREFIX environment variable for distributed gRPC servers.
    Falls back to loading all groups if no prefix specified.
    """
    group_prefix = os.environ.get("GROUP_PREFIX")
    
    if group_prefix:
        return _get_definitions_for_location(group_prefix=group_prefix)
    else:
        return _get_definitions_for_location()

def get_defs_for_group(group_name):
    """Get definitions for a specific group (convenience function)."""
    return _get_definitions_for_location(target_group=group_name)

def get_defs_for_prefix(prefix):
    """Get definitions for groups with a specific prefix (convenience function)."""
    return _get_definitions_for_location(group_prefix=prefix)

def get_all_defs():
    """Get definitions for all groups (convenience function)."""
    return _get_definitions_for_location()

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL DEFINITIONS (Required for Dagster gRPC servers)
# ═══════════════════════════════════════════════════════════════════════════════

# Create module-level definitions based on environment
group_prefix = os.environ.get("GROUP_PREFIX")

if group_prefix:
    # gRPC server mode - create filtered definitions
    try:
        defs = get_defs()
        print(f"📍 gRPC Server Mode - Loaded {len(defs.assets)} assets for prefix: {group_prefix}")
    except Exception as e:
        print(f"❌ Failed to create definitions for {group_prefix}: {e}")
        # Create empty definitions to prevent server crash
        defs = dg.Definitions(
            assets=[],
            resources={},
            jobs=[],
            schedules=[]
        )
else:
    # Local development mode - check if we should skip module definitions
    if os.environ.get("SKIP_MODULE_DEFS"):
        # Skip module definitions entirely (useful for testing)
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])
    else:
        # Load all definitions for local development
        try:
            defs = get_defs()
            print(f"🌍 Local Mode - Loaded {len(defs.assets)} assets (all groups)")
        except Exception as e:
            print(f"⚠️ Local mode failed, creating empty definitions: {e}")
            defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])