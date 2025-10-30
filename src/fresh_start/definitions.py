"""
Dagster definitions with optimized loading and worker process detection.
Supports distributed gRPC servers with GROUP_PREFIX environment variable.

Key features:
- Worker process detection to prevent redundant asset loading (fixes 100% CPU)
- YAML caching with pickle for faster subsequent loads
- Staggered scheduling to prevent all jobs running simultaneously
- Support for ULTRA_FAST_MODE for development
"""

from pathlib import Path
import os
import sys
import dagster as dg
import threading
import pickle
import hashlib

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
YAML_PATH = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
LOGS_DIR = BASE_DIR.parent.parent / "logs"

# Performance modes
FAST_MODE = os.environ.get("DAGSTER_FAST_MODE", "false").lower() == "true"
ULTRA_FAST_MODE = os.environ.get("DAGSTER_ULTRA_FAST", "false").lower() == "true"

# Global caches
_yaml_cache = None
_cache_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════════════════════
# WORKER PROCESS DETECTION (CRITICAL FOR CPU OPTIMIZATION)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_worker_process():
    """
    Detect if this is a worker subprocess.
    Workers are forked child processes, not the main gRPC server.
    """
    import os
    
    # Check if we're in a forked subprocess
    # Workers are spawned by multiprocessing and have a parent process
    try:
        # If DAGSTER_IS_CAPTURING_LOGS is set, we're in a worker
        if os.environ.get("DAGSTER_IS_CAPTURING_LOGS"):
            return True
        
        # Check if we're the main process or a fork
        # Main gRPC server process will have this set to main process
        import multiprocessing
        current_process = multiprocessing.current_process()
        
        # If not MainProcess, we're a worker
        if current_process.name != 'MainProcess':
            return True
            
    except Exception:
        pass
    
    return False

# ═══════════════════════════════════════════════════════════════════════════════
# YAML CACHING SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

def _get_cache_key():
    """
    Generate cache key based on YAML file content hash.
    This ensures cache is invalidated when YAML changes.
    
    Returns:
        str: 8-character MD5 hash prefix of YAML content
    """
    try:
        if YAML_PATH.exists():
            with open(YAML_PATH, 'rb') as f:
                content = f.read()
            return hashlib.md5(content).hexdigest()[:8]
    except Exception as e:
        if not FAST_MODE:
            print(f"⚠️ Cache key generation failed: {e}")
    
    return "no_yaml"

def _cleanup_old_cache_files(current_cache_file):
    """
    Remove old pickle cache files to prevent disk bloat.
    Keeps only the current cache file based on YAML hash.
    
    Args:
        current_cache_file: Path to the current cache file to preserve
    """
    try:
        cache_count = 0
        for old_cache in LOGS_DIR.glob("yaml_cache_*.pkl"):
            if old_cache != current_cache_file:
                old_cache.unlink()
                cache_count += 1
        
        if cache_count > 0 and not FAST_MODE:
            print(f"🧹 Cleaned up {cache_count} old cache files")
            
    except Exception as e:
        if not FAST_MODE:
            print(f"⚠️ Cache cleanup failed: {e}")

def _load_all_yaml_data():
    """
    Load and cache all YAML group data with thread-safe caching.
    
    Process:
    1. Check in-memory cache (_yaml_cache)
    2. If not cached, check pickle file cache
    3. If no pickle cache, load from YAML
    4. Save to pickle cache for next time
    
    Returns:
        list: List of group dictionaries from YAML file
    """
    global _yaml_cache
    
    # Return in-memory cache if available
    if _yaml_cache is not None:
        return _yaml_cache
    
    # Thread-safe initialization
    with _cache_lock:
        # Double-check inside lock
        if _yaml_cache is not None:
            return _yaml_cache
        
        try:
            # Ensure logs directory exists
            LOGS_DIR.mkdir(exist_ok=True)
            
            # Try to load from pickle cache
            cache_key = _get_cache_key()
            cache_file = LOGS_DIR / f"yaml_cache_{cache_key}.pkl"
            
            if cache_file.exists():
                try:
                    with open(cache_file, 'rb') as f:
                        _yaml_cache = pickle.load(f)
                    
                    if not FAST_MODE:
                        print(f"💾 Loaded {len(_yaml_cache)} groups from pickle cache")
                    
                    return _yaml_cache
                except Exception as e:
                    if not FAST_MODE:
                        print(f"⚠️ Pickle cache corrupted, reloading: {e}")
            
            # Load from YAML file (slowest path)
            if not FAST_MODE:
                print(f"📄 Loading groups from YAML: {YAML_PATH}")
            
            all_groups = load_enabled_groups(YAML_PATH)
            
            if not FAST_MODE:
                print(f"✅ Loaded {len(all_groups)} groups from YAML")
            
            # Save to pickle cache for next time
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(all_groups, f)
                _cleanup_old_cache_files(cache_file)
                
                if not FAST_MODE:
                    print(f"💾 Saved pickle cache: {cache_file.name}")
                    
            except Exception as e:
                if not FAST_MODE:
                    print(f"⚠️ Failed to save pickle cache: {e}")
            
            _yaml_cache = all_groups
            return _yaml_cache
            
        except Exception as e:
            print(f"❌ Failed to load YAML data: {e}")
            import traceback
            traceback.print_exc()
            _yaml_cache = []
            return _yaml_cache

# ═══════════════════════════════════════════════════════════════════════════════
# GROUP FILTERING AND VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _filter_groups(all_groups, target_group=None, group_prefix=None):
    """
    Filter groups based on target_group or group_prefix.
    
    Args:
        all_groups: List of all group dictionaries
        target_group: Specific group name (e.g., "d10")
        group_prefix: Group prefix (e.g., "d" for all d* groups)
    
    Returns:
        list: Filtered list of group dictionaries
    
    Raises:
        ValueError: If no groups match the filter criteria
    """
    if not all_groups:
        return []
    
    # Filter by specific group
    if target_group:
        filtered = [g for g in all_groups if g.get('name') == target_group]
        if not filtered:
            available = [g.get('name') for g in all_groups[:10]]
            raise ValueError(
                f"Group '{target_group}' not found. "
                f"Available (first 10): {available}"
            )
        return filtered
    
    # Filter by prefix
    elif group_prefix:
        filtered = [g for g in all_groups if g.get('name', '').startswith(group_prefix)]
        if not filtered:
            available_prefixes = sorted(set(
                g.get('name', '')[:3] for g in all_groups if g.get('name')
            ))
            raise ValueError(
                f"No groups with prefix '{group_prefix}'. "
                f"Available prefixes: {available_prefixes}"
            )
        return filtered
    
    # No filter - return all
    return all_groups

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _calculate_staggered_schedule(group_name, all_group_names, base_hour=2, stagger_minutes=1):
    """
    Calculate staggered schedule time for a group to prevent thundering herd.
    
    Example with 3 groups and base_hour=2, stagger_minutes=1:
    - Group 0: 02:00 AM
    - Group 1: 02:01 AM  
    - Group 2: 02:02 AM
    
    Args:
        group_name: Name of the group to schedule
        all_group_names: List of all group names (for calculating position)
        base_hour: Starting hour for schedules (default: 2 AM)
        stagger_minutes: Minutes between each group's schedule (default: 1)
    
    Returns:
        tuple: (schedule_hour, schedule_minute)
    """
    try:
        group_index = all_group_names.index(group_name)
    except ValueError:
        group_index = 0
    
    total_stagger_minutes = group_index * stagger_minutes
    schedule_hour = base_hour + (total_stagger_minutes // 60)
    schedule_minute = total_stagger_minutes % 60
    
    # Wrap around midnight if needed
    if schedule_hour >= 24:
        schedule_hour = schedule_hour % 24
    
    return schedule_hour, schedule_minute

def _create_jobs_and_schedules(filtered_groups, all_groups):
    """
    Create Dagster jobs and schedules for the filtered groups.
    
    Each group gets:
    - A job that selects all assets in the group
    - A daily schedule with staggered timing
    - Concurrency control via tags
    
    Args:
        filtered_groups: Groups to create jobs for (subset)
        all_groups: All groups (for staggered scheduling calculation)
    
    Returns:
        tuple: (list of JobDefinition, list of ScheduleDefinition)
    """
    if ULTRA_FAST_MODE:
        return [], []
    
    jobs = []
    schedules = []
    
    all_group_names = [g.get('name') for g in all_groups if g.get('name')]
    
    for group in filtered_groups:
        group_name = group.get("name")
        if not group_name:
            continue

        # Create job for this group
        job_name = f"replication_job_{group_name}"
        try:
            job = dg.define_asset_job(
                name=job_name,
                selection=dg.AssetSelection.groups(group_name),
                description=f"Replication job for group {group_name}",
                tags={
                    "dagster/concurrency_key": "replication",
                    "group": group_name,
                    "type": "replication",
                },
            )
            jobs.append(job)
        except Exception as e:
            if not FAST_MODE:
                print(f"⚠️ Failed to create job for {group_name}: {e}")
            continue

        # Create staggered schedule for this job
        schedule_hour, schedule_minute = _calculate_staggered_schedule(
            group_name, all_group_names
        )
        
        cron_schedule = f"{schedule_minute} {schedule_hour} * * *"
        
        try:
            schedule = dg.ScheduleDefinition(
                job_name=job_name,
                cron_schedule=cron_schedule,
                execution_timezone="America/New_York",
                description=f"Daily replication for {group_name} at {schedule_hour:02d}:{schedule_minute:02d} ET",
            )
            schedules.append(schedule)
        except Exception as e:
            if not FAST_MODE:
                print(f"⚠️ Failed to create schedule for {group_name}: {e}")
            continue
    
    return jobs, schedules

# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCE CREATION
# ═══════════════════════════════════════════════════════════════════════════════

def _create_resources(ultra_fast=False):
    """
    Create Dagster resources for database connections.
    
    Args:
        ultra_fast: If True, create minimal test resources with defaults
    
    Returns:
        dict: Dictionary of resource name -> resource instance
    """
    try:
        if ultra_fast:
            # Minimal resources for ultra-fast development mode
            return {
                "postgres": PostgresResource(
                    db_user=os.environ.get("DB_USER", "test"),
                    db_password=os.environ.get("DB_PASSWORD", "test"),
                    db_host=os.environ.get("DB_HOST", "localhost"),
                    db_port=os.environ.get("DB_PORT", "5432"),
                    db_name=os.environ.get("DB_NAME", "test"),
                )
            }
        
        # Full resources for production
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
    except Exception as e:
        print(f"❌ Failed to create resources: {e}")
        import traceback
        traceback.print_exc()
        return {}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DEFINITION BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _build_definitions(target_group=None, group_prefix=None):
    """
    Build Dagster definitions with performance optimization.
    
    Args:
        target_group: Specific group name to load (e.g., "d10")
        group_prefix: Group prefix to load (e.g., "d" for all d* groups)
    
    Returns:
        dg.Definitions: Dagster definitions object with assets, resources, jobs, schedules
    
    Raises:
        ValueError: If groups cannot be loaded or filtered
    """
    try:
        # Load all groups from YAML (cached)
        all_groups = _load_all_yaml_data()
        
        if not all_groups:
            raise ValueError("No groups loaded from YAML file")
        
        # Filter groups based on criteria
        filtered_groups = _filter_groups(all_groups, target_group, group_prefix)
        
        if not FAST_MODE:
            print(f"🎯 Building definitions for {len(filtered_groups)} groups")
        
        # Ultra-fast mode: minimal definitions (no jobs/schedules)
        if ULTRA_FAST_MODE:
            # Limit to 3 groups max in ultra-fast mode
            if len(filtered_groups) > 3:
                filtered_groups = filtered_groups[:3]
                if not FAST_MODE:
                    print(f"⚡ ULTRA_FAST_MODE: Limited to 3 groups")
            
            all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
            resources = _create_resources(ultra_fast=True)
            
            return dg.Definitions(
                assets=all_assets,
                resources=resources,
                jobs=[],
                schedules=[],
            )
        
        # Normal mode: full definitions with jobs and schedules
        all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
        
        if not FAST_MODE:
            print(f"📦 Built {len(all_assets)} assets")
        
        resources = _create_resources(ultra_fast=False)
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups)
        
        if not FAST_MODE:
            print(f"⚙️  Created {len(jobs)} jobs and {len(schedules)} schedules")
        
        return dg.Definitions(
            assets=all_assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
        )
        
    except Exception as e:
        print(f"❌ Error building definitions: {e}")
        import traceback
        traceback.print_exc()
        raise

# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

def get_defs():
    """
    Factory function to get Dagster definitions.
    Uses GROUP_PREFIX environment variable for distributed gRPC servers.
    
    Returns:
        dg.Definitions: Dagster definitions object
    """
    group_prefix = os.environ.get("GROUP_PREFIX")
    
    if group_prefix:
        return _build_definitions(group_prefix=group_prefix)
    else:
        return _build_definitions()

def get_defs_for_group(group_name):
    """
    Get definitions for a specific group.
    
    Args:
        group_name: Name of the group (e.g., "d10")
    
    Returns:
        dg.Definitions: Dagster definitions object
    """
    return _build_definitions(target_group=group_name)

def get_defs_for_prefix(prefix):
    """
    Get definitions for groups with a specific prefix.
    
    Args:
        prefix: Group prefix (e.g., "d")
    
    Returns:
        dg.Definitions: Dagster definitions object
    """
    return _build_definitions(group_prefix=prefix)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

# Check if worker process FIRST (critical for performance)
if _is_worker_process():
    print("⏩ Worker process detected - Skipping asset loading")
    defs = dg.Definitions(
        assets=[],
        resources={},
        jobs=[],
        schedules=[],
    )
else:
    # This is a gRPC server or webserver - load full definitions
    group_prefix = os.environ.get("GROUP_PREFIX")
    
    if group_prefix:
        try:
            defs = get_defs()
            if not FAST_MODE:
                print(f"📍 gRPC Server - Loaded {len(defs.assets)} assets for prefix: {group_prefix}")
        except Exception as e:
            print(f"❌ Failed to create definitions for {group_prefix}: {e}")
            import traceback
            traceback.print_exc()
            defs = dg.Definitions(
                assets=[],
                resources={},
                jobs=[],
                schedules=[],
            )
    else:
        try:
            defs = get_defs()
            if not FAST_MODE:
                print(f"🌍 Local Mode - Loaded {len(defs.assets)} assets (all groups)")
        except Exception as e:
            print(f"⚠️ Local mode failed: {e}")
            import traceback
            traceback.print_exc()
            defs = dg.Definitions(
                assets=[],
                resources={},
                jobs=[],
                schedules=[],
            )