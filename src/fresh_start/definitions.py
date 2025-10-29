from pathlib import Path
import os
import dagster as dg
import threading
import pickle
import hashlib
import logging
from datetime import datetime

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

_yaml_cache = None
_cache_lock = threading.Lock()

BASE_DIR = Path(__file__).parent
YAML_PATH = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
LOGS_DIR = BASE_DIR.parent.parent / "logs"
FAST_MODE = os.environ.get("DAGSTER_FAST_MODE", "false").lower() == "true"

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_location_logging(target_group=None, group_prefix=None):
    """Minimal logging setup."""
    logger = logging.getLogger(f"dagster.minimal.{os.getpid()}")
    logger.setLevel(logging.ERROR)  # Only errors
    return logger

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
    
    LOGS_DIR.mkdir(exist_ok=True)
    cache_key = _get_cache_key()
    cache_file = LOGS_DIR / f"yaml_cache_{cache_key}.pkl"

    # Check in-memory cache first (fastest)
    if _yaml_cache is not None:
        return _yaml_cache
    
    with _cache_lock:
        # Double-check pattern inside lock
        if _yaml_cache is not None:
            return _yaml_cache
        
        # Try to load from file cache
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

def _validate_and_filter_groups(all_groups, target_group=None, group_prefix=None, logger=None):
    """Validate inputs and filter groups based on criteria."""
    if not all_groups:
        raise ValueError("No groups loaded from YAML file")
    
    if target_group:
        # Filter to specific group
        filtered = [g for g in all_groups if g.get('name') == target_group]
        
        if not filtered:
            available = [g.get('name') for g in all_groups[:10]]
            error_msg = f"Group '{target_group}' not found. Available: {available}"
            if logger:
                logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        if logger:
            logger.info(f"🎯 Targeting group: {target_group}")
        return filtered
    
    elif group_prefix:
        # Filter by prefix
        filtered = [g for g in all_groups if g.get('name', '').startswith(group_prefix)]
        
        if not filtered:
            available_prefixes = sorted(set(
                g.get('name', '')[:3] for g in all_groups if g.get('name')
            ))
            error_msg = f"No groups with prefix '{group_prefix}'. Available: {available_prefixes}"
            if logger:
                logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        if logger:
            group_names = [g.get('name') for g in filtered]
            logger.info(f"🔎 Prefix '{group_prefix}': {len(filtered)} groups")
            logger.info(f"   Groups: {group_names}")
        return filtered
    
    else:
        # Return all groups
        if logger:
            logger.info(f"🌍 Loading all {len(all_groups)} groups")
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

def _create_jobs_and_schedules(filtered_groups, all_groups, logger=None):
    """Create jobs and schedules for the filtered groups."""
    jobs = []
    schedules = []
    
    # Get all group names for staggered scheduling
    all_group_names = [g.get('name') for g in all_groups if g.get('name')]
    
    for group in filtered_groups:
        group_name = group.get("name")
        if not group_name:
            if logger:
                logger.warning(f"⚠️ Skipping group with no name")
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
        except Exception as e:
            if logger:
                logger.error(f"❌ Failed to create job for {group_name}: {e}")
            continue

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
            
            if logger:
                logger.info(f"📅 {group_name}: {schedule_hour:02d}:{schedule_minute:02d}")
        except Exception as e:
            if logger:
                logger.error(f"❌ Failed to create schedule for {group_name}: {e}")
    
    return jobs, schedules

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN DEFINITION BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _get_definitions_for_location(target_group=None, group_prefix=None):
    """Build Dagster definitions with minimal logging."""

    if FAST_MODE:
        # Skip all logging and validation for maximum speed
        all_groups = _load_all_yaml_data()
        filtered_groups = _validate_and_filter_groups(all_groups, target_group, group_prefix)
        all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
        resources = _create_shared_resources()
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups)
        
        return dg.Definitions(
            assets=all_assets,
            resources=resources, 
            jobs=jobs,
            schedules=schedules,
        )
    
    logger = _setup_location_logging(target_group, group_prefix)
    
    try:
        # Load all groups from YAML (cached)
        all_groups = _load_all_yaml_data()
        
        # Validate and filter groups
        filtered_groups = _validate_and_filter_groups(
            all_groups, target_group, group_prefix, None  # No logger
        )
        
        # Build assets
        all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
        
        # Create resources
        resources = _create_shared_resources()
        
        # Create jobs and schedules
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups, None)
        
        # Build final definitions
        definitions = dg.Definitions(
            assets=all_assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
        )
        
        # Single minimal output
        print(f"✅ {len(all_assets)} assets loaded")
        
        return definitions
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        raise
    
# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_defs():
    """
    Factory function for workspace.yaml to get Dagster definitions.
    
    Supports both LOCATION_GROUP (new) and GROUP_PREFIX (legacy) environment variables.
    """
    # New approach: target specific group
    location_group = os.environ.get("LOCATION_GROUP")
    if location_group:
        return _get_definitions_for_location(target_group=location_group)
    
    # Legacy approach: filter by prefix
    group_prefix = os.environ.get("GROUP_PREFIX")
    if group_prefix:
        return _get_definitions_for_location(group_prefix=group_prefix)
    
    # Fallback: load all groups
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
# LEGACY COMPATIBILITY
# ═══════════════════════════════════════════════════════════════════════════════

def _get_definitions():
    """Legacy function - redirects to new approach."""
    group_prefix = os.environ.get("GROUP_PREFIX")
    return _get_definitions_for_location(group_prefix=group_prefix)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL DEFINITIONS (For gRPC servers)
# ═══════════════════════════════════════════════════════════════════════════════

# Only create module-level definitions if not in location-specific mode
group_prefix = os.environ.get("GROUP_PREFIX")
location_group = os.environ.get("LOCATION_GROUP")

if group_prefix or location_group:
    # gRPC server mode - create filtered definitions
    try:
        defs = get_defs()
        print(f"📍 gRPC Server Mode - Loaded {len(defs.assets)} assets for prefix/group: {group_prefix or location_group}")
    except Exception as e:
        print(f"❌ Failed to create definitions for {group_prefix or location_group}: {e}")
        # Create empty definitions to prevent server crash
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])
elif not os.environ.get("SKIP_MODULE_DEFS"):
    # Local development mode - load all
    try:
        defs = get_defs()
        print(f"🌍 Local Mode - Loaded {len(defs.assets)} assets (all groups)")
    except Exception:
        # If module-level definition fails, create empty definitions
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])
else:
    # Skip module definitions entirely
    defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])