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

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_location_logging(target_group=None, group_prefix=None):
    """Setup location-specific logging with proper handlers."""
    LOGS_DIR.mkdir(exist_ok=True)

    # Determine log identifier
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if target_group:
        log_id = f"group_{target_group}"
    elif group_prefix:
        log_id = f"prefix_{group_prefix}"
    else:
        log_id = "all"
    
    log_file = LOGS_DIR / f"dagster_location_{log_id}_{timestamp}.log"
    
    # Create logger with unique name to avoid conflicts
    logger_name = f"dagster.location.{log_id}.{os.getpid()}"
    logger = logging.getLogger(logger_name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.propagate = False  # Prevent duplicate messages
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [PID:%(process)d] %(levelname)s - %(message)s"
        ))
        logger.addHandler(file_handler)
        
        # Console handler for gRPC servers
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))
        logger.addHandler(console_handler)
    
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
    """
    Build Dagster definitions for a specific group or filtered by prefix.
    
    Args:
        target_group: Target a specific group by name
        group_prefix: Target groups by prefix (legacy support)
    
    Returns:
        dg.Definitions: Complete Dagster definitions
    
    Raises:
        ValueError: If invalid parameters or no groups found
    """
    # Setup logging
    logger = _setup_location_logging(target_group, group_prefix)
    logger.info(f"🔄 Building definitions - PID: {os.getpid()}")
    logger.info(f"   target_group={target_group}, group_prefix={group_prefix}")
    
    try:
        # Load all groups from YAML (cached)
        logger.info("📖 Loading YAML data...")
        all_groups = _load_all_yaml_data()
        logger.info(f"✅ Loaded {len(all_groups)} total groups")
        
        # Validate and filter groups
        logger.info("🔍 Filtering groups...")
        filtered_groups = _validate_and_filter_groups(
            all_groups, target_group, group_prefix, logger
        )
        
        # Build assets
        logger.info("🏗️ Building assets...")
        all_assets = build_assets_from_yaml(str(YAML_PATH), filtered_groups)
        logger.info(f"✅ Built {len(all_assets)} assets")
        
        # Create resources
        logger.info("🔧 Creating resources...")
        resources = _create_shared_resources()
        logger.info(f"✅ Created {len(resources)} resources")
        
        # Create jobs and schedules
        logger.info("📋 Creating jobs and schedules...")
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups, logger)
        logger.info(f"✅ Created {len(jobs)} jobs, {len(schedules)} schedules")
        
        # Build final definitions
        definitions = dg.Definitions(
            assets=all_assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
        )
        
        # Success summary
        group_names = [g.get('name') for g in filtered_groups]
        logger.info(f"🎉 Definitions complete!")
        logger.info(f"   Assets: {len(all_assets)}")
        logger.info(f"   Jobs: {len(jobs)}")
        logger.info(f"   Schedules: {len(schedules)}")
        logger.info(f"   Groups: {group_names}")
        
        return definitions
        
    except Exception as e:
        logger.error(f"❌ Failed to build definitions: {e}")
        
        # For gRPC servers, fail fast
        if target_group or group_prefix:
            logger.error("💥 gRPC server will not start due to definition failure")
            raise
        
        # For legacy compatibility, return empty definitions
        logger.warning("⚠️ Returning empty definitions for legacy compatibility")
        return dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])

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
# MODULE-LEVEL DEFINITIONS (Optional)
# ═══════════════════════════════════════════════════════════════════════════════

# Only create module-level definitions if not in location-specific mode
if not os.environ.get("LOCATION_GROUP") and not os.environ.get("SKIP_MODULE_DEFS"):
    try:
        defs = get_defs()
    except Exception:
        # If module-level definition fails, create empty definitions
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])