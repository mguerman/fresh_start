from pathlib import Path
import os
import dagster as dg
import threading
import pickle
import hashlib

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

import logging
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════════
# SIMPLE CACHING - No excessive logging
# ═══════════════════════════════════════════════════════════════════════════════

_yaml_cache = None
_lock = threading.Lock()

def _get_cache_key():
    """Generate cache key based on YAML file content."""
    BASE_DIR = Path(__file__).parent
    yaml_path = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
    
    if yaml_path.exists():
        with open(yaml_path, 'rb') as f:
            content = f.read()
        return hashlib.md5(content).hexdigest()[:8]
    return "no_yaml"

def _load_yaml_data():
    """Load and cache YAML data (not Dagster objects)."""
    global _yaml_cache
    
    BASE_DIR = Path(__file__).parent
    logs_dir = BASE_DIR.parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    cache_key = _get_cache_key()
    cache_file = logs_dir / f"yaml_cache_{cache_key}.pkl"
    
    # Check in-memory cache
    if _yaml_cache is not None:
        print(f"📦 Using cached YAML - PID: {os.getpid()}")
        return _yaml_cache
    
    with _lock:
        # Double-check in-memory cache
        if _yaml_cache is not None:
            return _yaml_cache
        
        # Check file cache
        if cache_file.exists():
            try:
                print(f"📁 Loading from cache - PID: {os.getpid()}")
                with open(cache_file, 'rb') as f:
                    _yaml_cache = pickle.load(f)
                print(f"✅ Loaded {len(_yaml_cache)} groups from cache")
                return _yaml_cache
            except Exception as e:
                print(f"⚠️ Cache failed: {e}, rebuilding...")
        
        # Read YAML (ONLY ONCE across all processes)
        print(f"📖 Reading YAML (SINGLE TIME) - PID: {os.getpid()}")
        
        # Clean up old cache files
        for old_cache in logs_dir.glob("yaml_cache_*.pkl"):
            if old_cache != cache_file:
                try:
                    old_cache.unlink()
                except:
                    pass
        
        yaml_path_obj = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
        groups_list = load_enabled_groups(yaml_path_obj, prefix='g')
        print(f"✅ Loaded {len(groups_list)} groups: {[g['name'] for g in groups_list]}")
        
        # Cache the data
        _yaml_cache = groups_list
        
        # Save to file
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(_yaml_cache, f)
            print(f"💾 Cached to: {cache_file}")
        except Exception as e:
            print(f"⚠️ Cache save failed: {e}")
        
        return _yaml_cache

def _get_definitions():
    """Build Dagster definitions using cached YAML data."""
    
    # Setup single log file (only once, shared across processes)
    BASE_DIR = Path(__file__).parent
    logs_dir = BASE_DIR.parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Single log file name (no timestamp = same file for all processes)
    log_file = logs_dir / "dagster_definitions.log"
    
    # Configure logging only if not already done
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),        # File output
                logging.StreamHandler()               # Terminal output (keep both)
            ]
        )
    
    logger = logging.getLogger(__name__)
    logger.info(f"🔄 Building definitions - PID: {os.getpid()}")
    
    # Load YAML data (cached)
    groups_list = _load_yaml_data()
    
    # Build assets
    logger.info("🏗️ Building assets...")
    BASE_DIR = Path(__file__).parent
    yaml_path_obj = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
    all_assets = build_assets_from_yaml(str(yaml_path_obj), groups_list)
    logger.info(f"✅ Built {len(all_assets)} assets")
    
    # Resources (keep existing code)
    resources = {
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
    
    # Jobs (keep existing code)
    jobs = []
    schedules = []
    
    for group in groups_list:
        group_name = group.get("name")
        if not group_name:
            continue

        job_name = f"replication_job_{group_name}"
        
        job = dg.define_asset_job(
            name=job_name,
            selection=dg.AssetSelection.groups(group_name),
            executor_def=None,
            description=f"Sequential replication for group {group_name}",
        )
        jobs.append(job)

        schedule = dg.ScheduleDefinition(
            job_name=job_name,
            cron_schedule="0 2 * * *",
            execution_timezone="America/New_York",
            description=f"Daily replication for {group_name}",
        )
        schedules.append(schedule)
    
    logger.info(f"✅ Complete: {len(all_assets)} assets, {len(jobs)} jobs")
    
    return dg.Definitions(
        assets=all_assets,
        resources=resources,
        jobs=jobs,
        schedules=schedules,
    )

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

print(f"📦 Importing definitions - PID: {os.getpid()}")
defs = _get_definitions()
print("📦 Definitions ready")