"""
Dagster definitions module for batched asset processing.

- Loads asset batches from YAML via assets.py
- Creates resource definitions for Oracle and Postgres
- Creates jobs that select assets by group with controlled concurrency and optional scheduling
- Supports filtering by target group or prefix
- Uses multiprocess executor with limited concurrency to avoid CPU overload
"""

import os
import threading
import pickle
import hashlib
from pathlib import Path
import traceback

import dagster as dg
from dagster import multiprocess_executor

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

# ──────────────── CONFIGURATION ────────────────────────

BASE_DIR = Path(__file__).parent
YAML_PATH = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
LOGS_DIR = BASE_DIR.parent.parent / "logs"

# Environment-driven flags for fallback modes
FAST_MODE = os.environ.get("DAGSTER_FAST_MODE", "false").lower() == "true"
ULTRA_FAST_MODE = os.environ.get("DAGSTER_ULTRA_FAST", "false").lower() == "true"

# Cache variables and lock for thread-safe YAML caching
_yaml_cache = None
_cache_lock = threading.Lock()

# ──────────────── YAML CACHING ────────────────────────

def _get_cache_key():
    """Produce a cache key from YAML contents hash."""
    try:
        if YAML_PATH.exists():
            content = YAML_PATH.read_bytes()
            return hashlib.md5(content).hexdigest()[:8]
    except Exception as e:
        if not FAST_MODE:
            print(f"⚠️ Cache key gen failed: {e}")
    return "no_yaml"

def _cleanup_old_cache_files(current_cache_file: Path):
    """Remove old pickle caches to avoid disk bloat."""
    try:
        count = 0
        for old_cache in LOGS_DIR.glob("yaml_cache_*.pkl"):
            if old_cache != current_cache_file:
                old_cache.unlink()
                count += 1
        if count > 0 and not FAST_MODE:
            print(f"🧹 Removed {count} old cache files")
    except Exception as e:
        if not FAST_MODE:
            print(f"⚠️ Cache cleanup failed: {e}")

def _load_all_yaml_data():
    """
    Load YAML all group configs with thread-safe caching + pickle cache use.
    This prevents repeated heavy YAML loads on workers.
    """
    global _yaml_cache
    if _yaml_cache is not None:
        return _yaml_cache

    with _cache_lock:
        if _yaml_cache is not None:
            return _yaml_cache

        try:
            LOGS_DIR.mkdir(exist_ok=True)

            cache_key = _get_cache_key()
            cache_file = LOGS_DIR / f"yaml_cache_{cache_key}.pkl"

            if cache_file.exists():
                try:
                    with open(cache_file, "rb") as f:
                        _yaml_cache = pickle.load(f)
                    if not FAST_MODE:
                        print(f"💾 Loaded {_yaml_cache and len(_yaml_cache)} groups from pickle cache")
                    return _yaml_cache
                except Exception as e:
                    if not FAST_MODE:
                        print(f"⚠️ Pickle cache corrupted; reloading YAML: {e}")

            if not FAST_MODE:
                print(f"📄 Loading groups from YAML: {YAML_PATH}")

            all_groups = load_enabled_groups(YAML_PATH)

            if not FAST_MODE:
                print(f"✅ Loaded {len(all_groups)} groups from YAML")

            try:
                with open(cache_file, "wb") as f:
                    pickle.dump(all_groups, f)
                _cleanup_old_cache_files(cache_file)
                if not FAST_MODE:
                    print(f"💾 Saved pickle cache: {cache_file.name}")
            except Exception as e:
                if not FAST_MODE:
                    print(f"⚠️ Saving pickle cache failed: {e}")

            _yaml_cache = all_groups
            return _yaml_cache

        except Exception as e:
            print(f"❌ Failed to load YAML data: {e}")
            traceback.print_exc()
            _yaml_cache = []
            return _yaml_cache


# ──────────────── GROUP FILTERING ────────────────────────

def _filter_groups(all_groups, target_group=None, group_prefix=None):
    """
    Filter groups by exact name or prefix.

    Raises ValueError for invalid filters.
    """
    if not all_groups:
        return []

    if target_group:
        filtered = [g for g in all_groups if g.get("name") == target_group]
        if not filtered:
            available = [g.get("name") for g in all_groups[:10]]
            raise ValueError(
                f"Group '{target_group}' not found. Available (first 10): {available}"
            )
        return filtered

    if group_prefix:
        filtered = [g for g in all_groups if g.get("name", "").startswith(group_prefix)]
        if not filtered:
            prefixes = sorted(set(g.get("name", "")[:3] for g in all_groups if g.get("name")))
            raise ValueError(
                f"No groups with prefix '{group_prefix}'. Available prefixes: {prefixes}"
            )
        return filtered

    return all_groups


# ──────────────── JOB & SCHEDULE CREATION ────────────────────────

def _calculate_staggered_schedule(group_name, all_group_names, base_hour=2, stagger_minutes=1):
    """
    Compute daily schedule time staggered by group index to avoid thundering herd.

    Returns (hour, minute)
    """
    try:
        idx = all_group_names.index(group_name)
    except ValueError:
        idx = 0

    total_minutes = idx * stagger_minutes
    hour = base_hour + total_minutes // 60
    minute = total_minutes % 60
    hour = hour % 24
    return hour, minute


def _create_jobs_and_schedules(filtered_groups, all_groups):
    """
    Create Dagster jobs for each group and schedules with staggered timing.

    Uses multiprocess executor limited to 4 concurrent workers
    to keep CPU usage moderate.
    """
    if ULTRA_FAST_MODE:
        return [], []

    jobs = []
    schedules = []

    group_names = [g["name"] for g in all_groups if g.get("name")]

    for group in filtered_groups:
        group_name = group.get("name")
        if not group_name:
            continue

        job_name = f"replication_job_{group_name}"

        try:
            job = dg.define_asset_job(
                name=job_name,
                selection=dg.AssetSelection.groups(group_name),
                description=f"Replication job for group {group_name} (batched processing)",
                tags={
                    "dagster/concurrency_key": "replication",
                    "group": group_name,
                    "type": "replication",
                },
                executor_def=multiprocess_executor.configured(
                    {"max_concurrent": 4}
                ),
            )
            jobs.append(job)

        except Exception as e:
            if not FAST_MODE:
                print(f"⚠️ Failed to create job for {group_name}: {e}")
            continue

        # Schedule staggered daily runs starting at 2am ET, 1 min apart for each group
        sched_hr, sched_min = _calculate_staggered_schedule(group_name, group_names)

        cron = f"{sched_min} {sched_hr} * * *"
        try:
            schedule = dg.ScheduleDefinition(
                job_name=job_name,
                cron_schedule=cron,
                execution_timezone="America/New_York",
                description=f"Daily replication for {group_name} at {sched_hr:02d}:{sched_min:02d} ET",
            )
            schedules.append(schedule)
        except Exception as e:
            if not FAST_MODE:
                print(f"⚠️ Failed to create schedule for {group_name}: {e}")
            continue

    return jobs, schedules


# ──────────────── RESOURCE CREATION ────────────────────────

def _create_resources(ultra_fast=False):
    """
    Create Dagster resources for Oracle and Postgres.

    ultra_fast mode creates mocked or minimal resources.
    """
    try:
        if ultra_fast:
            # Minimal dummy Postgres resource for ultra_fast
            return {
                "postgres": PostgresResource(
                    db_user=os.environ.get("DB_USER", "test"),
                    db_password=os.environ.get("DB_PASSWORD", "test"),
                    db_host=os.environ.get("DB_HOST", "localhost"),
                    db_port=os.environ.get("DB_PORT", "5432"),
                    db_name=os.environ.get("DB_NAME", "test"),
                )
            }

        # Normal full resources with env vars
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
        traceback.print_exc()
        return {}

# ──────────────── MAIN DEFINITIONS BUILDER ────────────────────────

def _build_definitions(target_group=None, group_prefix=None):
    """Build Dagster definitions with batched asset processing from YAML config."""
    try:
        all_groups = _load_all_yaml_data()
        if not all_groups:
            raise ValueError("No groups loaded from YAML")

        filtered_groups = _filter_groups(all_groups, target_group, group_prefix)
        if not FAST_MODE:
            print(f"🎯 Building definitions for {len(filtered_groups)} groups")

        filtered_group_names = [g.get("name") for g in filtered_groups if g.get("name")]
        if not FAST_MODE:
            print(f"🔧 Using groups: {filtered_group_names}")

        # Ultra fast mode uses minimal definitions; no jobs or schedules
        if ULTRA_FAST_MODE:
            if len(filtered_group_names) > 3:
                filtered_group_names = filtered_group_names[:3]
                if not FAST_MODE:
                    print(f"⚡ ULTRA_FAST_MODE limited to 3 groups")

            assets = build_assets_from_yaml(str(YAML_PATH), filtered_group_names)
            resources = _create_resources(ultra_fast=True)

            return dg.Definitions(
                assets=assets,
                resources=resources,
                jobs=[],
                schedules=[],
            )

        # Normal full mode
        assets = build_assets_from_yaml(str(YAML_PATH), filtered_group_names)
        if not FAST_MODE:
            print(f"📦 Built {len(assets)} assets")

        resources = _create_resources(ultra_fast=False)
        jobs, schedules = _create_jobs_and_schedules(filtered_groups, all_groups)

        if not FAST_MODE:
            print(f"⚙️ Created {len(jobs)} jobs and {len(schedules)} schedules")

        return dg.Definitions(
            assets=assets,
            resources=resources,
            jobs=jobs,
            schedules=schedules,
        )

    except Exception as e:
        print(f"❌ Error building definitions: {e}")
        traceback.print_exc()
        raise


# ──────────────── PUBLIC API FUNCTIONS ────────────────────────

def get_defs():
    """Get Dagster definitions, filtering by GROUP_PREFIX env var if set."""
    group_prefix = os.environ.get("GROUP_PREFIX")
    if group_prefix:
        return _build_definitions(group_prefix=group_prefix)
    else:
        return _build_definitions()

def get_defs_for_group(group_name):
    """Get Dagster definitions for a specific group by name."""
    return _build_definitions(target_group=group_name)

def get_defs_for_prefix(prefix):
    """Get Dagster definitions for groups with a given prefix."""
    return _build_definitions(group_prefix=prefix)


# ──────────────── MODULE LEVEL INIT ────────────────────────

def _is_worker_process():
    """Detect if we are in a worker subprocess, for logging and cache use."""
    import os
    try:
        if os.environ.get("DAGSTER_IS_CAPTURING_LOGS"):
            return True
        import multiprocessing
        if multiprocessing.current_process().name != "MainProcess":
            return True
    except Exception:
        pass
    return False

if _is_worker_process():
    print("⏩ Worker process detected - loading cached definitions")
    try:
        defs = get_defs()
        print(f"✅ Worker process loaded {len(defs.assets)} assets")
    except Exception as e:
        print(f"❌ Worker failed to load definitions: {e}")
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])
else:
    group_prefix = os.environ.get("GROUP_PREFIX")
    try:
        defs = get_defs()
        if not FAST_MODE:
            if group_prefix:
                print(f"📍 Loaded {len(defs.assets)} assets (prefix: {group_prefix})")
            else:
                print(f"🌍 Loaded {len(defs.assets)} assets (all groups)")
    except Exception as e:
        print(f"⚠️ Failed to create definitions: {e}")
        defs = dg.Definitions(assets=[], resources={}, jobs=[], schedules=[])