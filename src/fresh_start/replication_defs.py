from pathlib import Path
import os
import logging
import dagster as dg
from datetime import datetime

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups

def get_replication_definitions():
    """This function is only called once when Dagster loads the code location."""
    
    # Setup logging
    BASE_DIR = Path(__file__).parent
    logs_dir = BASE_DIR.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = logs_dir / f"dagster_definitions_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ],
        force=True
    )
    
    logger = logging.getLogger(__name__)
    
    logger.info(f"🔄 Loading definitions - PID: {os.getpid()}")
    logger.info(f"📝 Log file: {log_filename}")
    
    yaml_path_obj = BASE_DIR / "defs" / "replication_mapping_generated.yaml"
    
    # Heavy operations happen here - but only once
    logger.info("📖 Reading YAML file...")
    groups_list = load_enabled_groups(yaml_path_obj, prefix='g')
    logger.info(f"✅ Loaded {len(groups_list)} groups: {[g['name'] for g in groups_list]}")
    
    logger.info("🏗️ Building assets...")
    all_assets = build_assets_from_yaml(str(yaml_path_obj), groups_list)
    logger.info(f"✅ Built {len(all_assets)} assets")
    
    # Resources
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
    
    # Jobs
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
    
    logger.info(f"✅ All loaded: {len(all_assets)} assets, {len(jobs)} jobs")
    
    return dg.Definitions(
        assets=all_assets,
        resources=resources,
        jobs=jobs,
        schedules=schedules,
    )