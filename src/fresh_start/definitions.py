import os
import re
import yaml
import dagster as dg
from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource

prefix = 'q'  # Example prefix to filter groups

BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "defs", "replication_mapping_generated.yaml")

print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"

def get_enabled_groups_with_prefix_from_yaml(path, prefix):
    assert isinstance(prefix, str) and len(prefix) == 1, "Prefix must be a single character string"
    pattern = re.compile(rf"^{re.escape(prefix)}", re.IGNORECASE)
    with open(path) as f:
        data = yaml.safe_load(f)
    groups = data.get("groups", [])
    filtered = [g["name"] for g in groups if g.get("enabled") and g.get("name") and pattern.match(g["name"])]
    return filtered

groups_list = get_enabled_groups_with_prefix_from_yaml(yaml_path, prefix)
print(f"Enabled groups starting with '{prefix}': {groups_list}")

# Load assets for these groups
all_assets = build_assets_from_yaml(yaml_path, groups_list)

# Initialize resources — assuming environment variables exist
postgres_resource = PostgresResource(
    db_user=os.environ["DB_USER"],
    db_password=os.environ["DB_PASSWORD"],
    db_host=os.environ["DB_HOST"],
    db_port=os.environ["DB_PORT"],
    db_name=os.environ["DB_NAME"],
)

oracle_resource = OracleResource(
    db_user=os.environ["ORACLE_DB_USER"],
    db_password=os.environ["ORACLE_DB_PASSWORD"],
    db_host=os.environ["ORACLE_DB_HOST"],
    db_port=os.environ["ORACLE_DB_PORT"],
    db_service=os.environ["ORACLE_DB_SERVICE"],
)

db_executor = dg.multiprocess_executor.configured({"max_concurrent": 10})
max_parallel_jobs = 8
concurrency_key_name = "replication_jobs_group"

replication_jobs = []
schedules = []

for group in groups_list:
    job_name = f"replication_job_{group}"

    job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.groups(group),
        executor_def=db_executor,
        description=f"Replicates assets for group: {group}",
    )
    replication_jobs.append(job)

    schedule = dg.ScheduleDefinition(
        job_name=job_name,
        cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
        execution_timezone="America/New_York",
        # concurrency_key=concurrency_key_name,
        # concurrency_limit=max_parallel_jobs,
    )
    schedules.append(schedule)

defs = dg.Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource,
        "oracle": oracle_resource,
    },
    jobs=replication_jobs,
    schedules=schedules,
)