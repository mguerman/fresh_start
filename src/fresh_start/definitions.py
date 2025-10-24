import os
from pathlib import Path
import dagster as dg

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups, yaml_path

# Get enabled group list.
yaml_path_obj = Path(yaml_path)
groups_list = load_enabled_groups(yaml_path_obj, prefix='t')
print(f"Enabled groups: {groups_list}")

# Load assets for all enabled groups combined (all groups together for Dagster assets scope)
all_assets = []
for group in groups_list:
    group_name = group.get("name")
    if group_name is not None:
        group_assets = build_assets_from_yaml(yaml_path_obj, [group_name])
        all_assets.extend(group_assets)

# Initialize database resources from environment variables
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

# Configure multiprocess executor for heavy DB workloads
db_executor = dg.multiprocess_executor.configured({"max_concurrent": 10})

# Prepare jobs and schedules with per-job resource config for the group_resource
replication_jobs = []
schedules = []

for group in groups_list:
    group_name = group.get("name")
    if group_name is None:
        continue

    job_name = f"replication_job_{group_name}"

    job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.groups(group_name),
        executor_def=db_executor,
        description=f"Replicates assets for group: {group_name}",
    )
    replication_jobs.append(job)

    schedule = dg.ScheduleDefinition(
        job_name=job_name,
        cron_schedule="0 2 * * *",
        execution_timezone="America/New_York",
        run_config={
            "resources": {
                "group": {
                    "config": {"group_name": group_name}
                }
            }
        },
    )
    schedules.append(schedule)

# Construct definition with all assets, jobs, and shared resources
# NOTE: here the base resource dict does not set 'group' because group resource is per-job configured on execution
defs = dg.Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource,
        "oracle": oracle_resource,
        # We do NOT set 'group' here at repo-level because it's job-level configured below
    },
    jobs=replication_jobs,
    schedules=schedules,
)