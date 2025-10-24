import os
import dagster as dg

from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource
from .defs.util import load_enabled_groups, yaml_path

# Get enabled group list.
groups_list = load_enabled_groups(yaml_path, prefix='h')  # or prefix='h'
print(f"Enabled groups: {groups_list}")

# Load assets for all enabled groups combined (all groups together for Dagster assets scope)
all_assets = []
for group in groups_list:
    group_name = group.get("name")
    group_assets = build_assets_from_yaml(yaml_path, [group_name])  # Only load the one group
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
    job_name = f"replication_job_{group}"

    # Define job selecting assets of this group
    job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.groups(group),
        executor_def=db_executor,
        description=f"Replicates assets for group: {group}",
    )
    replication_jobs.append(job)

    # Create schedule per job (adjust timing as needed)
    schedule = dg.ScheduleDefinition(
    job_name=job_name,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="America/New_York",
    run_config={
        "resources": {
            "group": {
                "config": {"group_name": group}
            },
            # Optionally add other resource configs here, or rely on env vars
        }
    },
    # concurrency_key=..., concurrency_limit=... # Uncomment if supported and needed
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