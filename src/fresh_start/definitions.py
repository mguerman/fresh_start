import os
import dagster as dg
from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource, OracleResource

# Load assets from YAML
BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "defs", "replication_ora_pg.yaml")
# yaml_path = os.path.join(BASE_DIR, "defs", "replication_mapping_generated.yaml")
print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"
# groups_list = ["demographics_data", "admissions_data", "financial_aid_data"] # all the available groups for now
groups_list = ["test_data"] # use for test only
# groups_list = ["all"] 
all_assets = build_assets_from_yaml(yaml_path, groups_list)

# Instantiate PostgresResource safely
try:
    postgres_resource = PostgresResource(
        db_user=os.environ["DB_USER"],
        db_password=os.environ["DB_PASSWORD"],
        db_host=os.environ["DB_HOST"],
        db_port=os.environ["DB_PORT"],
        db_name=os.environ["DB_NAME"],
    )
except KeyError as e:
    raise RuntimeError(f"Missing required environment variable: {e}")

# Instantiate OracleResource safely
try:
    oracle_resource = OracleResource(
        db_user=os.environ["ORACLE_DB_USER"],
        db_password=os.environ["ORACLE_DB_PASSWORD"],
        db_host=os.environ["ORACLE_DB_HOST"],
        db_port=os.environ["ORACLE_DB_PORT"],
        db_service=os.environ["ORACLE_DB_SERVICE"],
    )
except KeyError as e:
    raise RuntimeError(f"Missing required environment variable: {e}")

# Configure executor for heavy database workloads
db_executor = dg.multiprocess_executor.configured({
    "max_concurrent": 10
})

# Old - one replication job for all the assets
"""# Define a job that uses the executor
replication_job = dg.define_asset_job(
    name="replication_job",
    selection="*",
    executor_def=db_executor,
    description="Replicates all assets with controlled concurrency"
)"""


# New - Create a job for each group in groups_list
replication_jobs = []
for group in groups_list:
    job = dg.define_asset_job(
        name=f"replication_job_{group}",
        selection=dg.AssetSelection.groups(group),
        executor_def=db_executor,
        description=f"Replicates assets for group: {group}"
    )
    replication_jobs.append(job)

# Old job definition block
"""# Register with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource,
        "oracle": oracle_resource
    },
    jobs=[replication_job]
)"""

# New - Register job per group with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource,
        "oracle": oracle_resource
    },
    jobs=replication_jobs
)
