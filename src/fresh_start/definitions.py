import os
import dagster as dg
from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource

# Load assets from YAML
BASE_DIR = os.path.dirname(__file__)
# yaml_path = os.path.join(BASE_DIR, "defs", "replication_raw_to_stage.yaml")
yaml_path = os.path.join(BASE_DIR, "defs", "replication_mapping_generated.yaml")
print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"
# groups_list = ["demographics_data", "admissions_data", "financial_aid_data"] # all the available groups for now
# groups_list = ["test_data"] # use for test only
groups_list = ["batch5"] 
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

# Configure executor for heavy database workloads
# Start conservative with 8 concurrent processes
db_executor = dg.multiprocess_executor.configured({
    "max_concurrent": 10  # for heavy DB operations
})

# Define a job that uses the executor
replication_job = dg.define_asset_job(
    name="replication_job",
    selection="*",
    executor_def=db_executor,
    description="Replicates all assets with controlled concurrency"
)

# Register with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources={"postgres": postgres_resource},
    jobs=[replication_job]
)
