import os
import dagster as dg
from .defs.assets import build_assets_from_yaml
from .defs.resources import PostgresResource

# Load assets from YAML
BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "defs", "replication_raw_to_stage.yaml")
print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"

# groups_list = ["demographics_data", "admissions_data", "financial_aid_data"] # all the available groups for now
groups_list = ["test_data"] # use for test only
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
# Start conservative with 4 concurrent processes
db_executor = dg.multiprocess_executor.configured({
    "max_concurrent": 4  # Conservative for heavy DB operations
})

# Define job with controlled concurrency
replication_job = dg.define_asset_job(
    name="raw_to_stage_replication",
    selection="*",  # Selects all assets
    executor_def=db_executor,
    description="Replicates data from raw to stage with controlled concurrency"
)

# Alternative: Define separate jobs per group for better control
test_job = dg.define_asset_job(
    name="test_replication",
    selection=dg.AssetSelection.groups("test_data"),
    executor_def=db_executor,
    description="Test data replication job"
)

# For production, you can define group-specific jobs with different concurrency
# demographics_job = dg.define_asset_job(
#     name="demographics_replication",
#     selection=dg.AssetSelection.groups("demographics_data"),
#     executor_def=dg.multiprocess_executor.configured({"max_concurrent": 6}),
# )
#
# admissions_job = dg.define_asset_job(
#     name="admissions_replication",
#     selection=dg.AssetSelection.groups("admissions_data"),
#     executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
# )
#
# financial_aid_job = dg.define_asset_job(
#     name="financial_aid_replication",
#     selection=dg.AssetSelection.groups("financial_aid_data"),
#     executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
# )

# Register with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources={"postgres": postgres_resource},
    jobs=[
        replication_job,  # Main job with all assets
        test_job,  # Test-specific job
        # Uncomment for production:
        # demographics_job,
        # admissions_job,
        # financial_aid_job,
    ]
)
