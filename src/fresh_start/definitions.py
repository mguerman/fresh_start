import os
from dagster import Definitions
from .defs.assets import build_assets_from_yaml

BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "defs", "replication_raw_to_stage.yaml")
print(f"Loading YAML from: {yaml_path}")
assert os.path.isfile(yaml_path), f"YAML file not found at {yaml_path}"

groups_list = ["test_data"]

all_assets = build_assets_from_yaml(yaml_path, groups_list)

defs = Definitions(
    assets=all_assets
)