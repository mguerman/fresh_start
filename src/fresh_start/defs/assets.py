import yaml
from typing import List, Dict, Optional, Union
from pathlib import Path
from dagster import AssetKey, AssetsDefinition, AssetIn

from .asset_creators import (
    create_asset_postgres_to_postgres,
    create_asset_oracle_to_postgres,
)

# ────────────────────────────────────────────────────────────────────────────────
# Load YAML configuration and filter by group names
# ────────────────────────────────────────────────────────────────────────────────
# requires newer Python > 3.10
# def load_yaml_groups(yaml_path: str | Path, groups_list: List[str]) -> List[Dict]:
def load_yaml_groups(yaml_path: Union[str, Path], groups_list: List[str]) -> List[Dict]:
    """
    Load YAML file and filter for specified group names.

    Args:
        yaml_path: Path to the YAML config file.
        groups_list: List of group names to include.

    Returns:
        List of group dictionaries matching the specified names.
    """
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)
    return [group for group in data.get("groups", []) if group.get("name") in groups_list]


# ────────────────────────────────────────────────────────────────────────────────
# Create a Dagster asset for a given stage: source, transform, or target
# ────────────────────────────────────────────────────────────────────────────────
def create_asset(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None,
    ) -> AssetsDefinition:
    source_db = table.get("source_db", "postgres").lower()
    target_db = table.get("target_db", "postgres").lower()

    if source_db == "postgres" and target_db == "postgres":
        return create_asset_postgres_to_postgres(
            group, table, stage, skip_existing_table=skip_existing_table, upstream_key=upstream_key
        )
    elif source_db == "oracle" and target_db == "postgres":
        return create_asset_oracle_to_postgres(
            group, table, stage, skip_existing_table=skip_existing_table, upstream_key=upstream_key
        )
    else:
        raise ValueError(
            f"Unsupported source-target database combination: '{source_db}' -> '{target_db}'. "
            "Supported: oracle->postgres, postgres->postgres."
        )

# ────────────────────────────────────────────────────────────────────────────────
# Build Dagster assets from YAML config
# ────────────────────────────────────────────────────────────────────────────────
# not supported in Python < 3.10
# def build_assets_from_yaml(yaml_path: str | Path, groups_list: List[str]) -> List[AssetsDefinition]:
def build_assets_from_yaml(yaml_path: Union[str, Path], groups_list: List[str]) -> List[AssetsDefinition]:
    """
    Build Dagster assets from YAML config, wiring dependencies:
       source → transform (optional) → target.

    Args:
        yaml_path: Path to the YAML asset config file.
        groups_list: List of group names to include.

    Returns:
        List of Dagster AssetsDefinition objects.
    """
    selected_groups = load_yaml_groups(yaml_path, groups_list)
    assets: List[AssetsDefinition] = []

    for group in selected_groups:
        group_name = group["name"]
        for table in group.get("tables", []):
            if not table.get("enabled", False):
                continue

            # Create source asset
            source_asset = create_asset(group_name, table, "source")
            assets.append(source_asset)
            source_key = AssetKey([group_name, table["table"], "source"])

            # Create transform asset if enabled
            if table.get("transformation", {}).get("enabled", False):
                transform_asset = create_asset(group_name, table, "transform", upstream_key=source_key)
                assets.append(transform_asset)
                upstream_key = AssetKey([group_name, table["table"], "transform"])
            else:
                upstream_key = source_key

            # Create target asset depending on upstream asset
            target_asset_obj = create_asset(group_name, table, "target", upstream_key=upstream_key)
            assets.append(target_asset_obj)

    return assets    