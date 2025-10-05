import yaml
from pathlib import Path
from typing import List, Dict, Optional
from dagster import asset, AssetKey, AssetsDefinition


def load_yaml_groups(yaml_path: str | Path, groups_list: List[str]) -> List[Dict]:
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


def create_asset(
    group: str,
    table: Dict,
    stage: str,
    upstream_key: AssetKey | None = None
) -> AssetsDefinition:
    """
    Dynamically create a Dagster asset for a given stage: source, transform, or target.

    Args:
        group: Name of the asset group.
        table: Dictionary containing table configuration from YAML.
        stage: One of "source", "transform", or "target".
        upstream_key: Optional AssetKey for dependency (used for transform and target).

    Returns:
        Dagster AssetsDefinition object for the specified stage.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"
    deps = [upstream_key] if upstream_key else []

    # Source asset: no dependencies
    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract data from {source_schema}.{table_name}",
        )
        def source_asset() -> str:
            return f"Extracted data from {source_schema}.{table_name}"

        source_asset.__name__ = func_name
        return source_asset

    # Transform asset: depends on source
    elif stage == "transform":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"transform", group},
            description=f"Transform data for {table_name} with steps: {transformation_steps}",
            deps=deps,
        )

        # Transform asset: depends on source
        def transform_asset(source: str) -> str:
            # Validate that the source input was received
            if source is None:
                raise ValueError(f"Missing upstream source asset for: {group}.{table_name}.transform")

            # Apply transformation steps if defined, otherwise pass through
            if transformation_steps:
                return f"Transformed ({transformation_steps}) data from: {source}"
            else:
                return source

        transform_asset.__name__ = func_name
        return transform_asset

    # Target asset: depends on transform if enabled, otherwise source
    elif stage == "target":
        # Determine upstream function name based on transformation toggle
        upstream_stage = "transform" if table.get("transformation", {}).get("enabled", False) else "source"
        upstream_func_name = f"{group}_{table_name}_{upstream_stage}"

        @asset(
            key=asset_key,
            group_name=group,
            kinds={"target", group},
            description=f"Load data into {target_schema}.{table_name}",
            deps=deps,
        )

        # Target asset: depends on transform if enabled, otherwise source
        def target_asset(transform: Optional[str] = None, source: Optional[str] = None) -> str:
            # Use whichever upstream asset is available
            upstream_data = transform if transform is not None else source
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group}.{table_name}.target")

            return f"Loaded into {target_schema}.{table_name} from: {upstream_data}"


        target_asset.__name__ = func_name
        return target_asset

    # Invalid stage
    raise ValueError(f"Unknown asset stage: {stage}")


def build_assets_from_yaml(yaml_path: str | Path, groups_list: List[str]) -> List[AssetsDefinition]:
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
                continue  # Skip disabled tables

            # Create source asset
            source_asset = create_asset(group_name, table, "source")
            assets.append(source_asset)
            source_key = AssetKey([group_name, table["table"], "source"])

            # Create transform asset if enabled
            if table.get("transformation", {}).get("enabled", False):
                transform_asset = create_asset(group_name, table, "transform", upstream_key=source_key)
                assets.append(transform_asset)
                transform_key = AssetKey([group_name, table["table"], "transform"])
                # Target depends on transform
                target_asset = create_asset(group_name, table, "target", upstream_key=transform_key)
            else:
                # Target depends directly on source
                target_asset = create_asset(group_name, table, "target", upstream_key=source_key)

            assets.append(target_asset)

    return assets
