from typing import List, Dict
from dagster import AssetsDefinition, AssetKey
from .asset_creators import create_source_asset, create_transform_asset, create_target_asset

def build_assets_from_yaml(yaml_path: str, groups_list: List[Dict]) -> List[AssetsDefinition]:
    all_assets = []

    for group in groups_list:
        group_name = group["name"]
        tables = group.get("tables", [])

        for table in tables:
            table_name = table["table"]
            upstream_key = AssetKey([group_name, table_name, "source"])

            # --- Get per-table DBs from YAML
            source_db = table.get("source_db")
            target_db = table.get("target_db")

            if source_db is None or target_db is None:
                raise ValueError(
                    f"Table '{table_name}' in group '{group_name}' must specify source_db and target_db."
                )

            # Create source asset
            source = create_source_asset(group_name, table)
            if source is not None:
                all_assets.append(source)

            # Create transform asset (may be None if disabled)
            transform = create_transform_asset(group_name, table, upstream_key)
            if transform is not None:
                all_assets.append(transform)
                # If transform exists, update upstream_key for target
                upstream_key = AssetKey([group_name, table_name, "transform"])

            # Create target asset (always depends on upstream_key, which may be source or transform)
            
            target = create_target_asset(
                            group_name,
                            table,
                            upstream_key,
                            source_db=source_db,
                            target_db=target_db,
            )

            if target is not None:
                all_assets.append(target)

    return all_assets
