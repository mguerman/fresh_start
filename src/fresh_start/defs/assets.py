import yaml
import time
from pathlib import Path
from typing import List, Dict, Optional, Union
from dagster import asset, AssetKey, AssetsDefinition, AssetIn
from sqlalchemy.orm import Session
from sqlalchemy import text

# Retry configuration
MAX_ATTEMPTS = 3
INITIAL_DELAY = 5  # seconds

# Table skip if exists
skip_existing_table = True

# ────────────────────────────────────────────────────────────────────────────────
# Configuration variable: control query LIMIT; None means no limit
# ────────────────────────────────────────────────────────────────────────────────
limit: Optional[int] = None  # Set to None or 0 for no limit

def get_limit_clause() -> str:
    """
    Return the SQL limit clause string based on limit variable.
    Returns empty string if no limit specified.
    """
    if limit is None or limit <= 0:
        return ""  # No limit
    else:
        return f"LIMIT {limit}"


# ────────────────────────────────────────────────────────────────────────────────
# Load YAML configuration and filter by group names
# ────────────────────────────────────────────────────────────────────────────────
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


# ────────────────────────────────────────────────────────────────────────────────
# Create a Dagster asset for a given stage: source, transform, or target
# ────────────────────────────────────────────────────────────────────────────────
def create_asset(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None
) -> AssetsDefinition:
    """
    Dynamically create a Dagster asset for a given stage.

    Args:
        group: Name of the asset group.
        table: Table configuration dictionary from YAML.
        stage: One of "source", "transform", or "target".
        upstream_key: Optional dependency AssetKey.

    Returns:
        Dagster AssetsDefinition object.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")
    transformation_enabled = transformation.get("enabled", False)

    asset_key = AssetKey([group, table_name, stage]) # - review - causing problem
    func_name = f"{group}_{table_name}_{stage}"

    sql_limit_clause = get_limit_clause()

    # ────────────────────────────────────────────────────────────────────────────
    # Source asset: returns a preview query with LIMIT (or no limit)
    # ────────────────────────────────────────────────────────────────────────────
    if stage == "source":
        @asset(
            # key=AssetKey([group, 'src_' + table_name]), ------------review - causing problem
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract data from {source_schema}.{table_name}",
            required_resource_keys={"postgres"},
        )

        def source_asset() -> str:
            return f"SELECT * FROM {source_schema}.{table_name} {sql_limit_clause};"

        source_asset.__name__ = func_name
        return source_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Transform asset: applies optional transformation steps
    # ────────────────────────────────────────────────────────────────────────────
    elif stage == "transform":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"transform", group},
            description=f"Transform data for {table_name} with steps: {transformation_steps}",
            deps=[upstream_key] if upstream_key else [],
            ins={"source": AssetIn(key=upstream_key)} if upstream_key else {},
        )
        def transform_asset(source: str) -> str:
            if source is None:
                raise ValueError(f"Missing upstream source asset for: {group}.{table_name}.transform")
            return f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source

        transform_asset.__name__ = func_name
        return transform_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Target asset: loads data into target schema with retry logic
    # ────────────────────────────────────────────────────────────────────────────
    elif stage == "target":
        upstream_stage = "transform" if transformation_enabled else "source"
        upstream_asset_key = AssetKey([group, table_name, upstream_stage])

        @asset(
            # key=AssetKey([group, 'tgt_' + table_name]), causing problem
            key=asset_key,
            group_name=group,
            kinds={"target", group},
            description=f"Load data into {target_schema}.{table_name}",
            deps=[upstream_asset_key],
            ins={upstream_stage: AssetIn(key=upstream_asset_key)},
            required_resource_keys={"postgres"},
        )


        def target_asset(context, *, skip_existing_table: bool = True, **kwargs) -> str:
            upstream_data = kwargs.get(upstream_stage)
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group}.{table_name}.target")
            
            cleaned_sql = upstream_data.strip().rstrip(";")
            limit_snippet = sql_limit_clause or ""

            wrapped_sql = (
                f"SELECT *, "
                f"now() AS dl_inserteddate, "
                f"'system' AS dl_insertedby, "
                f"md5(CAST(row_to_json(t) AS text)) AS row_hash "
                f"FROM ({cleaned_sql} {limit_snippet}) t"
            )
            
            create_structure_sql = wrapped_sql.replace(limit_snippet, "LIMIT 0") if limit_snippet else wrapped_sql

            create_sql = (
                f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS\n'
                f'SELECT * FROM (\n{create_structure_sql}\n) AS structure_only;'
            )
            create_index_sql = (
                f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash '
                f'ON "{target_schema}"."{table_name}" (row_hash);'
            )
            insert_sql = (
                f'INSERT INTO "{target_schema}"."{table_name}"\n'
                f'SELECT * FROM (\n{wrapped_sql}\n) AS new_data\n'
                f'WHERE NOT EXISTS (\n'
                f'    SELECT 1 FROM "{target_schema}"."{table_name}" existing\n'
                f'    WHERE existing.row_hash = new_data.row_hash\n'
                f');'
            )

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    with context.resources.postgres.get_session() as session:
                        context.log.debug(f"skip_existing_table flag is set to: {skip_existing_table}")

                        if skip_existing_table:
                            context.log.debug(f"Checking existence of table {target_schema}.{table_name}")
                            table_exists_query = text(
                                """
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_schema = :schema AND table_name = :table
                                );
                                """
                            )
                            result = session.execute(table_exists_query, {"schema": target_schema, "table": table_name})
                            exists = result.scalar()
                            context.log.debug(f"Table existence result for {target_schema}.{table_name}: {exists}")

                            if exists:
                                context.log.info(f"Skipping {target_schema}.{table_name} as it already exists.")
                                return f"Skipped: table {target_schema}.{table_name} exists"

                        context.log.info(f"Creating table:\n{create_sql}")
                        context.log.info(f"Creating index:\n{create_index_sql}")
                        context.log.info(f"Inserting data:\n{insert_sql}")

                        session.execute(text(create_sql))
                        session.execute(text(create_index_sql))
                        session.execute(text(insert_sql))
                        session.commit()

                        return f"Created table, index, and inserted data into {target_schema}.{table_name}"

                except Exception as e:
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed to load data into {target_schema}.{table_name} "
                                        f"after {MAX_ATTEMPTS} attempts: {e}")
                    context.log.info(f"Sleeping for {delay} seconds before retrying...")
                    time.sleep(delay)
                    delay *= 2

            # This line should be unreachable
            return "Unreachable code path: no action taken"
        
        target_asset.__name__ = func_name
        return target_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Invalid stage
    # ────────────────────────────────────────────────────────────────────────────
    raise ValueError(f"Unknown asset stage: {stage}")

# ────────────────────────────────────────────────────────────────────────────────
# Build Dagster assets from YAML config
# ────────────────────────────────────────────────────────────────────────────────
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
                upstream_key = AssetKey([group_name, table["table"], "transform"])
            else:
                upstream_key = source_key

            # Create target asset
            target_asset_obj = create_asset(group_name, table, "target", True, upstream_key=upstream_key)
            assets.append(target_asset_obj)

    return assets