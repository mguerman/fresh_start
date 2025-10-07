import yaml
from pathlib import Path
from typing import List, Dict, Optional, Union
from dagster import asset, AssetKey, AssetsDefinition, AssetIn
from sqlalchemy.orm import Session
from sqlalchemy import text

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

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"

    sql_limit_clause = get_limit_clause()

    # ────────────────────────────────────────────────────────────────────────────
    # Source asset: returns a preview query with LIMIT (or no limit)
    # ────────────────────────────────────────────────────────────────────────────
    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract data from {source_schema}.{table_name}",
        )
        def source_asset() -> str:
            # Return SELECT with conditional LIMIT
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

            # Apply transformation steps if defined
            return f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source

        transform_asset.__name__ = func_name
        return transform_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Target asset: loads data into target schema
    # ────────────────────────────────────────────────────────────────────────────
    elif stage == "target":
        upstream_stage = "transform" if transformation_enabled else "source"
        upstream_asset_key = AssetKey([group, table_name, upstream_stage])

        @asset(
            key=asset_key,
            group_name=group,
            kinds={"target", group},
            description=f"Load data into {target_schema}.{table_name}",
            deps=[upstream_asset_key],
            ins={upstream_stage: AssetIn(key=upstream_asset_key)},
            required_resource_keys={"postgres"},
        )

        def target_asset(context, **kwargs) -> str:
            upstream_data = kwargs.get(upstream_stage)
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group}.{table_name}.target")

            # Remove trailing semicolon to safely reuse SQL
            cleaned_sql = upstream_data.strip().rstrip(";")

            # The LIMIT clause snippet, empty if no limit is set
            limit_snippet = sql_limit_clause  # e.g. "LIMIT 10" or ""

            # Wrap upstream query to inject metadata columns including row_hash
            # Conditionally append LIMIT clause to cleaned_sql if exists
            wrapped_sql_with_metadata = (
                f"SELECT *, "
                f"now() AS dl_inserteddate, "
                f"'system' AS dl_insertedby, "
                f"md5(CAST(row_to_json(t) AS text)) AS row_hash "
                f"FROM ({cleaned_sql} {limit_snippet}) t"
            )

            # When creating table structure only, replace the LIMIT clause with LIMIT 0
            # But only do replacement when a LIMIT clause exists, to avoid syntax errors
            if limit_snippet:
                create_structure_sql = wrapped_sql_with_metadata.replace(limit_snippet, "LIMIT 0")
            else:
                # No limit to replace, just use original wrapped SQL
                create_structure_sql = wrapped_sql_with_metadata

            # Create table if it does not exist with structure including metadata columns
            create_sql = (
                f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS\n'
                f'SELECT * FROM (\n'
                f'{create_structure_sql}\n'
                f') AS structure_only;\n'
            )

            # Create a unique index on row_hash to enforce uniqueness of rows
            create_index_sql = (
                f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash ON "{target_schema}"."{table_name}" (row_hash);'
            )

            # Insert data selecting from the upstream query with metadata/resolved limit,
            # but only insert new rows that don't already exist based on row_hash
            insert_sql = (
                f'INSERT INTO "{target_schema}"."{table_name}"\n'
                f'SELECT * FROM (\n'
                f'{wrapped_sql_with_metadata}\n'
                f') AS new_data\n'
                f'WHERE NOT EXISTS (\n'
                f'    SELECT 1 FROM "{target_schema}"."{table_name}" existing\n'
                f'    WHERE existing.row_hash = new_data.row_hash\n'
                f');'
            )

            # Log the generated SQL statements for debugging and traceability
            context.log.info(f"Creating table if not exists:\n{create_sql}")
            context.log.info(f"Creating unique index on row_hash:\n{create_index_sql}")
            context.log.info(f"Inserting data:\n{insert_sql}")

            try:
                # Get SQLAlchemy session from the postgres resource
                session: Session = context.resources.postgres.get_session()

                # Execute table creation SQL
                session.execute(text(create_sql))

                # Execute create unique index SQL
                session.execute(text(create_index_sql))

                # Execute insert data SQL
                session.execute(text(insert_sql))

                # Commit the transaction
                session.commit()

                return f"Created table, created unique index, and inserted data into {target_schema}.{table_name}"
            except Exception as e:
                # Wrap and raise with contextual error message
                raise RuntimeError(f"Failed to load data into {target_schema}.{table_name}: {e}")
            
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
            target_asset_obj = create_asset(group_name, table, "target", upstream_key=upstream_key)
            assets.append(target_asset_obj)

    return assets