import yaml
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Union
from dagster import asset, AssetKey, AssetsDefinition, AssetIn
from sqlalchemy.orm import Session
from sqlalchemy import text, Table, Column, MetaData, String, inspect
import hashlib
import json

# Retry configuration
MAX_ATTEMPTS = 3
INITIAL_DELAY = 5  # seconds

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


def create_asset_oracle_postgres(
    group: str,
    table: Dict,
    stage: str,
    upstream_key: Optional[AssetKey] = None
    ) -> AssetsDefinition:
    """
    Create Dagster assets for Oracle→Postgres pipeline using staged CSV files.
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

    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract and stage data from {source_schema}.{table_name}",
            required_resource_keys={"oracle"},
        )
        def source_asset(context) -> str:
            query = f"SELECT * FROM {source_schema}.{table_name} {sql_limit_clause}"
            session = context.resources.oracle.get_session()
            df = pd.read_sql(query, session.bind)

            staging_path = f"/tmp/{group}_{table_name}_source.csv"
            df.to_csv(staging_path, index=False)

            context.log.info(f"Staged source data at: {staging_path}")
            return staging_path

        source_asset.__name__ = func_name
        return source_asset

    elif stage == "transform":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"transform", group},
            description=f"Transform data for {table_name} with steps: {transformation_steps}",
            deps=[upstream_key] if upstream_key else [],
            ins={"source": AssetIn(key=upstream_key)} if upstream_key else {},
        )
        def transform_asset(context, source_path: str) -> str:
            if not source_path:
                raise ValueError(f"Missing upstream staged file for {group}.{table_name}.transform")

            df = pd.read_csv(source_path)
            # TODO: Implement real transformation logic here
            if transformation_steps:
                context.log.info(f"Applying transformations: {transformation_steps}")
                # e.g. df = apply_transform_steps(df, transformation_steps)

            staging_path = f"/tmp/{group}_{table_name}_transform.csv"
            df.to_csv(staging_path, index=False)
            context.log.info(f"Transformed data staged at: {staging_path}")
            return staging_path

        transform_asset.__name__ = func_name
        return transform_asset

    elif stage == "target":
        upstream_stage = "transform" if transformation_enabled else "source"
        upstream_asset_key = AssetKey([group, table_name, upstream_stage])

        @asset(
            key=asset_key,
            group_name=group,
            kinds={"target", group},
            description=f"Load data into {target_schema}.{table_name} from staged CSV",
            deps=[upstream_asset_key],
            ins={upstream_stage: AssetIn(key=upstream_asset_key)},
            required_resource_keys={"postgres"},
        )


        def target_asset(context, **kwargs) -> str:
            staged_file_path = kwargs.get(upstream_stage)
            if not staged_file_path:
                raise ValueError(f"Missing upstream staged file for {group}.{table_name}.target")

            df = pd.read_csv(staged_file_path)

            # Add data load metadata and hash column
            df["dl_inserteddate"] = pd.Timestamp.now()
            df["dl_insertedby"] = "system"

            def hash_row(row: pd.Series) -> str:
                row_json = json.dumps(row.to_dict(), sort_keys=True)
                return hashlib.md5(row_json.encode("utf-8")).hexdigest()
            df["row_hash"] = df.apply(hash_row, axis=1)

            metadata = MetaData(schema=target_schema)
            columns = [Column(c, String) for c in df.columns]
            table_obj = Table(table_name, metadata, *columns, extend_existing=True)

            session: Session = context.resources.postgres.get_session()
            engine = session.get_bind()
            inspector = inspect(engine)

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    with engine.connect() as connection: # type: ignore
                        if not inspector.has_table(table_name, schema=target_schema):
                            metadata.create_all(engine)
                            context.log.info(f"Created table {target_schema}.{table_name}")

                        create_index_sql = (
                            f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash_{table_name} '
                            f'ON "{target_schema}"."{table_name}" (row_hash);'
                        )
                        connection.execute(text(create_index_sql))

                        # Insert statement with ON CONFLICT DO NOTHING for deduplication
                        insert_sql = text(
                            f'INSERT INTO "{target_schema}"."{table_name}" ({", ".join(df.columns)}) '
                            f'VALUES ({", ".join(":"+col for col in df.columns)}) '
                            f'ON CONFLICT (row_hash) DO NOTHING'
                        )

                        # Use session for the insert to benefit from transaction, or connection.execute in a transaction block
                        for row in df.to_dict(orient="records"):
                            session.execute(text(insert_sql), parameters=row) # type: ignore
                        session.commit()

                        context.log.info(f"Inserted {len(df)} rows into {target_schema}.{table_name}")
                        return f"Successfully inserted {len(df)} rows into {target_schema}.{table_name}"

                except Exception as e:
                    session.rollback()
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed with error: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed after {MAX_ATTEMPTS} attempts: {e}")
                    time.sleep(delay)
                    delay *= 2

            return "Unreachable code path: no action taken"
        target_asset.__name__ = func_name
        return target_asset

    else:
        raise ValueError(f"Unknown asset stage: {stage}")


# ────────────────────────────────────────────────────────────────────────────────
# Create a Dagster asset for a given stage: source, transform, or target
# ────────────────────────────────────────────────────────────────────────────────
def create_asset_postgres_postgres(
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
        )

        def source_asset() -> str:
            return f"SELECT * FROM {source_schema}.{table_name} {sql_limit_clause}"

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
        def target_asset(context, **kwargs) -> str:
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

            context.log.info(f"Creating table:\n{create_sql}")
            context.log.info(f"Creating index:\n{create_index_sql}")
            context.log.info(f"Inserting data:\n{insert_sql}")

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    session: Session = context.resources.postgres.get_session()
                    session.execute(text(create_sql))
                    session.execute(text(create_index_sql))
                    session.execute(text(insert_sql))
                    session.commit()
                    return f"Created table, index, and inserted data into {target_schema}.{table_name}"
                except Exception as e:
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        # Raise and immediately return a string to satisfy Pylance
                        raise RuntimeError(
                            f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts: {e}"
                        )
                    time.sleep(delay)
                    delay *= 2

            # This line is unreachable, but ensures all code paths return a string
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
            source_asset = create_asset_postgres_postgres(group_name, table, "source")
            assets.append(source_asset)
            source_key = AssetKey([group_name, table["table"], "source"])

            # Create transform asset if enabled
            if table.get("transformation", {}).get("enabled", False):
                transform_asset = create_asset_postgres_postgres(group_name, table, "transform", upstream_key=source_key)
                assets.append(transform_asset)
                upstream_key = AssetKey([group_name, table["table"], "transform"])
            else:
                upstream_key = source_key

            # Create target asset
            target_asset_obj = create_asset_postgres_postgres(group_name, table, "target", upstream_key=upstream_key)
            assets.append(target_asset_obj)

    return assets