import time
import json
import hashlib
from datetime import datetime
from typing import Dict, Optional
from dagster import asset, AssetKey, AssetIn, AssetsDefinition
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

MAX_ATTEMPTS = 3
INITIAL_DELAY = 5
BATCH_SIZE = 1000

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

def batch_iterator(iterator, batch_size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# ------------- Postgres to Postgres -------------

# ────────────────────────────────────────────────────────────────────────────────
# Create a Dagster asset for a given stage: source, transform, or target
# ────────────────────────────────────────────────────────────────────────────────
def create_asset_postgres_to_postgres(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None,
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
            required_resource_keys={"oracle"},
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


# ------------- Oracle to Postgres -------------

def create_asset_oracle_to_postgres(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None,
) -> asset:

    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")
    transformation_enabled = transformation.get("enabled", False)

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"

    def map_oracle_type_to_postgres(oracle_type: str) -> str:
        """
        Simple mapping of Oracle to Postgres types — extend if needed.
        """
        oracle_type = oracle_type.upper()
        if oracle_type.startswith("VARCHAR2") or oracle_type.startswith("NVARCHAR2"):
            return "VARCHAR"
        elif oracle_type == "NUMBER":
            return "NUMERIC"
        elif oracle_type == "DATE":
            return "TIMESTAMP"
        elif oracle_type == "CLOB":
            return "TEXT"
        else:
            return "TEXT"

    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            description=f"Extract data from {source_schema}.{table_name}",
            required_resource_keys={"oracle"},
        )
        def source_asset(context) -> str:
            # Return a select SQL string — this will be passed downstream
            sql = f'SELECT * FROM "{source_schema}"."{table_name}"'
            context.log.info(f"Source query for {source_schema}.{table_name}: {sql}")
            return sql
        source_asset.__name__ = func_name
        return source_asset

    elif stage == "transform":
        @asset(
            key=asset_key,
            group_name=group,
            description=f"Transform data for {table_name} with steps: {transformation_steps}",
            required_resource_keys=set(),
            deps=[upstream_key] if upstream_key else [],
            ins={"source": AssetIn(key=upstream_key)} if upstream_key else {},
        )
        def transform_asset(context, source: str) -> str:
            if source is None:
                raise ValueError(f"Missing upstream source asset for {group}.{table_name}.transform")
            # Example: Apply transformation steps (here just returning SQL string)
            transformed_sql = f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source
            context.log.info(f"Transform SQL for {table_name}: {transformed_sql}")
            return transformed_sql
        transform_asset.__name__ = func_name
        return transform_asset

    elif stage == "target":
        upstream_stage = "transform" if transformation_enabled else "source"
        upstream_asset_key = AssetKey([group, table_name, upstream_stage])

        @asset(
            key=asset_key,
            group_name=group,
            description=f"Load data into {target_schema}.{table_name}",
            required_resource_keys={"oracle", "postgres"},
            deps=[upstream_asset_key],
            ins={upstream_stage: AssetIn(key=upstream_asset_key)},
        )
        def target_asset(context, *, skip_existing_table: bool = skip_existing_table, **kwargs) -> str:
            upstream_sql = kwargs.get(upstream_stage)
            if not upstream_sql:
                raise ValueError(f"Missing upstream SQL for {group}.{table_name}.target")

            oracle_sql = upstream_sql.strip().rstrip(";")

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    with context.resources.oracle.get_session() as oracle_session, \
                         context.resources.postgres.get_session() as pg_session:

                        # Check if target table exists
                        if skip_existing_table:
                            context.log.debug(f"Checking existence of {target_schema}.{table_name}")
                            check_table_sql = text("""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_schema = :schema AND table_name = :table
                                );
                            """)
                            result = pg_session.execute(check_table_sql, {"schema": target_schema, "table": table_name})
                            exists = result.scalar()
                            context.log.debug(f"Table existence for {target_schema}.{table_name}: {exists}")
                            if exists:
                                context.log.info(f"Skipping {target_schema}.{table_name} (exists)")
                                return f"Skipped: table {target_schema}.{table_name} exists"

                        # Get Oracle columns metadata
                        col_query = text("""
                            SELECT column_name, data_type
                            FROM all_tab_columns
                            WHERE owner = :schema AND table_name = :table
                            ORDER BY column_id
                        """)
                        columns = oracle_session.execute(
                            col_query,
                            {"schema": source_schema.upper(), "table": table_name.upper()}
                        ).fetchall()
                        if not columns:
                            raise RuntimeError(f"No columns found in Oracle table {source_schema}.{table_name}")

                        # Build CREATE TABLE statement for Postgres
                        col_defs = []
                        for col in columns:
                            pg_type = map_oracle_type_to_postgres(col.column_name if hasattr(col, "column_name") else col[1])
                            col_name = col.column_name.lower() if hasattr(col, "column_name") else col[0].lower()
                            col_type = col.data_type if hasattr(col, "data_type") else col[1]
                            pg_type = map_oracle_type_to_postgres(col_type)
                            col_defs.append(f'"{col_name}" {pg_type}')

                        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" ({", ".join(col_defs)});'
                        context.log.info(f"Creating Postgres table structure:\n{create_table_sql}")
                        pg_session.execute(text(create_table_sql))

                        # Create unique index on row_hash column if you use it:
                        create_index_sql = f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_row_hash
                        ON "{target_schema}"."{table_name}" (row_hash);
                        """
                        context.log.info(f"Creating unique index:\n{create_index_sql}")
                        pg_session.execute(text(create_index_sql))

                        # Fetch data from Oracle
                        fetch_sql = (
                            f"SELECT t.*, "
                            f"SYSTIMESTAMP AS dl_inserteddate, "
                            f"'system' AS dl_insertedby, "
                            f"STANDARD_HASH(TO_CLOB(t), 'MD5') AS row_hash "
                            f"FROM ({oracle_sql}) t"
                        )
                        oracle_result = oracle_session.execute(text(fetch_sql)).yield_per(BATCH_SIZE)

                        # Insert into Postgres in batches
                        rowcount = 0
                        for batch in batch_iterator(oracle_result, BATCH_SIZE):
                            rows_to_insert = [dict(row.items()) for row in batch]
                            if rows_to_insert:
                                insert_stmt = pg_insert(text=f'"{target_schema}"."{table_name}"').values(rows_to_insert)
                                insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["row_hash"])
                                pg_session.execute(insert_stmt)
                                rowcount += len(rows_to_insert)

                        pg_session.commit()
                        context.log.info(f"Inserted {rowcount} rows into {target_schema}.{table_name}")
                        return f"Created table and inserted {rowcount} rows into {target_schema}.{table_name}"

                except Exception as e:
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts: {e}")
                    context.log.info(f"Sleeping {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2

            # Fallback: should never be reached
            return "No operation performed"

        target_asset.__name__ = func_name
        return target_asset

    else:
        raise ValueError(f"Unknown asset stage: {stage}")    