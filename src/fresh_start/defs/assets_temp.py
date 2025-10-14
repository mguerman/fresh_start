import json
import hashlib
import time
from datetime import datetime
from typing import Dict, Optional
from dagster import asset, AssetKey, AssetIn, AssetsDefinition
from sqlalchemy import insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

MAX_ATTEMPTS = 3
INITIAL_DELAY = 5
BATCH_SIZE = 1000

def batch_iterator(iterator, batch_size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

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
            required_resource_keys={"oracle", "postgres"},
        )
        def target_asset(context, *, skip_existing_table: bool = True, **kwargs) -> str:
            # Upstream SQL string (select query)
            upstream_data = kwargs.get(upstream_stage)
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group}.{table_name}.target")

            cleaned_sql = upstream_data.strip().rstrip(";")
            limit_snippet = sql_limit_clause or ""

            # Compose Oracle query to fetch rows with extra columns "row_hash", "dl_inserteddate", "dl_insertedby"
            oracle_fetch_sql = (
                f"SELECT t.*, "
                f"SYSTIMESTAMP AS dl_inserteddate, "
                f"'system' AS dl_insertedby, "
                f"STANDARD_HASH(TO_CLOB(t), 'MD5') AS row_hash "  # Oracle MD5 hash equivalent
                f"FROM ({cleaned_sql} {limit_snippet}) t"
            )

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    with context.resources.oracle.get_session() as oracle_session, \
                         context.resources.postgres.get_session() as pg_session:

                        # Check if target table exists
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
                            result = pg_session.execute(table_exists_query, {"schema": target_schema, "table": table_name})
                            exists = result.scalar()
                            context.log.debug(f"Table existence result for {target_schema}.{table_name}: {exists}")

                            if exists:
                                context.log.info(f"Skipping {target_schema}.{table_name} as it already exists.")
                                return f"Skipped: table {target_schema}.{table_name} exists"

                        # Create table structure in Postgres by running the upstream select with zero rows
                        create_structure_sql = (
                            f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS '
                            f'SELECT * FROM ({oracle_fetch_sql.replace(limit_snippet, "WHERE 1=0") if limit_snippet else oracle_fetch_sql} ) AS structure_only;'
                        )
                        context.log.info(f"Creating table structure:\n{create_structure_sql}")
                        pg_session.execute(text(create_structure_sql))

                        # Create unique index on row_hash
                        create_index_sql = (
                            f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash '
                            f'ON "{target_schema}"."{table_name}" (row_hash);'
                        )
                        context.log.info(f"Creating unique index:\n{create_index_sql}")
                        pg_session.execute(text(create_index_sql))

                        # Now fetch data from Oracle in batches and insert into Postgres
                        oracle_result = oracle_session.execute(text(oracle_fetch_sql)).yield_per(BATCH_SIZE)

                        rowcount = 0
                        for batch in batch_iterator(oracle_result, BATCH_SIZE):
                            rows_to_insert = []
                            for row in batch:
                                # Convert RowProxy or similar to dict
                                row_dict = dict(row.items())
                                rows_to_insert.append(row_dict)
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
                        raise RuntimeError(
                            f"Failed to load data into {target_schema}.{table_name} " 
                            f"after {MAX_ATTEMPTS} attempts: {e}"
                        )
                    context.log.info(f"Sleeping for {delay} seconds before retrying...")
                    time.sleep(delay)
                    delay *= 2

            return "Unreachable code path: no action taken"

        target_asset.__name__ = func_name
        return target_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Invalid stage
    # ────────────────────────────────────────────────────────────────────────────
    raise ValueError(f"Unknown asset stage: {stage}")