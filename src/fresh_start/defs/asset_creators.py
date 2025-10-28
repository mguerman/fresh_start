import time
from typing import Dict, Optional
from dagster import asset, OpExecutionContext, AssetIn

from sqlalchemy import text, Table, Column, MetaData
from sqlalchemy.orm import Session
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import String, Integer, DateTime, Float, Boolean

from dagster import AssetKey

# Retry and batching constants
INITIAL_DELAY = 5  # seconds
MAX_ATTEMPTS = 3
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
    """
    Yield items from an iterator in successive batches of batch_size.
    """
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def map_oracle_type_to_postgres(oracle_type: str):
    """
    Map Oracle data types (strings) to SQLAlchemy/Postgres types.
    """
    oracle_type = oracle_type.upper()
    if oracle_type in ("VARCHAR2", "CHAR", "NVARCHAR2"):
        return String
    elif oracle_type in ("NUMBER", "INTEGER", "INT"):
        return Integer
    elif oracle_type in ("DATE", "TIMESTAMP"):
        return DateTime
    elif oracle_type in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return Float
    elif oracle_type == "BOOLEAN":
        return Boolean
    else:
        return String  # Fallback

def create_source_asset(group_name: str, table: Dict):
    table_name = table["table"]
    source_schema = table["source_schema"]

    asset_key = AssetKey([group_name, table_name, "source"])

    @asset(
        key=asset_key,
        group_name=group_name,
        required_resource_keys={"postgres"},
        description=f"Extract data from {source_schema}.{table_name}",
    )
    def source_asset(context: OpExecutionContext) -> str:
        # Load YAML and filter group and table internally if needed if you want safety
        return f'SELECT * FROM "{source_schema}"."{table_name}"'

    return source_asset


def create_transform_asset(group_name: str, table: Dict, upstream_key: AssetKey):
    table_name = table["table"]
    transformation = table.get("transformation", {})
    transformation_enabled = transformation.get("enabled", False)
    transformation_steps = transformation.get("steps", "")

    if not transformation_enabled:
        # Skip transform asset creation if disabled
        return None

    asset_key = AssetKey([group_name, table_name, "transform"])

    @asset(
        key=asset_key,
        group_name=group_name,
        description=f"Transform data for {table_name} with steps: {transformation_steps}",
        ins={"source": AssetIn(key=upstream_key)},
    )
    def transform_asset(context: OpExecutionContext, source: str) -> str:
        if not source:
            raise ValueError(f"Missing upstream source asset for {group_name}.{table_name}.transform")
        # Simple example transform - prepend comment
        return f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source

    return transform_asset


def create_target_asset(group_name: str, table: Dict, upstream_key: AssetKey, source_db: str, target_db: str):

    """
    Create a Dagster target asset for a single table in a given group.

    Args:
        group_name: The name of the group this table belongs to.
        table: Table config dictionary from YAML.
        upstream_key: AssetKey of the upstream asset (source or transform) this target depends on.

    Returns:
        A Dagster AssetsDefinition for the target asset.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_enabled = transformation.get("enabled", False)
    upstream_stage = "transform" if transformation_enabled else "source"

    # Compose asset key with group, table, and stage
    asset_key = AssetKey([group_name, table_name, "target"])

    if source_db.lower() == "oracle" and target_db.lower() == "postgres":
        @asset(
            key=asset_key,
            group_name=group_name,
            required_resource_keys={"oracle", "postgres"},
            description=f"Load data into {target_schema}.{table_name}",
            ins={upstream_stage: AssetIn(key=upstream_key)},
        )
        def target_asset_ora_pg(context: OpExecutionContext, **kwargs) -> str:
            upstream_sql = kwargs.get(upstream_stage)
            skip_existing_table = table.get("skip_existing_table", True)

            if not upstream_sql:
                raise ValueError(f"Missing upstream SQL for {group_name}.{table_name}.target")

            logger = context.log

            attempt = 0
            delay = INITIAL_DELAY
            total_inserted = 0

            while attempt < MAX_ATTEMPTS:
                try:
                    with context.resources.oracle.get_session() as oracle_session, \
                        context.resources.postgres.get_session() as pg_session:

                        # Check if target table exists
                        exists_result = pg_session.execute(
                            text("""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_schema = :schema AND table_name = :table
                                );
                            """), {"schema": target_schema, "table": table_name}
                        )
                        table_exists = exists_result.scalar()

                        if not table_exists:
                            # Fetch Oracle metadata
                            columns = oracle_session.execute(
                                text("""
                                    SELECT column_name, data_type
                                    FROM all_tab_columns
                                    WHERE owner = :schema AND table_name = :table
                                    ORDER BY column_id
                                """),
                                {"schema": source_schema.upper(), "table": table_name.upper()}
                            ).fetchall()

                            if not columns:
                                raise RuntimeError(f"No columns found in Oracle table {source_schema}.{table_name}")

                            column_defs = [
                                f'"{col.column_name.lower()}" {map_oracle_type_to_postgres(col.data_type).compile(dialect=postgresql.dialect())}'
                                for col in columns
                            ] + ['"dl_inserteddate" timestamp', '"dl_insertedby" text', '"row_hash" text']

                            create_table_sql = f'CREATE TABLE "{target_schema}"."{table_name}" ({", ".join(column_defs)});'
                            logger.info(f"Creating Postgres table:\n{create_table_sql}")
                            pg_session.execute(text(create_table_sql))
                            pg_session.commit()

                            create_index_sql = f'CREATE UNIQUE INDEX idx_{table_name}_row_hash ON "{target_schema}"."{table_name}" (row_hash);'
                            logger.info(f"Creating unique index:\n{create_index_sql}")
                            pg_session.execute(text(create_index_sql))
                            pg_session.commit()

                        # Oracle query augmenting upstream SQL with hash & metadata
                        concat_expr = " || '|' || ".join(
                            [f"NVL(TO_CHAR(t.{col.column_name.upper()}), '')" for col in columns]
                        )
                        fetch_sql = (
                            f"SELECT t.*, "
                            f"SYSTIMESTAMP AS dl_inserteddate, "
                            f"'system' AS dl_insertedby, "
                            f"STANDARD_HASH(({concat_expr}), 'MD5') AS row_hash "
                            f"FROM ({upstream_sql}) t"
                        )
                        oracle_result = oracle_session.execute(text(fetch_sql))

                        # Map Postgres table for inserts
                        metadata = MetaData()
                        pg_table = Table(
                            table_name,
                            metadata,
                            *(Column(col.column_name.lower(), map_oracle_type_to_postgres(col.data_type)) for col in columns),
                            Column("dl_inserteddate", DateTime),
                            Column("dl_insertedby", String),
                            Column("row_hash", String),
                            schema=target_schema,
                        )

                        all_columns = [col.column_name.lower() for col in columns] + ["dl_inserteddate", "dl_insertedby", "row_hash"]

                        # Batch insert with conflict ignoring duplicates by row_hash
                        for batch in batch_iterator(oracle_result, BATCH_SIZE):
                            rows_to_insert = [{k: row._mapping[k] for k in all_columns} for row in batch]
                            insert_stmt = pg_insert(pg_table).values(rows_to_insert)
                            insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["row_hash"])
                            pg_session.execute(insert_stmt)
                            pg_session.commit()
                            total_inserted += len(rows_to_insert)

                    logger.info(f"Inserted {total_inserted} rows into {target_schema}.{table_name}")
                    return f"Inserted {total_inserted} rows into {target_schema}.{table_name}"

                except Exception as e:
                    attempt += 1
                    logger.warning(f"Attempt {attempt} failed with exception: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(
                            f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts."
                        ) from e
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff

            return "No data inserted due to retry exhaustion."

    elif source_db.lower() == "postgres" and target_db.lower() == "postgres":
        @asset(
            key=asset_key,
            group_name=group_name,
            required_resource_keys={"postgres"},
            description=f"Load data into {target_schema}.{table_name}",
            ins={upstream_stage: AssetIn(key=upstream_key)},
        )
        def target_asset(context, **kwargs) -> str:
            upstream_data = kwargs.get(upstream_stage)
            sql_limit_clause = get_limit_clause()
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group_name}.{table_name}.target")

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

    else:
        raise NotImplementedError(
            f"Target asset creation not implemented for source_db={source_db} to target_db={target_db}"
        )

    return target_asset