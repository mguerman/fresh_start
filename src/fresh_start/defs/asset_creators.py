import time
from typing import Dict, Optional
from dagster import asset, OpExecutionContext, AssetIn, AssetKey
from sqlalchemy import text, Table, Column, MetaData
from sqlalchemy.orm import Session
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import String, Integer, DateTime, Float, Boolean

# Constants for retrying and batching
INITIAL_DELAY = 5  # seconds
MAX_ATTEMPTS = 3
BATCH_SIZE = 1000

# Optional limit for queries (None or <=0 means no limit)
limit: Optional[int] = None

def get_limit_clause() -> str:
    """Return LIMIT clause or empty string if no limit set."""
    if limit is None or limit <= 0:
        return ""
    else:
        return f"LIMIT {limit}"

def batch_iterator(iterator, batch_size):
    """Yield successive batches of batch_size from iterator."""
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def map_oracle_type_to_postgres(oracle_type: str):
    """Map Oracle types string to SQLAlchemy/Postgres types."""
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
        return String  # default fallback


# ---------------- Core Execution Logic --------------------

def execute_oracle_to_postgres_replication(
    context: OpExecutionContext,
    upstream_sql: str,
    source_schema: str,
    target_schema: str,
    table_name: str,
    logger,
) -> int:
    """
    Core function to replicate data from Oracle to Postgres.

    Runs with retries and batching, creates target table if missing,
    and performs batch inserts with deduplication on row_hash.
    Returns the total number of inserted rows.
    """
    total_inserted = 0
    
    with context.resources.oracle.get_session() as oracle_session, \
         context.resources.postgres.get_session() as pg_session:

        # Check if Postgres target table exists
        if not _check_postgres_table_exists(pg_session, target_schema, table_name):
            logger.info(f"Creating target table {target_schema}.{table_name}")
            _create_postgres_table_from_oracle(oracle_session, pg_session, source_schema, target_schema, table_name, logger)

        # Fetch columns metadata from Oracle
        columns = _get_oracle_table_columns(oracle_session, source_schema, table_name)

        # Build full Oracle query with hash and metadata columns
        fetch_sql = _build_oracle_fetch_query(upstream_sql, columns)
        logger.info(f"Executing Oracle query to fetch {len(columns)} columns")

        oracle_result = oracle_session.execute(text(fetch_sql))

        # Insert fetched data into Postgres in batches
        total_inserted = _batch_insert_to_postgres(pg_session, oracle_result, target_schema, table_name, columns, logger)

    logger.info(f"Inserted {total_inserted} rows into {target_schema}.{table_name}")
    return total_inserted


def execute_postgres_to_postgres_replication(
    context: OpExecutionContext,
    upstream_data: str,
    target_schema: str,
    table_name: str,
    logger,
) -> str:
    """
    Core function to replicate data within Postgres from an upstream SQL string.

    Builds a table and inserts new data deduplicated by row_hash.
    """
    sql_statements = _build_postgres_sql_statements(upstream_data, target_schema, table_name)

    logger.info(f"Executing Postgres replication with {len(sql_statements)} statements")
    
    with context.resources.postgres.get_session() as session:
        for i, sql in enumerate(sql_statements):
            logger.info(f"Executing statement {i+1}: {sql[:100]}...")
            session.execute(text(sql))
        session.commit()

    return f"Successfully replicated data to {target_schema}.{table_name}"


# ---------------- Helper functions below -------------------

def _check_postgres_table_exists(pg_session: Session, target_schema: str, table_name: str) -> bool:
    """Check if PostgreSQL target table exists."""
    result = pg_session.execute(
        text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table
            )
        """), {"schema": target_schema, "table": table_name}
    )
    return result.scalar()

def _get_oracle_table_columns(oracle_session: Session, source_schema: str, table_name: str):
    """Retrieve Oracle table columns metadata."""
    columns = oracle_session.execute(
        text("""
            SELECT column_name, data_type
            FROM all_tab_columns
            WHERE owner = :schema AND table_name = :table
            ORDER BY column_id
        """), {"schema": source_schema.upper(), "table": table_name.upper()}
    ).fetchall()

    if not columns:
        raise RuntimeError(f"No columns found for {source_schema}.{table_name} in Oracle")

    return columns

def _create_postgres_table_from_oracle(oracle_session: Session, pg_session: Session, source_schema: str,
                                       target_schema: str, table_name: str, logger):
    """Create Postgres table matching Oracle table structure with additional metadata columns."""
    columns = _get_oracle_table_columns(oracle_session, source_schema, table_name)

    # Compose column definitions for create table
    column_defs = [
        f'"{col.column_name.lower()}" {map_oracle_type_to_postgres(col.data_type).compile(dialect=postgresql.dialect())}'
        for col in columns
    ] + [
        '"dl_inserteddate" timestamp',
        '"dl_insertedby" text',
        '"row_hash" text'
    ]

    create_table_sql = f'CREATE TABLE "{target_schema}"."{table_name}" ({", ".join(column_defs)});'
    logger.info(f"Creating PostgreSQL table {target_schema}.{table_name} (columns: {len(columns)})")
    pg_session.execute(text(create_table_sql))
    pg_session.commit()

    create_index_sql = f'CREATE UNIQUE INDEX idx_{table_name}_row_hash ON "{target_schema}"."{table_name}" (row_hash);'
    logger.info(f"Creating unique index on row_hash for {target_schema}.{table_name}")
    pg_session.execute(text(create_index_sql))
    pg_session.commit()

def _build_oracle_fetch_query(upstream_sql: str, columns):
    """Construct Oracle SQL query adding hash and metadata columns."""
    concat_expr = " || '|' || ".join(
        [f"NVL(TO_CHAR(t.{col.column_name.upper()}), '')" for col in columns]
    )
    return (
        f"SELECT t.*, "
        f"SYSTIMESTAMP AS dl_inserteddate, "
        f"'system' AS dl_insertedby, "
        f"STANDARD_HASH(({concat_expr}), 'MD5') AS row_hash "
        f"FROM ({upstream_sql}) t"
    )

def _batch_insert_to_postgres(pg_session: Session, oracle_result, target_schema: str, table_name: str, columns, logger) -> int:
    """Insert Oracle query results into Postgres in batches with ON CONFLICT DO NOTHING on row_hash."""
    
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
    total_inserted = 0

    for batch in batch_iterator(oracle_result, BATCH_SIZE):
        rows_to_insert = [{col: row._mapping[col] for col in all_columns} for row in batch]

        insert_stmt = pg_insert(pg_table).values(rows_to_insert)
        insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["row_hash"])

        pg_session.execute(insert_stmt)
        pg_session.commit()
        batch_size = len(rows_to_insert)
        total_inserted += batch_size
        if batch_size == BATCH_SIZE:
            logger.info(f"Inserted batch of {batch_size} rows (total so far: {total_inserted})")
    
    return total_inserted


def _build_postgres_sql_statements(upstream_data: str, target_schema: str, table_name: str):
    # Clean input
    cleaned_sql = upstream_data.strip().rstrip(";")
    limit_clause = get_limit_clause() or ""

    # Compose the inner query with added columns
    inner_query = (
        f"SELECT *, "
        f"now() AS dl_inserteddate, "
        f"'system' AS dl_insertedby, "
        f"md5(CAST(row_to_json(t) AS text)) AS row_hash "
        f"FROM ({cleaned_sql} {limit_clause}) t"
    )

    # Create table with 0 rows (structure only) by limiting query to 0 rows
    create_structure_sql = inner_query.replace(limit_clause, "LIMIT 0") if limit_clause else f"{inner_query} LIMIT 0"

    return [
        # Create table if not exists with structure only
        f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS ({create_structure_sql});',

        # Create unique index on row_hash if not exists
        f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash ON "{target_schema}"."{table_name}" (row_hash);',

        # Insert new rows avoiding duplicates using row_hash
        f'INSERT INTO "{target_schema}"."{table_name}" '
        f'SELECT * FROM ({inner_query}) new_data '
        f'WHERE NOT EXISTS ('
        f'  SELECT 1 FROM "{target_schema}"."{table_name}" existing '
        f'  WHERE existing.row_hash = new_data.row_hash'
        f');'
    ]