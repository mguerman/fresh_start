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
        source_db: Source database type ('oracle' or 'postgres')
        target_db: Target database type ('postgres')

    Returns:
        A Dagster AssetsDefinition for the target asset.
        
    Raises:
        NotImplementedError: If the source_db to target_db combination is not supported.
        ValueError: If required table configuration is missing.
    """
    # Extract table configuration
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_enabled = transformation.get("enabled", False)
    upstream_stage = "transform" if transformation_enabled else "source"

    # Compose asset key
    asset_key = AssetKey([group_name, table_name, "target"])

    # Route to appropriate implementation based on database types
    if source_db.lower() == "oracle" and target_db.lower() == "postgres":
        return _create_oracle_to_postgres_asset(
            asset_key, group_name, table_name, source_schema, target_schema,
            upstream_stage, upstream_key, table
        )
    elif source_db.lower() == "postgres" and target_db.lower() == "postgres":
        return _create_postgres_to_postgres_asset(
            asset_key, group_name, table_name, target_schema,
            upstream_stage, upstream_key
        )
    else:
        raise NotImplementedError(
            f"Target asset creation not implemented for source_db='{source_db}' to target_db='{target_db}'"
        )


def _create_oracle_to_postgres_asset(asset_key, group_name, table_name, source_schema, target_schema, 
                                    upstream_stage, upstream_key, table):
    """Create Oracle to Postgres target asset."""
    
    @asset(
        key=asset_key,
        group_name=group_name,
        required_resource_keys={"oracle", "postgres"},
        description=f"Load data from Oracle into {target_schema}.{table_name}",
        ins={upstream_stage: AssetIn(key=upstream_key)},
    )
    def oracle_to_postgres_target(context: OpExecutionContext, **kwargs) -> str:
        """Asset that loads data from Oracle to Postgres with deduplication."""
        upstream_sql = kwargs.get(upstream_stage)
        
        if not upstream_sql:
            raise ValueError(f"Missing upstream SQL for {group_name}.{table_name}.target")

        logger = context.log
        logger.info(f"Starting Oracle to Postgres replication for {target_schema}.{table_name}")

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return _execute_oracle_to_postgres_replication(
                    context, upstream_sql, source_schema, target_schema, 
                    table_name, logger
                )
            except Exception as e:
                if attempt == MAX_ATTEMPTS:
                    raise RuntimeError(
                        f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts."
                    ) from e
                
                delay = INITIAL_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)

        return "No data inserted due to retry exhaustion."
    
    return oracle_to_postgres_target


def _create_postgres_to_postgres_asset(asset_key, group_name, table_name, target_schema, 
                                      upstream_stage, upstream_key):
    """Create Postgres to Postgres target asset."""
    
    @asset(
        key=asset_key,
        group_name=group_name,
        required_resource_keys={"postgres"},
        description=f"Load data into {target_schema}.{table_name}",
        ins={upstream_stage: AssetIn(key=upstream_key)},
    )
    def postgres_to_postgres_target(context: OpExecutionContext, **kwargs) -> str:
        """Asset that loads data from Postgres to Postgres with deduplication."""
        upstream_data = kwargs.get(upstream_stage)
        
        if upstream_data is None:
            raise ValueError(f"Missing upstream asset for: {group_name}.{table_name}.target")

        logger = context.log
        logger.info(f"Starting Postgres to Postgres replication for {target_schema}.{table_name}")

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return _execute_postgres_to_postgres_replication(
                    context, upstream_data, target_schema, table_name, logger
                )
            except Exception as e:
                if attempt == MAX_ATTEMPTS:
                    raise RuntimeError(
                        f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts: {e}"
                    )
                
                delay = INITIAL_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)

        return "Unreachable code path: no action taken"
    
    return postgres_to_postgres_target


def _execute_oracle_to_postgres_replication(context, upstream_sql, source_schema, target_schema, 
                                           table_name, logger):
    """Execute the actual Oracle to Postgres data replication."""
    total_inserted = 0
    
    with context.resources.oracle.get_session() as oracle_session, \
         context.resources.postgres.get_session() as pg_session:

        # Check if target table exists
        table_exists = _check_postgres_table_exists(pg_session, target_schema, table_name)
        
        if not table_exists:
            logger.info(f"Creating target table {target_schema}.{table_name}")
            _create_postgres_table_from_oracle(
                oracle_session, pg_session, source_schema, target_schema, table_name, logger
            )

        # Get Oracle metadata for hash calculation
        columns = _get_oracle_table_columns(oracle_session, source_schema, table_name)
        
        # Build and execute data fetch query
        fetch_sql = _build_oracle_fetch_query(upstream_sql, columns)
        logger.info(f"Executing Oracle query with {len(columns)} columns")
        
        oracle_result = oracle_session.execute(text(fetch_sql))

        # Batch insert into Postgres
        total_inserted = _batch_insert_to_postgres(
            pg_session, oracle_result, target_schema, table_name, columns, logger
        )

    logger.info(f"Successfully inserted {total_inserted} rows into {target_schema}.{table_name}")
    return f"Inserted {total_inserted} rows into {target_schema}.{table_name}"


def _execute_postgres_to_postgres_replication(context, upstream_data, target_schema, table_name, logger):
    """Execute the actual Postgres to Postgres data replication."""
    # Prepare SQL statements
    sql_statements = _build_postgres_sql_statements(upstream_data, target_schema, table_name)
    
    logger.info(f"Executing Postgres replication with {len(sql_statements)} statements")
    
    with context.resources.postgres.get_session() as session:
        for i, sql in enumerate(sql_statements):
            logger.info(f"Executing statement {i+1}: {sql[:100]}...")
            session.execute(text(sql))
        
        session.commit()
    
    return f"Successfully replicated data to {target_schema}.{table_name}"


def _check_postgres_table_exists(pg_session, target_schema, table_name):
    """Check if a Postgres table exists."""
    exists_result = pg_session.execute(
        text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            );
        """), {"schema": target_schema, "table": table_name}
    )
    return exists_result.scalar()


def _get_oracle_table_columns(oracle_session, source_schema, table_name):
    """Get Oracle table column metadata."""
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
    
    return columns


def _create_postgres_table_from_oracle(oracle_session, pg_session, source_schema, target_schema, 
                                      table_name, logger):
    """Create Postgres table based on Oracle table structure."""
    columns = _get_oracle_table_columns(oracle_session, source_schema, table_name)
    
    # Build column definitions
    column_defs = [
        f'"{col.column_name.lower()}" {map_oracle_type_to_postgres(col.data_type).compile(dialect=postgresql.dialect())}'
        for col in columns
    ] + [
        '"dl_inserteddate" timestamp',
        '"dl_insertedby" text',
        '"row_hash" text'
    ]

    # Create table
    create_table_sql = f'CREATE TABLE "{target_schema}"."{table_name}" ({", ".join(column_defs)});'
    logger.info(f"Creating table with {len(columns)} columns")
    pg_session.execute(text(create_table_sql))
    pg_session.commit()

    # Create unique index
    create_index_sql = f'CREATE UNIQUE INDEX idx_{table_name}_row_hash ON "{target_schema}"."{table_name}" (row_hash);'
    logger.info("Creating unique index on row_hash")
    pg_session.execute(text(create_index_sql))
    pg_session.commit()


def _build_oracle_fetch_query(upstream_sql, columns):
    """Build Oracle query with hash and metadata columns."""
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


def _batch_insert_to_postgres(pg_session, oracle_result, target_schema, table_name, columns, logger):
    """Batch insert Oracle results into Postgres with conflict resolution."""
    # Create SQLAlchemy table metadata
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

    # Process in batches
    for batch in batch_iterator(oracle_result, BATCH_SIZE):
        rows_to_insert = [
            {col: row._mapping[col] for col in all_columns} 
            for row in batch
        ]
        
        insert_stmt = pg_insert(pg_table).values(rows_to_insert)
        insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["row_hash"])
        
        pg_session.execute(insert_stmt)
        pg_session.commit()
        total_inserted += len(rows_to_insert)
        
        if len(batch) == BATCH_SIZE:  # Only log for full batches to reduce noise
            logger.info(f"Processed batch of {len(batch)} rows (total: {total_inserted})")

    return total_inserted


def _build_postgres_sql_statements(upstream_data, target_schema, table_name):
    """Build SQL statements for Postgres to Postgres replication."""
    cleaned_sql = upstream_data.strip().rstrip(";")
    sql_limit_clause = get_limit_clause()
    limit_snippet = sql_limit_clause or ""

    # Build data selection query with metadata
    wrapped_sql = (
        f"SELECT *, "
        f"now() AS dl_inserteddate, "
        f"'system' AS dl_insertedby, "
        f"md5(CAST(row_to_json(t) AS text)) AS row_hash "
        f"FROM ({cleaned_sql} {limit_snippet}) t"
    )

    # Build structure-only query for table creation
    create_structure_sql = wrapped_sql.replace(limit_snippet, "LIMIT 0") if limit_snippet else wrapped_sql

    # Return list of SQL statements to execute
    return [
        # Create table with structure
        f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS '
        f'SELECT * FROM ({create_structure_sql}) AS structure_only;',
        
        # Create unique index
        f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash '
        f'ON "{target_schema}"."{table_name}" (row_hash);',
        
        # Insert new data
        f'INSERT INTO "{target_schema}"."{table_name}" '
        f'SELECT * FROM ({wrapped_sql}) AS new_data '
        f'WHERE NOT EXISTS ('
        f'    SELECT 1 FROM "{target_schema}"."{table_name}" existing '
        f'    WHERE existing.row_hash = new_data.row_hash '
        f');'
    ]