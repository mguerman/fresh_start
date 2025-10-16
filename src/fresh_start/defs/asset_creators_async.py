import asyncio
import oracledb

import time
from typing import Dict, Optional
from dagster import asset, AssetKey, AssetIn, AssetsDefinition

from sqlalchemy import text, Table, Column, MetaData
from sqlalchemy import String, Integer, DateTime, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from io import StringIO

# Retry and batching constants
MAX_ATTEMPTS = 3
INITIAL_DELAY = 5  # seconds
BATCH_SIZE = 10000

limit: Optional[int] = None  # Global row limit, if needed

async def fetch_oracle_batches(
    context,
    oracle_sql: str,
    column_names: list[str],
    pg_session,
    pg_table,
    all_columns: list[str],
    table_created: bool,
    target_schema: str,
    table_name: str
    ) -> int:
    """
    Fetches data from Oracle in async batches and inserts into PostgreSQL.

    Args:
        context: Dagster context with logging and resources.
        oracle_sql: Base SQL query to paginate.
        column_names: Expected Oracle column names.
        pg_session: SQLAlchemy session for PostgreSQL.
        pg_table: SQLAlchemy table object for inserts.
        all_columns: List of all columns for copy_from or insert.
        table_created: Whether the target table already exists.
        target_schema: PostgreSQL schema name.
        table_name: PostgreSQL table name.

    Returns:
        Total number of rows inserted.
    """

    # Create async Oracle session from Dagster resource
    session_factory = context.resources.oracle.get_async_session(context)
    async with session_factory() as oracle_session:
        # Get raw connection and cursor
        async with (await oracle_session.connection()) as conn:
            async with conn.cursor() as cursor:
                offset = 0
                batch_number = 1
                rowcount = 0

                while True:
                    # Paginate the Oracle SQL query
                    paginated_sql = (
                        f"SELECT * FROM ({oracle_sql}) "
                        f"OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY"
                    )

                    await cursor.execute(paginated_sql)
                    batch = await cursor.fetchall()

                    if not batch:
                        break  # No more data

                    context.log.info(f"Batch {batch_number}: Fetched {len(batch)} rows")

                    # Convert rows to dicts using column names
                    rows_to_insert = [
                        {desc[0].lower(): val for desc, val in zip(cursor.description, row)}
                        for row in batch
                    ]

                    context.log.info(f"Batch {batch_number}: Prepared {len(rows_to_insert)} rows for insert")

                    if not rows_to_insert:
                        continue  # Skip empty batches

                    if table_created:
                        # Use PostgreSQL COPY for bulk insert
                        buffer = StringIO()
                        for row in rows_to_insert:
                            buffer.write('\t'.join(str(row.get(col, '') or '') for col in all_columns) + '\n')
                        buffer.seek(0)

                        raw_conn = pg_session.connection().connection
                        pg_cursor = raw_conn.cursor()
                        pg_cursor.copy_from(
                            buffer,
                            f'"{target_schema}.{table_name}"',
                            sep='\t',
                            columns=all_columns
                        )
                        raw_conn.commit()
                    else:
                        # Use INSERT with conflict handling
                        insert_stmt = pg_insert(pg_table).values(rows_to_insert)
                        insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["row_hash"])
                        pg_session.execute(insert_stmt)
                        pg_session.commit()

                    rowcount += len(rows_to_insert)
                    offset += BATCH_SIZE
                    batch_number += 1

    return rowcount


def get_limit_clause(dialect: str = "postgres") -> str:
    """
    Generate SQL limit clause depending on DB dialect.
    Returns empty string if no limit is set.
    """
    if limit is None or limit <= 0:
        return ""

    if dialect == "postgres":
        return f"LIMIT {limit}"
    elif dialect == "oracle":
        return f"FETCH FIRST {limit} ROWS ONLY"
    return ""


def batch_iterator(iterator, batch_size):
    """
    Yield items from iterator in batches.
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
        return String  # Default fallback


def create_asset_postgres_to_postgres(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None
    ) -> AssetsDefinition:
    """
    Postgres to Postgres pipeline assets builder.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")
    transformation_enabled = transformation.get("enabled", False)

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"

    sql_limit_clause = get_limit_clause("postgres")

    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract data from {source_schema}.{table_name}",
            required_resource_keys={"postgres"},
        )
        def source_asset() -> str:
            return f'SELECT * FROM "{source_schema}"."{table_name}" {sql_limit_clause}'

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
        def transform_asset(source: str) -> str:
            if source is None:
                raise ValueError(f"Missing upstream source asset for {group}.{table_name}.transform")
            return f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source

        transform_asset.__name__ = func_name
        return transform_asset

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
        def target_asset(context, *, skip_existing_table: bool = skip_existing_table, **kwargs) -> str:
            upstream_sql = kwargs.get(upstream_stage)
            if not upstream_sql:
                raise ValueError(f"Missing upstream SQL for {group}.{table_name}.target")

            cleaned_sql = upstream_sql.strip().rstrip(";")
            wrapped_sql = (
                f"SELECT *, "
                f"now() AS dl_inserteddate, "
                f"'system' AS dl_insertedby, "
                f"md5(CAST(row_to_json(t) AS text)) AS row_hash "
                f"FROM ({cleaned_sql}) t"
            )

            create_structure_sql = f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS SELECT * FROM ({wrapped_sql} LIMIT 0) t;'

            create_index_sql = f'CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_row_hash ON "{target_schema}"."{table_name}" (row_hash);'

            insert_sql = (
                f'INSERT INTO "{target_schema}"."{table_name}" '
                f'SELECT * FROM ({wrapped_sql}) AS new_data '
                f'WHERE NOT EXISTS (SELECT 1 FROM "{target_schema}"."{table_name}" existing '
                f'WHERE existing.row_hash = new_data.row_hash);'
            )

            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    with context.resources.postgres.get_session() as session:
                        context.log.debug(f"skip_existing_table = {skip_existing_table}")

                        if skip_existing_table:
                            checking_sql = text(
                                """
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_schema = :schema AND table_name = :table
                                );
                                """
                            )
                            result = session.execute(checking_sql, {"schema": target_schema, "table": table_name})
                            exists = result.scalar()
                            context.log.debug(f"Table existence for {target_schema}.{table_name}: {exists}")
                            if exists:
                                context.log.info(f"Skipping {target_schema}.{table_name} as it already exists.")
                                return f"Skipped: table {target_schema}.{table_name} exists"

                        context.log.info(f"Creating table:\n{create_structure_sql}")
                        session.execute(text(create_structure_sql))

                        context.log.info(f"Creating index:\n{create_index_sql}")
                        session.execute(text(create_index_sql))

                        # context.log.info(f"Inserting data:\n{insert_sql}")
                        session.execute(text(insert_sql))

                        session.commit()
                        return f"Created table, index, and inserted data into {target_schema}.{table_name}"

                except Exception as e:
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts: {e}")
                    context.log.info(f"Sleeping for {delay}s before retry...")
                    time.sleep(delay)
                    delay *= 2

            return "No operation performed"

        target_asset.__name__ = func_name
        return target_asset

    else:
        raise ValueError(f"Unknown asset stage: {stage}")


def create_asset_oracle_to_postgres(
    group: str,
    table: Dict,
    stage: str,
    skip_existing_table: bool = True,
    upstream_key: Optional[AssetKey] = None,
    ) -> AssetsDefinition:
    """
    Create Dagster assets for Oracle-to-Postgres replication stages.
    Handles source, transform, and target assets with retry on DB operations.

    Args:
        group: asset group name.
        table: configuration dict describing the source and target tables.
        stage: current pipeline stage - "source", "transform", or "target".
        skip_existing_table: whether to skip creation if the target table exists.
        upstream_key: optional Dagster AssetKey dependency.

    Returns:
        Dagster AssetsDefinition for the specified stage.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")
    transformation_enabled = transformation.get("enabled", False)

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"

    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            description=f"Extract data from {source_schema}.{table_name}",
            required_resource_keys={"oracle"},
        )
        def source_asset(context) -> str:
            # Compose SQL with uppercase schema and table names without quotes
            sql = f'SELECT * FROM {source_schema.upper()}.{table_name.upper()}'
            # context.log.info(f"Source query for {source_schema}.{table_name}: {sql}")
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
            transformed_sql = f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source
            # context.log.info(f"Transform SQL for {table_name}: {transformed_sql}")
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

        async def target_asset(context, *, skip_existing_table: bool = skip_existing_table, **kwargs) -> str:
            """
            Dagster asset that replicates data from Oracle to PostgreSQL in async batches.

            Args:
                context: Dagster context with access to resources and logging.
                skip_existing_table: Whether to skip table creation if it already exists.
                kwargs: Contains upstream SQL under the key `upstream_stage`.

            Returns:
                A message indicating how many rows were inserted.
            """
            upstream_sql = kwargs.get(upstream_stage)
            if not upstream_sql:
                raise ValueError(f"Missing upstream SQL for {group}.{table_name}.target")

            oracle_sql = upstream_sql.strip().rstrip(";")
            attempt = 0
            delay = INITIAL_DELAY

            while attempt < MAX_ATTEMPTS:
                try:
                    # Create async sessions for metadata and table checks
                    oracle_session_factory = context.resources.oracle.get_async_session()
                    pg_session_factory = context.resources.postgres.get_async_session()

                    async with oracle_session_factory() as oracle_session, pg_session_factory() as pg_session:
                        # Check if target table exists in PostgreSQL
                        context.log.debug(f"Checking existence of {target_schema}.{table_name}")
                        result = await pg_session.execute(
                            text("""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_schema = :schema AND table_name = :table
                                );
                            """),
                            {"schema": target_schema, "table": table_name}
                        )
                        table_exists = result.scalar()
                        table_created = not table_exists

                        # Fetch Oracle column metadata
                        result = await oracle_session.execute(
                            text("""
                                SELECT column_name, data_type
                                FROM all_tab_columns
                                WHERE owner = :schema AND table_name = :table
                                ORDER BY column_id
                            """),
                            {"schema": source_schema.upper(), "table": table_name.upper()}
                        )
                        columns = await result.fetchall()

                        if not columns:
                            raise RuntimeError(f"No columns found in Oracle table {source_schema}.{table_name}")

                        # Map Oracle columns to PostgreSQL types
                        column_names, pg_types = zip(*[
                            (
                                col.column_name.lower() if hasattr(col, "column_name") else col[0].lower(),
                                map_oracle_type_to_postgres(col.data_type if hasattr(col, "data_type") else col[1])
                            )
                            for col in columns
                        ])

                        # Define column definitions for table creation
                        col_defs = [
                            f'"{name}" {pg_type().compile(dialect=postgresql.dialect())}'
                            for name, pg_type in zip(column_names, pg_types)
                        ] + [
                            '"dl_inserteddate" timestamp',
                            '"dl_insertedby" text',
                            '"row_hash" text',
                        ]

                        if table_created and not skip_existing_table:
                            # Create target table
                            create_table_sql = f'CREATE TABLE "{target_schema}"."{table_name}" ({", ".join(col_defs)});'
                            context.log.info(f"Creating Postgres table:\n{create_table_sql}")
                            await pg_session.execute(text(create_table_sql))
                            await pg_session.commit()

                            # Create unique index on row_hash
                            create_index_sql = f'''
                                CREATE UNIQUE INDEX idx_{table_name}_row_hash
                                ON "{target_schema}"."{table_name}" (row_hash);
                            '''
                            context.log.info(f"Creating index:\n{create_index_sql}")
                            await pg_session.execute(text(create_index_sql))
                            await pg_session.commit()

                        # Compose Oracle query with metadata columns and row hash
                        concat_expr = " || '|' || ".join([
                            f"NVL(TO_CHAR(t.{col.upper()}), '')" for col in column_names
                        ])
                        fetch_sql = (
                            f"SELECT t.*, "
                            f"SYSTIMESTAMP AS dl_inserteddate, "
                            f"'system' AS dl_insertedby, "
                            f"STANDARD_HASH(({concat_expr}), 'MD5') AS row_hash "
                            f"FROM ({oracle_sql}) t"
                        )

                        # Define SQLAlchemy table object for PostgreSQL inserts
                        metadata = MetaData()
                        pg_table = Table(
                            table_name,
                            metadata,
                            *(Column(name, pg_type) for name, pg_type in zip(column_names, pg_types)),
                            Column("dl_inserteddate", DateTime),
                            Column("dl_insertedby", String),
                            Column("row_hash", String),
                            schema=target_schema
                        )

                        # Prepare column list and run async batch fetch
                        all_columns = list(column_names) + ["dl_inserteddate", "dl_insertedby", "row_hash"]
                        rowcount = await fetch_oracle_batches(
                            context,
                            fetch_sql,
                            column_names,
                            pg_session,
                            pg_table,
                            all_columns,
                            table_created,
                            target_schema,
                            table_name
                        )

                        context.log.info(f"Inserted {rowcount} rows into {target_schema}.{table_name}")
                        return f"Inserted {rowcount} rows into {target_schema}.{table_name}"

                except Exception as e:
                    attempt += 1
                    context.log.warning(f"Attempt {attempt} failed: {e}")
                    if attempt >= MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed to load data into {target_schema}.{table_name} after {MAX_ATTEMPTS} attempts: {e}")
                    context.log.info(f"Sleeping for {delay} seconds before retry...")
                    await asyncio.sleep(delay)
                    delay *= 2

            return "No operation performed"

        target_asset.__name__ = func_name
        return target_asset

    else:
        raise ValueError(f"Unknown asset stage: {stage}")
