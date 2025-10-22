import os
import asyncio
import asyncpg
import oracledb
import hashlib
import json
import time
import traceback
import re
from datetime import datetime, timezone
from dotenv import load_dotenv

from sqlalchemy import (
    String, Integer, Float, SmallInteger, BigInteger, DateTime,
    Boolean, Text, LargeBinary
)
from sqlalchemy.dialects import postgresql as pg_dialect

# Initialize Oracle client
oracledb.init_oracle_client(lib_dir="/etc/oracle_client/instantclient_23_26/")

def load_env():
    load_dotenv()
    return {
        "oracle": {
            "user": os.environ["ORACLE_DB_USER"],
            "password": os.environ["ORACLE_DB_PASSWORD"],
            "host": os.environ["ORACLE_DB_HOST"],
            "port": int(os.environ["ORACLE_DB_PORT"]),
            "service": os.environ["ORACLE_DB_SERVICE"],
        },
        "postgres": {
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "host": os.environ["DB_HOST"],
            "port": int(os.environ["DB_PORT"]),
            "database": os.environ["DB_NAME"],
        }
    }

def map_oracle_type_to_postgres(data_type: str, precision=None, scale=None):
    normalized_type = data_type.strip().upper()

    # Match a NUMBER(precision, scale) formatted string (some drivers return it as string)
    m = re.match(r"NUMBER\((\d+),\s*(\d+)\)", normalized_type)
    if m:
        _, parsed_scale = map(int, m.groups())
        return Integer() if parsed_scale == 0 else Float()

    # Handle generic NUMBER type with precision/scale info if available
    if normalized_type == "NUMBER":
        if scale is not None and scale > 0:
            return Float()
        if precision is not None:
            if precision <= 4:
                return SmallInteger()
            elif precision <= 9:
                return Integer()
            else:
                return BigInteger()
        return Integer()

    type_map = {
        "VARCHAR2": String(255),
        "CHAR": String(255),
        "NVARCHAR2": String(255),
        "NVARCHAR": String(255),
        "INTEGER": Integer(),
        "INT": Integer(),
        "DECIMAL": Integer(),
        "DATE": DateTime(),
        "TIMESTAMP": DateTime(),
        "TIMESTAMP WITH TIME ZONE": DateTime(),
        "TIMESTAMP WITH LOCAL TIME ZONE": DateTime(),
        "FLOAT": Float(),
        "BINARY_FLOAT": Float(),
        "BINARY_DOUBLE": Float(),
        "REAL": Float(),
        "BOOLEAN": Boolean(),
        "CLOB": Text(),
        "BLOB": LargeBinary(),
        "LONG": Text(),
        "RAW": LargeBinary(),
    }

    return type_map.get(normalized_type, String(255))

def get_oracle_table_columns(cfg, owner: str, table: str):
    oracle_cfg = cfg["oracle"]
    dsn = f"{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service']}"
    conn = oracledb.connect(user=oracle_cfg['user'], password=oracle_cfg['password'], dsn=dsn)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :ownr AND TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
    """, {'ownr': owner.upper(), 'tbl': table.upper()})
    cols = cursor.fetchall()
    cursor.close()
    conn.close()
    return cols  # List of tuples: (col_name, data_type, precision, scale)

def build_create_table_sql(columns, pg_schema: str, table: str):
    col_defs = []
    pg_types = []
    for col_name, data_type, precision, scale in columns:
        pg_type_obj = map_oracle_type_to_postgres(data_type, precision, scale)
        type_str = pg_type_obj.compile(dialect=pg_dialect.dialect())
        col_defs.append(f"{col_name.lower()} {type_str}")
        pg_types.append(type_str)  # this is a string like 'TEXT', 'INTEGER', etc.

    # Add extra columns
    extra_columns = [
        ("dl_inserteddate", "TIMESTAMP"),
        ("dl_insertedby", "TEXT"),
        ("row_hash", "TEXT"),
    ]
    for col_name, type_str in extra_columns:
        col_defs.append(f"{col_name} {type_str}")
        pg_types.append(type_str)

    full_table_name = f"{pg_schema}.{table.lower()}"
    cols_sql = ",\n  ".join(col_defs)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          {cols_sql}
        );
    """
    return create_sql, pg_types

def get_unique_index_columns(cfg, owner, table):
    """
    Finds a unique index on the Oracle table.

    Prefers single-column unique index if available for efficient batching.

    Returns:
        List of column names for the unique index (ordered by position),
        or empty list if no unique index found.
    """
    oracle_cfg = cfg["oracle"]
    dsn = f"{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service']}"
    conn = oracledb.connect(user=oracle_cfg['user'], password=oracle_cfg['password'], dsn=dsn)
    cursor = conn.cursor()

    # Find all unique indexes for the table
    cursor.execute("""
        SELECT INDEX_NAME 
        FROM ALL_INDEXES 
        WHERE TABLE_OWNER = :ownr AND TABLE_NAME = :tbl AND UNIQUENESS = 'UNIQUE'
        ORDER BY INDEX_NAME
    """, {'ownr': owner.upper(), 'tbl': table.upper()})

    indexes = cursor.fetchall()

    # Prefer single-column unique index
    for (idx_name,) in indexes:
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM ALL_IND_COLUMNS 
            WHERE INDEX_OWNER = :ownr AND INDEX_NAME = :idx
            ORDER BY COLUMN_POSITION
        """, {'ownr': owner.upper(), 'idx': idx_name})
        cols = [row[0] for row in cursor.fetchall()]
        if len(cols) == 1:
            cursor.close()
            conn.close()
            print(f"Using single-column unique index '{idx_name}' with columns {cols}")
            return cols

    # Otherwise take the first unique index (can be composite)
    if indexes:
        idx_name = indexes[0][0]
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM ALL_IND_COLUMNS 
            WHERE INDEX_OWNER = :ownr AND INDEX_NAME = :idx
            ORDER BY COLUMN_POSITION
        """, {'ownr': owner.upper(), 'idx': idx_name})
        cols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        print(f"Using composite unique index '{idx_name}' with columns {cols}")
        return cols

    cursor.close()
    conn.close()
    print("No unique index found.")
    return []

def convert_oracle_row_to_pg(row, ora_types, pg_types):
    converted = []
    for val, ora_type, pg_type in zip(row, ora_types, pg_types):
        if val is None:
            converted.append(None)
            continue
        
        ora_type = ora_type.upper() if isinstance(ora_type, str) else ora_type
        pg_type = pg_type.upper() if isinstance(pg_type, str) else pg_type
        
        if ora_type in ('DATE', 'TIMESTAMP'):
            if isinstance(val, datetime):
                converted.append(val.isoformat())
            else:
                converted.append(str(val))
        elif ora_type in ('BLOB', 'RAW'):
            if hasattr(val, 'read'):
                converted.append(val.read())
            else:
                converted.append(val)
        elif pg_type == 'TEXT':
            # Convert any non-str to string, including ints or floats
            if not isinstance(val, str):
                converted.append(str(val))
            else:
                converted.append(val)
        else:
            converted.append(val)
    return tuple(converted)

async def insert_partition_to_postgres(
    columns, rows, pg_cfg, cfg, owner, table, pg_schema,
    create_table=False
):
    if not rows:
        print("No rows to insert.")
        return

    MAX_ATTEMPTS = 5
    attempt = 0
    delay = 2
    full_table_name = f"{pg_schema}.{table.lower()}"

    oracle_cols = None
    oracle_col_types = None  # List[str]
    pg_types = None  # List[str]

    while attempt < MAX_ATTEMPTS:
        try:
            conn = await asyncpg.connect(
                user=pg_cfg["user"],
                password=pg_cfg["password"],
                host=pg_cfg["host"],
                port=pg_cfg["port"],
                database=pg_cfg["database"]
            )

            if create_table:
                # Get column metadata from Oracle
                oracle_cols = get_oracle_table_columns(cfg, owner, table)
                # Build CREATE TABLE SQL and get only the PG type strings
                create_sql, pg_types = build_create_table_sql(oracle_cols, pg_schema, table)
                print(f"Creating target table with SQL:\n{create_sql}")
                await conn.execute(create_sql, timeout=30)
                # Extract oracle data types as strings for conversion
                oracle_col_types = [ora_type for _, ora_type, *_ in oracle_cols]
                create_table = False

            elif oracle_col_types is None or pg_types is None:
                # If we don't have metadata yet (not creating table now), fetch it
                oracle_cols = get_oracle_table_columns(cfg, owner, table)
                oracle_col_types = [ora_type for _, ora_type, *_ in oracle_cols]
                _, pg_types = build_create_table_sql(oracle_cols, pg_schema, table)

            # Only convert for the actual columns, excluding extra fields at end
            oracle_only_pg_types = pg_types[:len(oracle_col_types)]

            # Convert rows accordingly
            converted_rows = [
                convert_oracle_row_to_pg(row, oracle_col_types, oracle_only_pg_types)
                for row in rows
            ]

            now_str = datetime.now(timezone.utc).isoformat()
            enriched_rows = []
            column_names = [col[0].lower() for col in oracle_cols]
            for row in converted_rows:
                row_dict = dict(zip(column_names, row))
                row_hash = hashlib.md5(json.dumps(row_dict, default=str).encode()).hexdigest()
                enriched_rows.append(row + (now_str, "system", row_hash))

            placeholders = ", ".join([f"${i + 1}" for i in range(len(columns) + 3)])
            insert_sql = f"""
                INSERT INTO {full_table_name} ({', '.join(column_names)}, dl_inserteddate, dl_insertedby, row_hash)
                VALUES ({placeholders})
            """

            await conn.executemany(insert_sql, enriched_rows)
            await conn.close()
            print(f"Inserted {len(rows)} rows.")
            return

        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}")
            if attempt >= MAX_ATTEMPTS:
                raise RuntimeError(f"Failed to load data into {full_table_name} after {MAX_ATTEMPTS} attempts: {e}")
            print(f"Sleeping for {delay}s before retry...")
            time.sleep(delay)
            delay *= 2

def fetch_rows_by_index_batch(cfg, owner, table, unique_cols, last_values, batch_size):
    """
    Fetches the next batch of rows from Oracle table `owner.table` where the unique index columns > `last_values`,
    ordered by the unique index columns, up to `batch_size` rows.

    Args:
        cfg: Configuration dict with database connection info.
        owner: Oracle schema/owner name.
        table: Oracle table name.
        unique_cols: List of unique index columns (strings).
        last_values: tuple of last unique index values fetched; or empty tuple to start from beginning.
        batch_size: number of rows to fetch.

    Returns:
        Tuple (columns, rows, new_last_values):
           columns: list of lowercase column names,
           rows: list of tuples (data rows),
           new_last_values: tuple of unique index column values from last row fetched, or None if no rows.
    """
    oracle_cfg = cfg["oracle"]
    dsn = f"{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service']}"
    conn = oracledb.connect(user=oracle_cfg['user'], password=oracle_cfg['password'], dsn=dsn)
    cursor = conn.cursor()

    order_cols = ', '.join(unique_cols)
    where_clause = ""
    bind_vars = {}

    if last_values:
        placeholders = ', '.join([f":v{i}" for i in range(len(unique_cols))])
        where_clause = f"WHERE ({order_cols}) > ({placeholders})"
        bind_vars = {f"v{i}": val for i, val in enumerate(last_values)}

    query = f"""
        SELECT * FROM {owner}.{table}
        {where_clause}
        ORDER BY {order_cols}
        FETCH NEXT {batch_size} ROWS ONLY
    """

    cursor.execute(query, bind_vars)
    columns = [col[0].lower() for col in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    if rows:
        last_row = rows[-1]
        last_row_values = tuple(last_row[columns.index(col.lower())] for col in unique_cols)
        return columns, rows, last_row_values
    else:
        return columns, [], None

async def process_batches(cfg, owner, table, unique_cols, batch_size, pg_cfg, pg_schema):
    """
    Sequentially fetches batches from Oracle by unique index ranges,
    inserts them into Postgres, until no more rows remain.
    """
    last_values = tuple()
    first_batch = True
    total_rows = 0

    while True:
        loop = asyncio.get_running_loop()
        # fetch_rows_by_index_batch should be implemented as synchronous function
        columns, rows, new_last_values = await loop.run_in_executor(
            None,
            fetch_rows_by_index_batch,
            cfg, owner, table, unique_cols, last_values, batch_size
        )

        if not rows:
            break  # No more rows

        await insert_partition_to_postgres(
            columns, rows, pg_cfg, cfg, owner, table, pg_schema,
            create_table=first_batch
        )
        first_batch = False
        last_values = new_last_values
        total_rows += len(rows)
        print(f"Processed batch of {len(rows)} rows, total so far: {total_rows}")

    print(f"All batches processed, total rows inserted: {total_rows}")

async def main():
    cfg = load_env()
    owner = 'SYSADM'
    table = 'PS_STDNT_FA_TERM'
    batch_size = 10_000
    max_concurrent = 5  # For potential future concurrency (not used herein)

    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("No unique index found, exiting.")
        return

    print(f"Using unique index columns for batching: {unique_index_cols}")

    # Currently, batches are processed sequentially
    # If you want parallelism, wrap process_batches in tasks with semaphores

    await process_batches(cfg, owner, table, unique_index_cols, batch_size, cfg["postgres"], pg_schema="cs_raw")

    print("✅ All batches processed and inserted into PostgreSQL.")


if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        asyncio.run(main())
    except Exception:
        print("An error occurred during execution:")
        traceback.print_exc()
    finally:
        end_time = datetime.now()
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        elapsed = end_time - start_time
        print(f"Total execution time: {elapsed.total_seconds():.2f} seconds")