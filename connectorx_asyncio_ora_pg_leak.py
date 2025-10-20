import os
import io
import csv
import asyncio
import csv
import re

from typing import Optional, List
from datetime import datetime

import concurrent.futures

from dotenv import load_dotenv
import oracledb
import connectorx as cx
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.types import Integer, SmallInteger, BigInteger, Float, String, DateTime, Boolean, TypeEngine, Text, LargeBinary
from sqlalchemy.engine import Engine

# Load config with nested dict for oracle and postgres as you indicated
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

def normalize_dataframe(df: pd.DataFrame, oracle_columns: list) -> pd.DataFrame:
    """
    Normalize DataFrame column types to match inferred PostgreSQL types.
    """
    for col_name, oracle_type, precision, scale in oracle_columns:
        col = col_name.lower()
        if col not in df.columns:
            continue

        oracle_type = oracle_type.strip().upper()

        if oracle_type == "NUMBER":
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if scale == 0 or (scale is None and (df[col] % 1 == 0).all()):
                df[col] = df[col].fillna(0).astype(int)
                if precision is not None:
                    max_val = 10 ** precision
                    df[col] = df[col].clip(upper=max_val - 1)
            else:
                df[col] = df[col].astype(float)

        # Handle NUMBER(p,s)
        match = re.match(r"NUMBER\((\d+),\s*(\d+)\)", oracle_type)
        if match:
            _, scale = map(int, match.groups())
            if scale == 0:
                # Force integer conversion from float-like values
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif oracle_type == "NUMBER":
            # Default to float, but check if all values are whole numbers
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if (df[col] % 1 == 0).all():
                df[col] = df[col].fillna(0).astype(int)
            else:
                df[col] = df[col].astype(float)
        elif oracle_type in {"INTEGER", "INT", "DECIMAL"}:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        elif oracle_type in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "REAL"}:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif oracle_type in {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"}:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(str)

    return df


def init_oracle_thick(lib_dir="/home/mguerman/oracle_client/instantclient_19_28/"):
    print(f"Initializing Oracle thick client from {lib_dir}")
    oracledb.init_oracle_client(lib_dir=lib_dir)

def build_connectorx_dsn(cfg):
    return f"oracle://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['service']}"

def build_oracledb_dsn(cfg):
    return f"{cfg['host']}:{cfg['port']}/{cfg['service']}"

def build_postgres_url(pg_cfg):
    return f"postgresql://{pg_cfg['user']}:{pg_cfg['password']}@{pg_cfg['host']}:{pg_cfg['port']}/{pg_cfg['database']}"

def map_oracle_type_to_postgres(data_type: str, precision: Optional[int], scale: Optional[int]) -> TypeEngine:
    """
    Maps Oracle data types to SQLAlchemy-compatible PostgreSQL types.
    """
    normalized_type = data_type.strip().upper()

    # Handle formatted NUMBER(p,s)
    match = re.match(r"NUMBER\((\d+),\s*(\d+)\)", normalized_type)
    if match:
        _, parsed_scale = map(int, match.groups())
        return Integer() if parsed_scale == 0 else Float()

    # Handle generic NUMBER with precision/scale
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
        return Integer()  # Default fallback

    # Type categories
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
        "RAW": LargeBinary()
    }

    return type_map.get(normalized_type, String(255))

def get_unique_index_columns(cfg, owner, table):
    dsn_oracle = build_oracledb_dsn(cfg['oracle'])
    conn = oracledb.connect(user=cfg['oracle']['user'], password=cfg['oracle']['password'], dsn=f"//{dsn_oracle}")
    cursor = conn.cursor()

    sql_indexes = """
        SELECT INDEX_NAME 
        FROM ALL_INDEXES 
        WHERE TABLE_OWNER = :ownr AND TABLE_NAME = :tbl_name AND UNIQUENESS = 'UNIQUE'
        ORDER BY INDEX_NAME
    """
    cursor.execute(sql_indexes, {'ownr': owner.upper(), 'tbl_name': table.upper()})

    indexes = [row[0] for row in cursor.fetchall()]
    for idx_name in indexes:
        sql_cols = """
            SELECT COLUMN_NAME 
            FROM ALL_IND_COLUMNS 
            WHERE INDEX_OWNER = :ownr AND INDEX_NAME = :idx
            ORDER BY COLUMN_POSITION
        """
        cursor.execute(sql_cols, {'ownr': owner.upper(), 'idx': idx_name})
        cols = [row[0] for row in cursor.fetchall()]
        if cols:
            cursor.close()
            conn.close()
            print(f"Using unique index '{idx_name}' with columns {cols} for partitioning")
            return cols

    cursor.close()
    conn.close()
    print("No unique index found on table.")
    return []

def get_oracle_table_columns(cfg, owner, table):
    """
    Fetches column metadata from Oracle including name, type, precision, and scale.
    """
    dsn_oracle = build_oracledb_dsn(cfg['oracle'])
    conn = oracledb.connect(
        user=cfg['oracle']['user'],
        password=cfg['oracle']['password'],
        dsn=f"//{dsn_oracle}"
    )
    cursor = conn.cursor()

    sql = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl_name
        ORDER BY COLUMN_ID
    """
    cursor.execute(sql, {'owner': owner.upper(), 'tbl_name': table.upper()})
    columns = []
    for row in cursor.fetchall():
        col_name = row[0]
        data_type = row[1]
        precision = row[2] if row[2] is not None else None
        scale = row[3] if row[3] is not None else None
        columns.append((col_name, data_type, precision, scale))

    cursor.close()
    conn.close()
    return columns


def create_postgres_table(pg_url, schema, table, oracle_columns):
    """
    Dynamically creates a PostgreSQL table based on Oracle column metadata,
    and adds metadata columns for auditing and row hashing.
    """
    metadata = MetaData(schema=schema)
    engine = create_engine(pg_url)

    # Ensure schema exists (optional if schema is pre-created)
    # with engine.connect() as conn:
    #     conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    columns = [
        Column(col_name.lower(), map_oracle_type_to_postgres(data_type, precision, scale))
        for col_name, data_type, precision, scale in oracle_columns
    ]

    # Add metadata columns
    columns.append(Column("dl_inserteddate", DateTime(), server_default=text("now()")))
    columns.append(Column("dl_insertedby", String(), server_default=text("'system'")))
    columns.append(Column("row_hash", String(), nullable=True))  # Will be computed later

    pg_table = Table(table.lower(), metadata, *columns)
    metadata.create_all(engine, checkfirst=True)

    return f"{schema}.{table.lower()}", engine


def build_where_clause_for_keyset(columns):
    """
    Builds a SQL WHERE clause fragment for keyset paging on multiple columns.

    Example output for columns (col1, col2):
      (col1, col2) > (:last_col1, :last_col2)
    """
    if len(columns) == 1:
        return f"{columns[0]} > :last_{columns[0]}"
    else:
        cols = ", ".join(columns)
        placeholders = ", ".join(f":last_{col}" for col in columns)
        return f"({cols}) > ({placeholders})"

def fetch_batch_keyset(dsn, owner, table, columns, last_key_values, batch_size):
    """
    Fetch a batch of rows where the unique index columns > last_key_values.

    If last_key_values is None, fetch from start.
    """
    base_order = ", ".join(columns)
    where_clause = ""
    params = {}

    if last_key_values is not None:
        where_clause = "WHERE " + build_where_clause_for_keyset(columns)
        for col, val in zip(columns, last_key_values):
            params[f"last_{col}"] = val

    sql = f"""
        SELECT * FROM {owner}.{table}
        {where_clause}
        ORDER BY {base_order}
        FETCH FIRST {batch_size} ROWS ONLY
    """

    # connectorx currently does not support query parameters directly,
    # so we assemble SQL with parameters embedded as literals carefully here.

    # Simple safe literal quoting (assuming all are strings or numbers)
    def sql_literal(val):
        if val is None:
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        # escape single quotes in strings
        else:
            return f"'{str(val).replace('\'', '\'\'')}'"

    if last_key_values is not None:
        # Replace bind placeholders by literal values to embed into sql
        for col, val in zip(columns, last_key_values):
            placeholder = f":last_{col}"
            sql = sql.replace(placeholder, sql_literal(val))

    print(f"Executing batch SQL:\n{sql}")

    df = cx.read_sql(dsn, sql)
    return df

def keyset_pagination_loop(dsn, owner, table, columns, batch_size=1000):
    """
    Generator that yields batches of DataFrame using keyset pagination.
    """
    last_key = None
    while True:
        df = fetch_batch_keyset(dsn, owner, table, columns, last_key, batch_size)
        if df.empty:
            break
        yield df
        # Extract last key values from this batch
        last_key = tuple(df.iloc[-1][col] for col in columns)

def fetch_batch_offset(dsn, owner, table, columns, offset, batch_size):
    order_clause = ", ".join(columns)
    sql = f"""
        SELECT * FROM {owner}.{table}
        ORDER BY {order_clause}
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
    """
    return cx.read_sql(dsn, sql)

async def async_fetch_batches(
    dsn: str,
    owner: str,
    table: str,
    order_columns: list,
    batch_size: int = 1000,
    max_workers: int = 5,
    expected_columns: Optional[List[str]] = None  # Pass this from main using [col.name for col in oracle_columns]
):
    """
    Asynchronously fetches batches of data from Oracle using offset-based pagination,
    launching multiple concurrent fetches per batch and normalizing schema.
    """
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    offset = 0

    async def fetch(offset):
        return await loop.run_in_executor(
            executor,
            fetch_batch_offset,
            dsn,
            owner,
            table,
            order_columns,
            offset,
            batch_size
        )

    while True:
        # Launch multiple concurrent fetches
        offsets = [offset + i * batch_size for i in range(max_workers)]
        tasks = [fetch(o) for o in offsets]
        results = await asyncio.gather(*tasks)

        # Filter out empty results
        non_empty_batches = [df for df in results if not df.empty]
        if not non_empty_batches:
            break

        for df in non_empty_batches:
            if expected_columns:
                # Add missing columns with default nulls
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = pd.NA
                # Reorder columns to match expected schema
                df = df[expected_columns]

            yield df

        offset += batch_size * max_workers


def copy_batch_to_postgres(engine: Engine, table_name: str, dataframe: pd.DataFrame, oracle_columns: list) -> None:
    """
    Efficiently copies a pandas DataFrame to a PostgreSQL table using COPY FROM STDIN.
    Handles schema-qualified table names by setting the search_path.
    """
    if dataframe.empty:
        print("Empty DataFrame; skipping copy.")
        return

    # Normalize types
    dataframe = normalize_dataframe(dataframe, oracle_columns)

    # Prepare buffer for COPY
    buffer = io.StringIO()
    dataframe.to_csv(
        buffer,
        sep='\t',
        header=False,
        index=False,
        na_rep='\\N',
        quoting=csv.QUOTE_MINIMAL
    )
    buffer.seek(0)

    # Extract schema and table name
    if '.' in table_name:
        schema, raw_table_name = table_name.split('.', 1)
    else:
        schema, raw_table_name = None, table_name

    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Set search_path if schema is provided
        if schema:
            cursor.execute(f"SET search_path TO {schema}")

        columns = [col[0].lower() if isinstance(col, tuple) else col.lower() for col in oracle_columns]
        cursor.copy_from(buffer, raw_table_name, sep='\t', null='\\N', columns=columns)
        connection.commit()
    except Exception as error:
        print(f"Failed to copy data to PostgreSQL: {error}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


async def main():
    # Load environment and initialize Oracle client
    cfg = load_env()
    init_oracle_thick()

    # Define source and target
    owner = "SYSADM"
    table = "PS_STDNT_FA_TERM"
    pg_schema = "cs_raw"  # PostgreSQL target schema
    dsn_cx = build_connectorx_dsn(cfg['oracle'])
    pg_url = build_postgres_url(cfg["postgres"])

    # Get unique index columns for keyset pagination
    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("No unique index found, exiting.")
        return

    # Get Oracle column metadata and create PostgreSQL table
    oracle_columns = get_oracle_table_columns(cfg, owner, table)
    pg_table_name, pg_engine = create_postgres_table(pg_url, pg_schema, table, oracle_columns)


    expected_columns = [col[0] for col in oracle_columns]
    # Async batch fetch and copy loop
    async for batch_df in async_fetch_batches(
        dsn_cx,
        owner,
        table,
        unique_index_cols,
        batch_size=60000,
        max_workers=5,
        expected_columns=expected_columns
    ):
        copy_batch_to_postgres(pg_engine, pg_table_name, batch_df, oracle_columns)
        last_key = tuple(batch_df.iloc[-1][col] for col in unique_index_cols)
        print(f"✅ Processed batch with last key: {last_key}")

# Run with asyncio
if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    asyncio.run(main())

    end_time = datetime.now()
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")