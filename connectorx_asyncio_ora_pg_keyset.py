import os
import io
import csv
import asyncio
import csv
import re
import gc
import time
from contextlib import contextmanager

import numpy as np
import datetime

from typing import List, Optional, Tuple, Any, AsyncGenerator, Generator

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


@contextmanager
def oracle_connection(cfg):
    dsn_oracle = build_oracledb_dsn(cfg['oracle'])
    conn = oracledb.connect(user=cfg['oracle']['user'], password=cfg['oracle']['password'], dsn=f"//{dsn_oracle}")
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def oracle_cursor(cfg):
    with oracle_connection(cfg) as conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

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
    with oracle_cursor(cfg) as cursor:
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
                print(f"Using unique index '{idx_name}' with columns {cols} for partitioning")
                return cols
    print("No unique index found on table.")
    return []

def get_oracle_table_columns(cfg, owner, table):
    with oracle_cursor(cfg) as cursor:
        sql = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :tbl_name
            ORDER BY COLUMN_ID
        """
        cursor.execute(sql, {'owner': owner.upper(), 'tbl_name': table.upper()})
        columns = [(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]
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


def sql_literal(val):
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, datetime):
        formatted = val.strftime('%Y-%m-%d %H:%M:%S')
        return f"TO_TIMESTAMP('{formatted}', 'YYYY-MM-DD HH24:MI:SS')"
    if isinstance(val, datetime.date):
        formatted = val.strftime('%Y-%m-%d')
        return f"TO_DATE('{formatted}', 'YYYY-MM-DD')"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"

def build_where_clause_for_keyset(columns):
    if len(columns) == 1:
        return f"{columns[0]} > :last_{columns[0]}"
    else:
        cols = ", ".join(columns)
        placeholders = ", ".join(f":last_{col}" for col in columns)
        return f"({cols}) > ({placeholders})"

def build_lexicographical_where(columns, last_key_values):
    if not last_key_values:
        return ""  # No WHERE clause for the first batch

    assert len(columns) == len(last_key_values), "Mismatch between columns and key values"

    conditions = []
    for i in range(len(columns)):
        parts = []
        for j in range(i):
            col = columns[j]
            val = last_key_values[j]
            parts.append(f"{col} = {format_sql_value(val)}")
        col = columns[i]
        val = last_key_values[i]
        parts.append(f"{col} > {format_sql_value(val)}")
        condition = " AND ".join(parts)
        conditions.append(f"({condition})")

    where_clause = "WHERE " + " OR ".join(conditions)
    return where_clause

def format_sql_value(val):
    if val is None:
        return "NULL"
    elif isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(val, (int, float, np.integer, np.floating)):
        return str(val)
    elif isinstance(val, (datetime.datetime, np.datetime64)):
        if isinstance(val, np.datetime64):
            val = pd.to_datetime(val).to_pydatetime()
        return f"TO_TIMESTAMP('{val.isoformat()}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF')"
    else:
        raise ValueError(f"Unsupported value type: {type(val)}")

def fetch_batch_keyset(dsn, owner, table, columns, last_key_values, batch_size):
    base_order = ", ".join(columns)
    where_clause = build_lexicographical_where(columns, last_key_values)

    sql = f"""
        SELECT * FROM {owner}.{table}
        {where_clause}
        ORDER BY {base_order}
        FETCH FIRST {batch_size} ROWS ONLY
    """

    start_time = time.monotonic()
    df = cx.read_sql(dsn, sql)
    end_time = time.monotonic()

    fetch_duration = end_time - start_time
    return df, fetch_duration

async def async_fetch_batches_keyset(
    dsn: str,
    owner: str,
    table: str,
    order_columns: List[str],
    batch_size: int = 1000,
    max_workers: int = 5,
    expected_columns: Optional[List[str]] = None,
) -> AsyncGenerator[Tuple[pd.DataFrame, float], None]:
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    last_keys: List[Optional[Tuple[Any, ...]]] = [None] * max_workers
    finished_workers = 0

    async def fetch(worker_id: int) -> Tuple[pd.DataFrame, float]:
        key = last_keys[worker_id]  # None for first fetch
        return await loop.run_in_executor(
            executor,
            fetch_batch_keyset,
            dsn,
            owner,
            table,
            order_columns,
            key,
            batch_size,
        )

    tasks = [fetch(i) for i in range(max_workers)]
    while finished_workers < max_workers:
        results = await asyncio.gather(*tasks)
        new_tasks = []
        yielded_any = False

        for worker_id, (df, fetch_duration) in enumerate(results):
            if df.empty:
                print(f"[Worker {worker_id}] empty batch, marking finished.")
                finished_workers += 1
                last_keys[worker_id] = None
                continue

            # Ensure all expected columns present
            if expected_columns is not None:
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[expected_columns]

            n_rows = len(df)
            print(f"[Worker {worker_id}] fetched {n_rows} rows in {fetch_duration:.2f}s")

            new_last_key = tuple(df.iloc[-1][col] for col in order_columns)

            if last_keys[worker_id] == new_last_key:
                print(f"[Worker {worker_id}] last key did not advance, marking finished.")
                finished_workers += 1
                last_keys[worker_id] = None
                continue

            # Update last key for next batch fetch
            last_keys[worker_id] = new_last_key

            # If less than batch size, this is final batch
            if n_rows < batch_size:
                print(f"[Worker {worker_id}] fetched last partial batch, marking finished.")
                finished_workers += 1

            yielded_any = True
            yield df, fetch_duration

            # Re-queue fetching only if not finished
            if last_keys[worker_id] is not None:
                new_tasks.append(fetch(worker_id))

        if not yielded_any:
            print("No batches yielded this iteration, finishing loop.")
            break

        tasks = new_tasks
        gc.collect()

def keyset_pagination_loop(
    dsn: str,
    owner: str,
    table: str,
    columns: List[str],
    batch_size: int = 1000
) -> Generator[pd.DataFrame, None, None]:
    """
    Generator that yields batches of DataFrame using keyset pagination.
    """
    last_key: Optional[tuple] = None

    while True:
        df = fetch_batch_keyset(dsn, owner, table, columns, last_key, batch_size)

        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

        if df.empty:
            break

        yield df

        # Extract last key values from this batch safely
        if not df.empty:
            last_key = tuple(df.iloc[-1][col] for col in columns)
        else:
            last_key = None

def fetch_batch_offset(dsn, owner, table, columns, offset, batch_size):
    import time
    order_clause = ", ".join(columns)
    sql = f"""
        SELECT * FROM {owner}.{table}
        ORDER BY {order_clause}
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
    """
    start_time = time.monotonic()
    df = cx.read_sql(dsn, sql)
    end_time = time.monotonic()
    fetch_duration = end_time - start_time
    return df, fetch_duration

async def async_fetch_batches(
    dsn: str,
    owner: str,
    table: str,
    order_columns: list,
    batch_size: int = 1000,
    max_workers: int = 5,
    expected_columns: Optional[List[str]] = None
):
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
        offsets = [offset + i * batch_size for i in range(max_workers)]
        tasks = [fetch(o) for o in offsets]
        results = await asyncio.gather(*tasks)

        batches = [(df, duration) for df, duration in results if not df.empty]
        if not batches:
            break

        for df, fetch_duration in batches:
            if expected_columns:
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[expected_columns]
            yield df, fetch_duration  # yield tuple with df and fetch time
            gc.collect()

        offset += batch_size * max_workers

def escape_newlines_in_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace newlines and carriage returns in string columns with literal '\n' characters.
    Leaves NaN and '\\N' sentinel values unchanged.
    """
    str_cols = df.select_dtypes(include=['object']).columns
    df = df.copy()
    for col in str_cols:
        df[col] = df[col].where(
            df[col].isna() | (df[col] == '\\N'),
            df[col].str.replace('\n', '\\n').str.replace('\r', '\\n'),
        )
    return df

def fix_null_timestamps(df: pd.DataFrame, oracle_columns: list) -> pd.DataFrame:
    df = df.copy()
    for col_name, oracle_type, *_ in oracle_columns:
        col = col_name.lower()
        if col not in df.columns:
            continue
        oracle_type_up = oracle_type.strip().upper()
        if oracle_type_up in {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"}:
            # Replace string "\N" or empty strings with NaT
            df.loc[df[col] == '\\N', col] = pd.NaT
            df.loc[df[col] == '', col] = pd.NaT
            # Convert to datetime to ensure proper type
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def copy_batch_to_postgres(engine: Engine, table_name: str, dataframe: pd.DataFrame, oracle_columns: List) -> None:
    if dataframe.empty:
        print("Empty DataFrame; skipping copy.")
        return

    # Normalize dataframe schema and types before escaping newlines
    dataframe = normalize_dataframe(dataframe, oracle_columns)
    dataframe = fix_null_timestamps(dataframe, oracle_columns)
    dataframe = escape_newlines_in_df(dataframe)

    buffer = io.StringIO()
    dataframe.to_csv(
        buffer,
        sep='\t',
        header=False,
        index=False,
        na_rep='NULL',
        quoting=csv.QUOTE_NONE,
        escapechar=None
    )
    buffer.seek(0)

    if '.' in table_name:
        schema, raw_table_name = table_name.split('.', 1)
    else:
        schema, raw_table_name = None, table_name

    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        if schema:
            cursor.execute(f"SET search_path TO {schema}")

        columns = [col[0].lower() for col in oracle_columns]
        cursor.copy_from(buffer, raw_table_name, sep='\t', null='NULL', columns=columns)
        conn.commit()
        print(f"Copied {len(dataframe)} rows into {table_name}")
    except Exception as error:
        print(f"Failed to copy data to PostgreSQL: {error}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def main():
    cfg = load_env()
    init_oracle_thick()

    owner = "SYSADM"
    table = "PS_STDNT_FA_TERM"
    pg_schema = "cs_raw"
    dsn_cx = build_connectorx_dsn(cfg['oracle'])
    pg_url = build_postgres_url(cfg["postgres"])

    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("No unique index found, exiting.")
        return

    oracle_columns = get_oracle_table_columns(cfg, owner, table)
    pg_table_name, pg_engine = create_postgres_table(pg_url, pg_schema, table, oracle_columns)

    expected_columns = [col[0] for col in oracle_columns]

    last_key = None

    async for batch_df, fetch_time in async_fetch_batches_keyset(
        dsn_cx,
        owner,
        table,
        unique_index_cols,
        batch_size=60000,
        max_workers=1,
        expected_columns=expected_columns,
    ):
        copy_start = time.monotonic()
        copy_batch_to_postgres(pg_engine, pg_table_name, batch_df, oracle_columns)
        copy_end = time.monotonic()
        copy_time = copy_end - copy_start

        mins_fetch, secs_fetch = divmod(fetch_time, 60)
        mins_copy, secs_copy = divmod(copy_time, 60)
        last_key = tuple(batch_df.iloc[-1][col] for col in unique_index_cols)
        print(f"✅ Processed batch with last key: {last_key}; Oracle fetch: {int(mins_fetch)}m {secs_fetch:.1f}s, Postgres copy: {int(mins_copy)}m {secs_copy:.1f}s")

if __name__ == "__main__":

    start_time = datetime.datetime.now()
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    asyncio.run(main())

    end_time = datetime.datetime.now()
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")