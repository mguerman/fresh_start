import os
import io
import csv
import asyncio
import csv
import re
import gc
import time
from datetime import datetime, date 
from contextlib import contextmanager

from typing import List, Optional, Tuple, Any, AsyncGenerator
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
    if isinstance(val, date):
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
    """
    Build WHERE condition for lexicographical keyset paging for Oracle.

    columns: list of column names
    last_key_values: tuple of last key values corresponding to columns

    Returns SQL WHERE string with embedded literals (no bind params).
    """
    if last_key_values is None:
        return ""

    conditions = []
    for i in range(len(columns)):
        equal_parts = " AND ".join(
            f"{columns[j]} = {sql_literal(last_key_values[j])}"
            for j in range(i)
        )
        greater_part = f"{columns[i]} > {sql_literal(last_key_values[i])}"
        if equal_parts:
            condition = f"({equal_parts} AND {greater_part})"
        else:
            condition = greater_part
        conditions.append(condition)

    where_clause = " OR ".join(conditions)
    return f"WHERE ({where_clause})"

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
        key = last_keys[worker_id]  # can be None for first call
        return await loop.run_in_executor(
            executor,
            fetch_batch_keyset,
            dsn,
            owner,
            table,
            order_columns,
            key,  # pass None okay for fetching first batch
            batch_size,
        )

    tasks = [fetch(worker_id) for worker_id in range(max_workers)]

    while finished_workers < max_workers:
        results = await asyncio.gather(*tasks)
        new_tasks = []
        yielded_any = False

        for worker_id, (df, fetch_duration) in enumerate(results):
            if df.empty:
                print(f"Worker {worker_id} finished: empty batch")
                finished_workers += 1
                last_keys[worker_id] = None
                continue

            n_rows = len(df)
            print(f"Worker {worker_id} fetched {n_rows} rows, fetch time: {fetch_duration:.2f}s")

            if expected_columns is not None:
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[expected_columns]

            new_last_key = tuple(df.iloc[-1][col] for col in order_columns)

            if last_keys[worker_id] == new_last_key:
                print(f"Worker {worker_id} no progress on last key, marking finished")
                finished_workers += 1
                last_keys[worker_id] = None
                continue

            if n_rows < batch_size:
                print(f"Worker {worker_id} fetched less than batch_size ({n_rows} < {batch_size}), marking finished")
                finished_workers += 1
                last_keys[worker_id] = None
                yielded_any = True
                yield df, fetch_duration
                continue

            # Update last key for next fetch
            last_keys[worker_id] = new_last_key

            yielded_any = True
            yield df, fetch_duration
            new_tasks.append(fetch(worker_id))

        if not yielded_any:
            print("No batches yielded in this iteration, finishing")
            break

        tasks = new_tasks
        gc.collect()

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

def copy_batch_to_postgres(engine: Engine, table_name: str, dataframe: pd.DataFrame, oracle_columns: list) -> None:
    if dataframe.empty:
        print("Empty DataFrame; skipping copy.")
        return

    dataframe = normalize_dataframe(dataframe, oracle_columns)
    buffer = io.StringIO()
    dataframe.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N', quoting=csv.QUOTE_MINIMAL)
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
        cursor.copy_from(buffer, raw_table_name, sep='\t', null='\\N', columns=columns)
        conn.commit()
    except Exception as error:
        print(f"Failed to copy data to PostgreSQL: {error}")
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
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
        max_workers=5,
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
    from datetime import datetime
    start_time = datetime.now()
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    asyncio.run(main())

    end_time = datetime.now()
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")