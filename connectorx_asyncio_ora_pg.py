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

from typing import List, Optional, Tuple, Any, AsyncGenerator, Generator
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
    print("Oracle thick client initialized") 

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

def fetch_batch_by_rowid(
    dsn: str,
    owner: str,
    table: str,
    last_rowid: Optional[str],
    batch_size: int,
) -> Tuple[pd.DataFrame, float]:
    """
    Fetch a batch of rows from Oracle using ROWID-based pagination.

    Args:
        dsn: Oracle DSN/connection string.
        owner: Schema owner name.
        table: Table name.
        last_rowid: ROWID of last fetched row or None for first batch.
        batch_size: Number of rows to fetch.

    Returns:
        Tuple containing DataFrame and fetch duration in seconds.

    Raises:
        Exception on query failure.
    """
    try:
        where_clause = "WHERE ROWID > :last_rowid" if last_rowid else ""
        params = {'last_rowid': last_rowid} if last_rowid else {}

        sql = f"""
            SELECT ROWID, {owner}.{table}.*
            FROM {owner}.{table}
            {where_clause}
            ORDER BY ROWID
            FETCH FIRST {batch_size} ROWS ONLY
        """

        start_time = time.monotonic()
        df = cx.read_sql(dsn, sql, parameters=params)
        duration = time.monotonic() - start_time

        return df, duration

    except Exception:
        # Propagate any exception
        raise

    # Unreachable, added to satisfy type checkers
    return pd.DataFrame(), 0.0

async def async_fetch_batches_rowid(
    dsn: str,
    owner: str,
    table: str,
    batch_size: int = 1000,
    max_workers: int = 5,
    expected_columns: Optional[List[str]] = None
) -> AsyncGenerator[Tuple[pd.DataFrame, float], None]:
    """
    Async generator fetching batches concurrently from Oracle using ROWID-based pagination.

    Args:
        dsn: Oracle connection string.
        owner: Table owner/schema.
        table: Table name.
        batch_size: Number of rows per batch.
        max_workers: Number of concurrent workers.
        expected_columns: List of columns to enforce presence/order in each DataFrame batch.

    Yields:
        Tuple of DataFrame (batch) and fetch duration in seconds.
    """
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    last_rowids: List[Optional[str]] = [None] * max_workers
    finished_workers = 0

    async def fetch(worker_id: int) -> Tuple[pd.DataFrame, float]:
        return await loop.run_in_executor(
            executor,
            fetch_batch_by_rowid,
            dsn,
            owner,
            table,
            last_rowids[worker_id],
            batch_size,
        )

    tasks = [fetch(worker_id) for worker_id in range(max_workers)]

    while finished_workers < max_workers:
        results = await asyncio.gather(*tasks)
        new_tasks = []
        yielded_any = False

        for worker_id, (df, fetch_time) in enumerate(results):
            if df.empty:
                print(f"Worker {worker_id} finished: no more data")
                finished_workers += 1
                last_rowids[worker_id] = None
                continue

            if expected_columns:
                missing_cols = [col for col in expected_columns if col not in df.columns]
                if missing_cols:
                    missing_df = pd.DataFrame({col: pd.NA for col in missing_cols}, index=df.index)
                    df = pd.concat([df, missing_df], axis=1)
                df = df[expected_columns]

            new_last_rowid = df.iloc[-1]['ROWID']
            if last_rowids[worker_id] == new_last_rowid:
                print(f"Worker {worker_id} no progress on ROWID, stopping")
                finished_workers += 1
                last_rowids[worker_id] = None
                continue

            last_rowids[worker_id] = new_last_rowid
            if len(df) < batch_size:
                finished_workers += 1

            yielded_any = True
            yield df, fetch_time

            new_tasks.append(fetch(worker_id))

        if not yielded_any:
            print("No batches yielded; finishing.")
            break

        tasks = new_tasks
        gc.collect()

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
    print("Starting main()")
    cfg = load_env()
    print("Loaded environment")

    init_oracle_thick()
    print("Oracle Client ready")

    owner = "SYSADM"
    table = "PS_STDNT_FA_TERM"
    pg_schema = "cs_raw"

    dsn_cx = build_connectorx_dsn(cfg['oracle'])
    print(f"Built Oracle DSN: {dsn_cx}")

    pg_url = build_postgres_url(cfg["postgres"])
    print(f"Built Postgres URL: {pg_url}")

    oracle_columns = get_oracle_table_columns(cfg, owner, table)
    print(f"Got Oracle columns: {[col[0] for col in oracle_columns]}")

    pg_table_name, pg_engine = create_postgres_table(pg_url, pg_schema, table, oracle_columns)
    print(f"Postgres table initialized: {pg_table_name}")

    expected_columns = [col[0].lower() for col in oracle_columns]

    # Start async fetching batches
    print("Starting async fetch batches by ROWID")
    async for batch_df, fetch_time in async_fetch_batches_rowid(
        dsn_cx,
        owner,
        table,
        batch_size=60000,
        max_workers=5,
        expected_columns=expected_columns,
    ):
        print(f"Fetched batch of {len(batch_df)} rows, fetching took {fetch_time:.2f}s")
        copy_start = time.monotonic()
        copy_batch_to_postgres(pg_engine, pg_table_name, batch_df, oracle_columns)
        copy_end = time.monotonic()
        print(f"Copied batch to Postgres in {copy_end - copy_start:.2f}s")

    print("All batches fetched and copied, main() complete")
    
if __name__ == "__main__":
    from datetime import datetime
    start_time = datetime.now()
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    asyncio.run(main())

    end_time = datetime.now()
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")