import os
import asyncio
import asyncpg
import oracledb
import hashlib
import json
import time
import traceback
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Initialize Oracle thick client
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

def get_unique_index_columns(cfg, owner, table):
    oracle_cfg = cfg["oracle"]
    dsn = f"{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service']}"
    conn = oracledb.connect(user=oracle_cfg['user'], password=oracle_cfg['password'], dsn=dsn)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT INDEX_NAME 
        FROM ALL_INDEXES 
        WHERE TABLE_OWNER = :ownr AND TABLE_NAME = :tbl AND UNIQUENESS = 'UNIQUE'
        ORDER BY INDEX_NAME
    """, {'ownr': owner, 'tbl': table})

    for idx_name, in cursor.fetchall():
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM ALL_IND_COLUMNS 
            WHERE INDEX_OWNER = :ownr AND INDEX_NAME = :idx
            ORDER BY COLUMN_POSITION
        """, {'ownr': owner, 'idx': idx_name})
        cols = [row[0] for row in cursor.fetchall()]
        if cols:
            cursor.close()
            conn.close()
            print(f"Using unique index '{idx_name}' with columns {cols} for hashing partition")
            return cols

    cursor.close()
    conn.close()
    print("No unique index found.")
    return []

def build_hash_expression(columns):
    return " || ".join([f"NVL(TO_CHAR({col}), '')" for col in columns])

def fetch_partition_from_oracle(cfg, owner, table, columns, num_partitions, partition_id):
    oracle_cfg = cfg["oracle"]
    dsn = f"{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service']}"
    conn = oracledb.connect(user=oracle_cfg['user'], password=oracle_cfg['password'], dsn=dsn)
    cursor = conn.cursor()

    hash_expr = build_hash_expression(columns)
    max_bucket = num_partitions - 1

    query = f"""
        SELECT * FROM {owner}.{table}
        WHERE ORA_HASH({hash_expr}, {max_bucket}) = {partition_id}
    """
    cursor.execute(query)
    columns = [col[0].lower() for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return columns, rows

async def insert_partition_to_postgres(columns, rows, pg_cfg, partition_id, create_table=False):
    if not rows:
        print(f"[Partition {partition_id}] No rows to insert.")
        return

    MAX_ATTEMPTS = 5
    attempt = 0
    delay = 2  # seconds

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
                col_defs = ", ".join([f"{col} TEXT" for col in columns])
                extra_defs = "dl_inserteddate TIMESTAMP, dl_insertedby TEXT, row_hash TEXT"
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS cs_raw.ps_stdnt_fa_term (
                        {col_defs},
                        {extra_defs}
                    );
                """
                print(f"[Partition {partition_id}] Executing SQL:\n{create_sql}")
                await conn.execute(create_sql, timeout=30)

            now = datetime.utcnow()
            enriched_rows = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                row_hash = hashlib.md5(json.dumps(row_dict, default=str).encode()).hexdigest()
                enriched_rows.append(tuple(row) + (now, "system", row_hash))

            placeholders = ', '.join([f"${i+1}" for i in range(len(columns) + 3)])
            insert_sql = f"""
                INSERT INTO cs_raw.ps_stdnt_fa_term ({', '.join(columns)}, dl_inserteddate, dl_insertedby, row_hash)
                VALUES ({placeholders})
            """
            await conn.executemany(insert_sql, enriched_rows)
            await conn.close()
            print(f"[Partition {partition_id}] Inserted {len(rows)} rows.")
            return  # success, exit loop

        except Exception as e:
            attempt += 1
            print(f"[Partition {partition_id}] Attempt {attempt} failed: {e}")
            if attempt >= MAX_ATTEMPTS:
                raise RuntimeError(f"Failed to load data into cs_raw.ps_stdnt_fa_term after {MAX_ATTEMPTS} attempts: {e}")
            print(f"[Partition {partition_id}] Sleeping for {delay}s before retry...")
            time.sleep(delay)
            delay *= 2

semaphore = asyncio.Semaphore(10)

async def process_partition(cfg, owner, table, columns, num_partitions, partition_id, pg_cfg, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        columns, rows = await loop.run_in_executor(None, fetch_partition_from_oracle, cfg, owner, table, columns, num_partitions, partition_id)
        await insert_partition_to_postgres(columns, rows, pg_cfg, partition_id, create_table=(partition_id == 0))

async def main():
    cfg = load_env()
    owner = 'SYSADM'
    table = 'PS_STDNT_FA_TERM'
    num_partitions = 50
    max_concurrent = 10

    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("No unique index found, exiting.")
        return

    semaphore = asyncio.Semaphore(max_concurrent)

    # Run partition 0 first to create the table
    await process_partition(cfg, owner, table, unique_index_cols, num_partitions, 0, cfg["postgres"], semaphore)

    # Then run the rest concurrently
    tasks = [
        process_partition(cfg, owner, table, unique_index_cols, num_partitions, pid, cfg["postgres"], semaphore)
        for pid in range(1, num_partitions)
    ]
    await asyncio.gather(*tasks)
    print("✅ All partitions processed and inserted into PostgreSQL.")


if __name__ == "__main__":
    start_time = time.time()
    try:
        asyncio.run(main())
    except Exception as e:
        print("An error occurred during execution:")
        traceback.print_exc()
    finally:
        end_time = time.time()
        print(f"Execution started at: {start_time}")
        print(f"Execution ended at: {end_time}")
        print(f"Total execution time: {end_time - start_time:.2f} seconds")
