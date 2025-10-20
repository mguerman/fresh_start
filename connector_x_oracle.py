import os
from dotenv import load_dotenv
import oracledb
import connectorx as cx
import pandas as pd
import concurrent.futures
import psycopg2
import hashlib
import json
from datetime import datetime
from io import StringIO

# Load environment variables
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
            "dbname": os.environ["DB_NAME"],
        }
    }

# Initialize Oracle client
def init_oracle_thick(lib_dir="/etc/oracle_client/instantclient_23_26/"):
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    except Exception as e:
        print(f"Oracle client init failed: {e}")

# Build Oracle DSN
def build_dsn(user, password, host, port, service):
    return f"oracle+oci://{user}:{password}@{host}:{port}/{service}"

# Map Oracle types to Postgres
def map_oracle_type_to_postgres(oracle_type: str):
    oracle_type = oracle_type.upper()
    if oracle_type in ("VARCHAR2", "CHAR", "NVARCHAR2"):
        return "TEXT"
    elif oracle_type in ("NUMBER", "INTEGER", "INT"):
        return "INTEGER"
    elif oracle_type in ("DATE", "TIMESTAMP"):
        return "TIMESTAMP"
    elif oracle_type in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "FLOAT"
    elif oracle_type == "BOOLEAN":
        return "BOOLEAN"
    else:
        return "TEXT"

# Get unique index columns from Oracle
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
    indexes = [row[0] for row in cursor.fetchall()]

    for idx_name in indexes:
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
    print(f"No unique index found on table {table}.")
    return []

# Build hash expression for partitioning
def build_hash_expression(columns):
    return " || ".join(f"NVL(TO_CHAR({col}), '')" for col in columns)

# Create Postgres table with mapped types
def create_postgres_table(df, pg_cfg, table_name):
    conn = psycopg2.connect(**pg_cfg)
    cursor = conn.cursor()

    col_defs = ", ".join([f"{col.lower()} TEXT" for col in df.columns if col not in ["dl_inserteddate", "dl_insertedby", "row_hash"]])
    extra_defs = "dl_inserteddate TIMESTAMP, dl_insertedby TEXT, row_hash TEXT"
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS mguerman.{table_name} (
            {col_defs},
            {extra_defs}
        );
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Copy data to Postgres with explicit columns
def copy_to_postgres(df, pg_cfg, table_name):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)

    conn = psycopg2.connect(**pg_cfg)
    cursor = conn.cursor()
    try:
        columns = [col.lower() for col in df.columns]
        cursor.copy_from(buffer, f'cs_raw.{table_name}', sep='\t', null='', columns=columns)
        conn.commit()
        print(f"✅ Partition committed to Postgres: mguerman.{table_name}")
    except Exception as e:
        print(f"❌ COPY failed: {e}")
    finally:
        cursor.close()
        conn.close()

# Process a single partition
def process_partition(args):
    dsn, owner, table, columns, num_partitions, partition_id, pg_cfg, output_dir = args
    init_oracle_thick()

    hash_expr = build_hash_expression(columns)
    max_bucket = num_partitions - 1

    query = f"""
        SELECT * FROM {owner}.{table}
        WHERE ORA_HASH({hash_expr}, {max_bucket}) = {partition_id}
    """
    print(f"🔄 Fetching partition {partition_id} of {num_partitions}")
    try:
        df = cx.read_sql(dsn, query)
        print(f"📦 Partition {partition_id} fetched {len(df)} rows.")
    except Exception as e:
        print(f"❌ Error fetching partition {partition_id}: {e}")
        return

    if df.empty:
        print(f"⚠️ Partition {partition_id} is empty.")
        return

    df["dl_inserteddate"] = pd.Timestamp.now()
    df["dl_insertedby"] = "system"
    df["row_hash"] = df.apply(lambda row: hashlib.md5(json.dumps(row.to_dict(), default=str).encode()).hexdigest(), axis=1)

    if partition_id == 0:
        create_postgres_table(df, pg_cfg, table)

    copy_to_postgres(df, pg_cfg, table)

    if output_dir:
        output_file = f"{output_dir}/partition_{partition_id:03d}.csv"
        df.to_csv(output_file, index=False)
        print(f"📁 Partition {partition_id} saved to {output_file}")

# Main orchestration
def main():
    cfg = load_env()
    init_oracle_thick()

    owner = 'SYSADM'.upper()
    table = 'PS_AUDIT_CARTM'.upper()
    output_dir = "partitions_output"
    num_partitions = 15
    max_workers = min(10, num_partitions)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    num_partitions = 10  # Adjust as needed for concurrency/load balancing

    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("❌ No unique index found, exiting.")
        return

    args = [
        (build_dsn(**cfg["oracle"]), owner, table, unique_index_cols, num_partitions, pid, cfg["postgres"], output_dir)
        for pid in range(num_partitions)
    ]

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_partition, args)

    print(f"✅ Data fetching complete. CSV files are in '{output_dir}' directory.")

# Entry point
if __name__ == "__main__":
    start_time = datetime.now()
    print(f"🚀 Script started at: {start_time}")
    main()
    end_time = datetime.now()
    print(f"✅ Script completed at: {end_time}")
    print(f"⏱️ Total execution time: {end_time - start_time}")
