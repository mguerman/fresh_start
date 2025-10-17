import os
from dotenv import load_dotenv
import oracledb
import connectorx as cx
import pandas as pd
import concurrent.futures

def load_env():
    load_dotenv()
    return {
        "user": os.environ["ORACLE_DB_USER"],
        "password": os.environ["ORACLE_DB_PASSWORD"],
        "host": os.environ["ORACLE_DB_HOST"],
        "port": int(os.environ["ORACLE_DB_PORT"]),
        "service": os.environ["ORACLE_DB_SERVICE"],
    }

def init_oracle_thick(lib_dir="/etc/oracle_client/instantclient_23_26/"):
    print(f"Initializing Oracle thick client from {lib_dir}")
    oracledb.init_oracle_client(lib_dir=lib_dir)

def build_dsn(user, password, host, port, service):
    return f"oracle+oci://{user}:{password}@{host}:{port}/{service}"

def get_unique_index_columns(cfg, owner, table):
    dsn = f"{cfg['host']}:{cfg['port']}/{cfg['service']}"
    conn = oracledb.connect(user=cfg['user'], password=cfg['password'], dsn=dsn)
    cursor = conn.cursor()

    sql_indexes = """
        SELECT INDEX_NAME 
        FROM ALL_INDEXES 
        WHERE TABLE_OWNER = :ownr AND TABLE_NAME = :tbl AND UNIQUENESS = 'UNIQUE'
        ORDER BY INDEX_NAME
    """
    cursor.execute(sql_indexes, {'ownr': owner, 'tbl': table})

    indexes = [row[0] for row in cursor.fetchall()]
    for idx_name in indexes:
        sql_cols = """
            SELECT COLUMN_NAME 
            FROM ALL_IND_COLUMNS 
            WHERE INDEX_OWNER = :ownr AND INDEX_NAME = :idx
            ORDER BY COLUMN_POSITION
        """
        cursor.execute(sql_cols, {'ownr': owner, 'idx': idx_name})
        cols = [row[0] for row in cursor.fetchall()]
        if cols:
            cursor.close()
            conn.close()
            print(f"Using unique index '{idx_name}' with columns {cols} for hashing partition")
            return cols

    cursor.close()
    conn.close()
    print("No unique index found on table.")
    return []

def build_hash_expression(columns):
    # Convert all columns to string with NVL(TO_CHAR()) and concatenate for ORA_HASH
    def col_expr(col):
        return f"NVL(TO_CHAR({col}), '')"
    return " || ".join(col_expr(col) for col in columns)

def fetch_partition_hash(dsn, owner, table, columns, num_partitions, partition_id, output_dir):
    hash_expr = build_hash_expression(columns)
    max_bucket = num_partitions - 1

    query = f"""
        SELECT * FROM {owner}.{table}
        WHERE ORA_HASH({hash_expr}, {max_bucket}) = {partition_id}
    """
    print(f"Fetching partition {partition_id} of {num_partitions} (hash = {partition_id})")
    df = cx.read_sql(dsn, query)
    print(f"Partition {partition_id} fetched {len(df)} rows.")

    output_file = f"{output_dir}/partition_{partition_id:03d}.csv"
    df.to_csv(output_file, index=False)
    print(f"Partition {partition_id} saved to {output_file}")

def main():
    cfg = load_env()
    init_oracle_thick()

    owner = 'SYSADM'  # update to your schema
    table = 'PS_STDNT_FA_TERM'  # update to your table

    dsn = build_dsn(cfg['user'], cfg['password'], cfg['host'], cfg['port'], cfg['service'])
    output_dir = "partitions_output"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    num_partitions = 50  # Adjust as needed for concurrency/load balancing

    unique_index_cols = get_unique_index_columns(cfg, owner, table)
    if not unique_index_cols:
        print("No unique index found, exiting.")
        return

    args = [
        (dsn, owner, table, unique_index_cols, num_partitions, pid, output_dir)
        for pid in range(num_partitions)
    ]

    max_workers = min(10, num_partitions)  # tune concurrency here
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda p: fetch_partition_hash(*p), args)

    print(f"Data fetching complete. CSV files are in '{output_dir}' directory.")

if __name__ == "__main__":
    main()