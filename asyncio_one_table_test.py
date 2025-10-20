import asyncio
import asyncpg
import oracledb
from datetime import datetime
from decimal import Decimal

# Enable thick mode
#oracledb.init_oracle_client()
oracledb.init_oracle_client(lib_dir="/etc/oracle_client/instantclient_23_26/")

def map_oracle_type_to_postgres(oracle_type: str):
    oracle_type = oracle_type.upper()
    if oracle_type in ("VARCHAR2", "CHAR", "NVARCHAR2"):
        return str
    elif oracle_type in ("NUMBER", "INTEGER", "INT"):
        return lambda x: str(x) if x is not None else None
    elif oracle_type in ("DATE", "TIMESTAMP"):
        return lambda x: x.isoformat() if isinstance(x, datetime) else str(x) if x is not None else None
    elif oracle_type in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return lambda x: str(x) if x is not None else None
    elif oracle_type == "BOOLEAN":
        return lambda x: str(bool(x)) if x is not None else None
    else:
        return str  # Fallback to string

async def get_distinct_combinations():
    """Get distinct EMPLID,INSTITUTION,STRM combinations"""
    print(f"Getting distinct combinations: {datetime.now()}")
    
    oracle_conn = oracledb.connect(
        user="BU_SNAP_EMPH", 
        password="Change.Pass.BU123!!!", 
        host="10.63.133.38", 
        port=1521, 
        service_name="buprd1.buocisubpridbph.buocivcnphxdr.oraclevcn.com"
    )
    
    cursor = oracle_conn.cursor()
    cursor.execute("""
        SELECT DISTINCT EMPLID,INSTITUTION,STRM
        FROM "SYSADM"."PS_STDNT_FA_TERM"
        ORDER BY EMPLID,INSTITUTION,STRM
    """)
    
    combinations = cursor.fetchall()
    cursor.close()
    oracle_conn.close()
    
    print(f"Found {len(combinations)} distinct combinations: {datetime.now()}")
    return combinations

async def process_combination(emplid, institution, strm, pg_pool):
    """Process a single combination of emplid, institution, and term"""
    try:
        print(f"Processing {emplid}-{institution}-{strm}: {datetime.now()}")
        
        # Oracle query for this combination
        oracle_conn = oracledb.connect(
            user="BU_SNAP_EMPH", 
            password="Change.Pass.BU123!!!", 
            host="10.63.133.38", 
            port=1521, 
            service_name="buprd1.buocisubpridbph.buocivcnphxdr.oraclevcn.com"
        )
        
        cursor = oracle_conn.cursor()
        cursor.execute("""
            SELECT pse.*
            FROM "SYSADM"."PS_STDNT_FA_TERM" pse
            WHERE pse.EMPLID = :1
            AND pse.INSTITUTION = :2
            AND pse.STRM = :3
        """, (emplid, institution, strm))
        
        # Extract column names and type mappers
        columns = [col[0].lower() for col in cursor.description]
        type_mappers = [map_oracle_type_to_postgres(col[1].__name__) for col in cursor.description]
        rows = cursor.fetchall()

        cursor.close()
        oracle_conn.close()

        if not rows:
            print(f"No data for {emplid}-{institution}-{strm}")
            return 0

        # Convert each row using mapped types
        def convert_row(row):
            return [
                type_mappers[i](val) if callable(type_mappers[i]) and val is not None else val
                for i, val in enumerate(row)
            ]

        converted_rows = [convert_row(row) for row in rows]

        # Insert into PostgreSQL
        async with pg_pool.acquire() as pg_conn:
            placeholders = ','.join([f"${i}" for i in range(1, len(columns) + 1)])
            insert_query = f"""
                INSERT INTO mguerman.ps_stdnt_fa_term ({', '.join(columns)})
                VALUES ({placeholders})
            """
            await pg_conn.executemany(insert_query, converted_rows)

        print(f"Completed {emplid}-{institution}-{strm}: {len(rows)} rows: {datetime.now()}")
        return len(rows)

    except Exception as e:
        print(f"Error processing {emplid}-{institution}-{strm}: {e}")
        return 0

async def main():
    print(f"Program started: {datetime.now()}")
    
    # Get distinct combinations
    combinations = await get_distinct_combinations()
    
    # Create PostgreSQL connection pool
    pg_pool = await asyncpg.create_pool(
        host="ist-pg-dl-dev01.bu.edu",
        port=5432,
        database="dl_db",
        user="mguerman",
        password="um1MNsWOXSnTtdFi/ziIVg==",
        min_size=5,
        max_size=20
    )

    # Process combinations concurrently (limit concurrency to avoid overwhelming DB)
    semaphore = asyncio.Semaphore(5)  # Max 5 concurrent operations
    
    async def process_with_semaphore(combo):
        async with semaphore:
            return await process_combination(combo[0], combo[1], combo[2], pg_pool)
    
    print(f"Starting async processing: {datetime.now()}")
    
    # Process all combinations concurrently
    tasks = [process_with_semaphore(combo) for combo in combinations]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Summary
    total_rows = sum(r for r in results if isinstance(r, int))
    errors = [r for r in results if isinstance(r, Exception)]
    
    await pg_pool.close()
    
    print(f"Program completed: {datetime.now()}")
    print(f"Total combinations processed: {len(combinations)}")
    print(f"Total rows inserted: {total_rows}")
    print(f"Errors: {len(errors)}")

if __name__ == "__main__":
    # Install asyncpg if not installed: pip install asyncpg
    asyncio.run(main())