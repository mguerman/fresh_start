import asyncio
import asyncpg
import oracledb
from datetime import datetime
import concurrent.futures

# Enable thick mode
#oracledb.init_oracle_client()
oracledb.init_oracle_client(lib_dir=r"C:\ORACLE\instantclient-basic-windows.x64-23.9.0.25.07\instantclient_23_9")

async def get_distinct_combinations():
    """Get distinct INSTITUTION, ACAD_CAREER, STRM combinations"""
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
        SELECT DISTINCT INSTITUTION, ACAD_CAREER, STRM
        FROM "SYSADM"."PS_STDNT_ENRL"
        ORDER BY INSTITUTION, ACAD_CAREER, STRM
    """)
    
    combinations = cursor.fetchall()
    cursor.close()
    oracle_conn.close()
    
    print(f"Found {len(combinations)} distinct combinations: {datetime.now()}")
    return combinations

async def process_combination(institution, acad_career, strm, pg_pool):
    """Process a single combination of institution, career, and term"""
    try:
        print(f"Processing {institution}-{acad_career}-{strm}: {datetime.now()}")
        
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
            FROM "SYSADM"."PS_STDNT_ENRL" pse
            WHERE pse.INSTITUTION = :1
            AND pse.ACAD_CAREER = :2
            AND pse.STRM = :3
        """, (institution, acad_career, strm))
        
        columns = [col[0].lower() for col in cursor.description]
        rows = cursor.fetchall()
        
        cursor.close()
        oracle_conn.close()
        
        if not rows:
            print(f"No data for {institution}-{acad_career}-{strm}")
            return 0
        
        # Insert into PostgreSQL
        async with pg_pool.acquire() as pg_conn:
            # Create the insert query
            placeholders = ','.join(['$' + str(i) for i in range(1, len(columns) + 1)])
            insert_query = f"""
                INSERT INTO sad.ps_stdnt_enrl ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            # Insert all rows for this combination
            await pg_conn.executemany(insert_query, rows)
        
        print(f"Completed {institution}-{acad_career}-{strm}: {len(rows)} rows: {datetime.now()}")
        return len(rows)
        
    except Exception as e:
        print(f"Error processing {institution}-{acad_career}-{strm}: {e}")
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
        user="sdelano",
        password="snaplogic2025",
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