import asyncio
import psycopg
from psycopg_pool import AsyncConnectionPool
import oracledb
from datetime import datetime
import concurrent.futures
import io
import sys
 
   try:
        cursor = oracle_conn.cursor()
        cursor.execute("""
            SELECT pse.*
            FROM "SYSADM"."PS_STDNT_ENRL" pse
            WHERE pse.INSTITUTION = :1
            AND pse.ACAD_CAREER = :2
            AND pse.STRM = :3
        """, (institution, acad_career, strm))
       
        columns = [col[0].lower() for col in cursor.description]
       
        # Use fetchmany instead of fetchall for memory efficiency
        all_rows = []
        batch_size = 10000
       
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_rows.extend(rows)
       
        cursor.close()
        return columns, all_rows
 