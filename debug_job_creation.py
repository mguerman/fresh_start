"""Debug why jobs aren't being created."""

import os
from dotenv import load_dotenv

# Load environment variables FIRST
load_dotenv()

os.environ['DAGSTER_HOME'] = '/home/mguerman/.dagster'

from src.fresh_start.definitions import defs

print("=" * 80)
print("DEBUGGING JOB CREATION")
print("=" * 80)

# Get repository from definitions
repo = defs.get_repository_def()

# Get all jobs from repository
jobs = repo.get_all_jobs()
print(f"\n✅ Total jobs found: {len(jobs)}")

if jobs:
    print("\n📋 First 20 job names:")
    for i, job in enumerate(jobs[:20]):
        print(f"   {i+1}. {job.name}")
    if len(jobs) > 20:
        print(f"   ... and {len(jobs) - 20} more")
    
    # Search for d10 specifically
    print("\n🔍 Searching for 'd10' jobs:")
    d10_jobs = [j for j in jobs if 'd10' in j.name.lower()]
    if d10_jobs:
        for job in d10_jobs:
            print(f"   ✅ Found: {job.name}")
    else:
        print("   ❌ No jobs containing 'd10' found!")
        
        # Show what d jobs exist
        print("\n🔍 Jobs starting with 'replication_job_d':")
        d_jobs = [j for j in jobs if j.name.startswith('replication_job_d')]
        for job in d_jobs[:10]:
            print(f"   - {job.name}")
else:
    print("\n❌ NO JOBS FOUND!")

print("\n" + "=" * 80)