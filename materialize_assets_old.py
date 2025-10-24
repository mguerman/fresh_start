import os
import time
import logging
from dotenv import load_dotenv
from dagster import Definitions, define_asset_job

# Load environment variables from .env file
load_dotenv()

# Import assets and resources
from src.fresh_start.definitions import all_assets, PostgresResource, OracleResource

# -------------------- Logging Setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -------------------- Utility Functions --------------------
def chunk_list(lst, size):
    """
    Yield successive chunks from a list.
    Useful for batching assets.
    """
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def create_postgres_resource():
    """
    Initialize PostgresResource from environment variables.
    """
    return PostgresResource(
        db_user=os.environ["DB_USER"],
        db_password=os.environ["DB_PASSWORD"],
        db_host=os.environ["DB_HOST"],
        db_port=os.environ["DB_PORT"],
        db_name=os.environ["DB_NAME"],
    )

def create_oracle_resource():
    """
    Initialize OracleResource from environment variables.
    """
    return OracleResource(
        db_user=os.environ["ORACLE_DB_USER"],
        db_password=os.environ["ORACLE_DB_PASSWORD"],
        db_host=os.environ["ORACLE_DB_HOST"],
        db_port=os.environ["ORACLE_DB_PORT"],
        db_service=os.environ["ORACLE_DB_SERVICE"],
    )

# -------------------- Batch Execution --------------------
def materialize_batch(batch_num, total_batches, assets_batch, resources):
    """
    Materialize a batch of Dagster assets using a dynamically defined job.
    """
    logging.info(f"Batch {batch_num}/{total_batches} started. Materializing {len(assets_batch)} assets...")
    start_time = time.time()

    job_name = f"batch_{batch_num}"
    job_def = define_asset_job(name=job_name, selection=assets_batch)

    defs = Definitions(
        assets=assets_batch,
        resources=resources,
        jobs=[job_def]
    )

    try:
        resolved_job = defs.resolve_job_def(job_name)
        result = resolved_job.execute_in_process(resources=resources)
    except Exception as ex:
        logging.error(f"Batch {batch_num} failed due to exception: {ex}")
        logging.exception(ex)
        return False

    if result.success:
        elapsed = time.time() - start_time
        logging.info(f"Batch {batch_num} completed successfully in {elapsed:.1f} seconds.")
        return True
    else:
        logging.warning(f"Batch {batch_num} failed to materialize some assets.")
        return False

# -------------------- Main Entry Point --------------------
def main():
    """
    Entry point for batch materialization.
    Initializes resources and executes assets in batches.
    """
    resources = {
        "postgres": create_postgres_resource(),
        "oracle": create_oracle_resource(),
    }

    assets = list(all_assets)
    batch_size = 100
    total_batches = (len(assets) + batch_size - 1) // batch_size

    logging.info(f"Starting materialization of {len(assets)} assets in {total_batches} batches of {batch_size}.")

    for batch_num, assets_batch in enumerate(chunk_list(assets, batch_size), start=1):
        materialize_batch(batch_num, total_batches, assets_batch, resources)

    logging.info("All batches completed.")

# -------------------- Script Execution --------------------
if __name__ == "__main__":
    main()
