import os
import time
import logging
import re
from dotenv import load_dotenv
from pathlib import Path

from dagster import Definitions, define_asset_job
from src.fresh_start.defs.util import load_enabled_groups, yaml_path

# Load environment variables from .env file
load_dotenv()

BATCH_SIZE = 100

# Optional prefix for filtering groups, can be None or empty to select all groups
GROUP_PREFIX = None  # e.g. 'g' or 'h' or None to process all groups


# -------------------- Logging Setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -------------------- Utility Functions --------------------

def filter_groups(groups, prefix: str | None = None):
    """Filter group names by prefix if provided, else return all."""
    if prefix:
        pattern = re.compile(rf"^{re.escape(prefix)}", re.IGNORECASE)
        return [g for g in groups if pattern.match(g.get("name", ""))]
    else:
        return groups


def chunk_list(lst, size):
    """Yield successive chunks from a list."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def create_postgres_resource():
    """Initialize PostgresResource from env vars."""
    from src.fresh_start.defs.resources import PostgresResource  # Import here to avoid circular

    return PostgresResource(
        db_user=os.environ["DB_USER"],
        db_password=os.environ["DB_PASSWORD"],
        db_host=os.environ["DB_HOST"],
        db_port=os.environ["DB_PORT"],
        db_name=os.environ["DB_NAME"],
    )


def create_oracle_resource():
    """Initialize OracleResource from env vars."""
    from src.fresh_start.defs.resources import OracleResource  # Import here to avoid circular

    return OracleResource(
        db_user=os.environ["ORACLE_DB_USER"],
        db_password=os.environ["ORACLE_DB_PASSWORD"],
        db_host=os.environ["ORACLE_DB_HOST"],
        db_port=os.environ["ORACLE_DB_PORT"],
        db_service=os.environ["ORACLE_DB_SERVICE"],
    )


# -------------------- Main Asset Loading Function --------------------
def build_assets_from_yaml_for_groups(yaml_path: Path, groups_list):
    """
    Wrapper that calls your existing asset builder function
    but filtered by group list.
    """
    from src.fresh_start.defs.assets import build_assets_from_yaml

    group_names = [g.get("name") for g in groups_list]
    return build_assets_from_yaml(str(yaml_path), group_names)


# -------------------- Batch Materialization --------------------
def materialize_batch(batch_num, total_batches, assets_batch, resources):
    """
    Materialize a batch of assets via dynamically built Dagster job.
    """
    logging.info(
        f"Batch {batch_num}/{total_batches} started. Materializing {len(assets_batch)} assets..."
    )
    start_time = time.time()

    job_name = f"batch_{batch_num}"
    job_def = define_asset_job(name=job_name, selection=assets_batch)

    defs = Definitions(assets=assets_batch, resources=resources, jobs=[job_def])

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
    logging.info(f"Loading YAML from: {yaml_path}")
    all_groups = load_enabled_groups(yaml_path, prefix="h")
    filtered_groups = filter_groups(all_groups, GROUP_PREFIX)
    logging.info(f"Groups to process: {[g.get('name') for g in filtered_groups]}")

    # Load assets only for filtered groups
    all_assets = build_assets_from_yaml_for_groups(yaml_path, filtered_groups)
    assets = list(all_assets)

    resources = {
        "postgres": create_postgres_resource(),
        "oracle": create_oracle_resource(),
        # If you use your group_resource, set it here or per-job run config
    }

    batch_size = BATCH_SIZE
    total_batches = (len(assets) + batch_size - 1) // batch_size

    logging.info(f"Starting materialization of {len(assets)} assets in {total_batches} batches")

    for batch_num, assets_batch in enumerate(chunk_list(assets, batch_size), start=1):
        success = materialize_batch(batch_num, total_batches, assets_batch, resources)
        if not success:
            logging.error(f"Batch {batch_num} failed. Stopping further processing.")
            break

    logging.info("Materialization script completed.")


# -------------------- Script Execution --------------------
if __name__ == "__main__":
    main()