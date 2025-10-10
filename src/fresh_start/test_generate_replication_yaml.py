import os
import sys
from pathlib import Path
from typing import Set
from dotenv import load_dotenv

# Define constants for paths
ENV_PATH = Path("/home/mguerman/fresh_start/.env")
BASE_DIR = Path(__file__).resolve().parent.parent
DEFS_DIR = BASE_DIR / "fresh_start" / "defs"
EXCLUSIONS_FILE = DEFS_DIR / "table_exclusions.txt"
TEMPLATE_FILE = DEFS_DIR / "replication_mapping_template.yaml"

# Check and load environment variables
if not ENV_PATH.exists():
    raise FileNotFoundError(f".env file not found at expected location: {ENV_PATH}")

load_dotenv(dotenv_path=ENV_PATH)
print(f"Loaded .env: DB_USER={os.getenv('DB_USER')}")

# Adjust sys.path to import from `defs` folder located one level up
sys.path.insert(0, str(DEFS_DIR))

from fresh_start.defs.resources import PostgresResource
from fresh_start.defs.replication_mapping_generator import generate_replication_mapping_groups_yaml


def load_exclusions(exclusions_path: Path) -> Set[str]:
    """
    Load excluded table names from the exclusions file.
    Assumes one table name per line; lines starting with '#' and blank lines are ignored.
    """
    if not exclusions_path.exists():
        print(f"Exclusions file not found: {exclusions_path}. Proceeding with empty exclusions set.")
        return set()

    with exclusions_path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return set(lines)


def main() -> None:
    try:
        pg_resource = PostgresResource(
            db_user=os.environ["DB_USER"],
            db_password=os.environ["DB_PASSWORD"],
            db_host=os.environ["DB_HOST"],
            db_port=os.environ["DB_PORT"],
            db_name=os.environ["DB_NAME"],
        )
    except KeyError as missing_key:
        raise RuntimeError(f"Missing required environment variable: {missing_key}") from None

    exclusions = load_exclusions(EXCLUSIONS_FILE)
    print(f"Loaded exclusions: {exclusions}")

    print("Generating combined replication mapping YAML file with 'all' and 'excluded' groups...")
    generated_path = generate_replication_mapping_groups_yaml(
        pg_resource=pg_resource,
        schema=None,  # Let the function extract schema from template
        template_path=TEMPLATE_FILE,
        output_dir=DEFS_DIR,
        output_filename="replication_mapping_generated.yaml",
        excluded_tables=exclusions,
    )
    print(f"Generated combined YAML file at: {generated_path}")


if __name__ == "__main__":
    main()