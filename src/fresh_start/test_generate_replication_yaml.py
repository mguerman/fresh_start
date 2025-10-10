import os
import sys
from pathlib import Path

from dotenv import load_dotenv

env_path = Path("/home/mguerman/fresh_start/.env")
if not env_path.exists():
    raise FileNotFoundError(f".env file not found: {env_path}")

load_dotenv(dotenv_path=env_path)  # Get environment loaded

print(f"After loading .env: DB_USER={os.getenv('DB_USER')}")

# Adjust sys.path to import from `defs` folder located one level up
sys.path.insert(0, str(Path(__file__).parent.parent / "defs"))

from fresh_start.defs.resources import PostgresResource
from fresh_start.defs.replication_mapping_generator import generate_replication_mapping_yaml


def main():
    try:
        postgres_resource = PostgresResource(
            db_user=os.environ["DB_USER"],
            db_password=os.environ["DB_PASSWORD"],
            db_host=os.environ["DB_HOST"],
            db_port=os.environ["DB_PORT"],
            db_name=os.environ["DB_NAME"],
        )
    except KeyError as e:
        raise RuntimeError(f"Missing required environment variable: {e}")

    # base_dir is two levels up from the current script (e.g., src/fresh_start)
    base_dir = Path(__file__).resolve().parent.parent

    # Adjusted template path with extra 'fresh_start' folder
    template_path = base_dir / "fresh_start" / "defs" / "replication_mapping_template.yaml"
    output_dir = base_dir / "fresh_start" / "defs"
    output_filename = "replication_mapping_generated.yaml"
    output_path = output_dir / output_filename

    print(f"Using template YAML at: {template_path}")
    print(f"Will save generated YAML to file: {output_path}")

    generated_path = generate_replication_mapping_yaml(
        pg_resource=postgres_resource,
        schema="xyz",  # replace with your schema name
        template_path=template_path,
        output_dir=output_dir,
        output_group_name="all",
        output_filename=output_filename,
    )
    print(f"Generated YAML file created at: {generated_path}")


if __name__ == "__main__":
    main()