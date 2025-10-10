import yaml
from pathlib import Path
from typing import List
from sqlalchemy import text
from .resources import PostgresResource

def get_foreign_tables(pg_resource: PostgresResource, schema_name: str) -> List[str]:
    """
    Query PostgreSQL via the resource to get foreign table names in the given schema.
    """
    query = """
    SELECT c.relname AS foreign_table_name
    FROM pg_foreign_table ft
    JOIN pg_class c ON ft.ftrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = :schema_name
    ORDER BY foreign_table_name;
    """
    session = pg_resource.get_session()
    try:
        result = session.execute(text(query), {"schema_name": schema_name})
        return [row.foreign_table_name for row in result.fetchall()]
    finally:
        session.close()


def generate_replication_mapping_yaml(
    pg_resource: PostgresResource,
    schema: str,
    template_path: Path,
    output_dir: Path,
    output_filename: str = "replication_mapping_generated.yaml",
    output_group_name: str = "all",
) -> Path:
    """
    Generate a replication_mapping YAML using foreign table list from Postgres and the given template.

    Args:
        pg_resource: Instance of PostgresResource for DB access.
        schema: Schema name to query foreign tables from.
        template_path: Path to existing YAML template file.
        output_dir: Directory to save generated YAML file.
        output_filename: File name for output YAML.
        output_group_name: Name of group in generated YAML (default 'all').

    Returns:
        Path to saved generated YAML file.
    """
    # Load template YAML for group and table structure
    with open(template_path, "r") as f:
        template = yaml.safe_load(f)

    if not template or "groups" not in template or not template["groups"]:
        raise ValueError("Invalid or empty YAML template at %s" % template_path)

    template_group = template["groups"][0]  # first group as base template
    table_template = template_group.get("tables", [{}])[0]  # first table as base

    # Get foreign tables for schema
    schema = 'cs_fdw'
    tables = get_foreign_tables(pg_resource, schema)

    # Build new group dict
    new_group = {
        "name": output_group_name,
        "enabled": True,  # Add enabled flag to control group activation
        "schedule": template_group.get("schedule", "daily"),
        "dependency": template_group.get("dependency", None),
        "tables": [],
    }

    # Create one table entry per foreign table using template, replacing 'table' attr
    for table_name in tables:
        table_entry = dict(table_template)  # shallow copy
        table_entry["table"] = table_name
        # Optionally overwrite source_schema and target_schema if needed:
        # table_entry["source_schema"] = schema
        # table_entry["target_schema"] = some_target_schema
        new_group["tables"].append(table_entry)

    output_yaml = {"groups": [new_group]}

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / output_filename

    class NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True  # disables anchors and aliases altogether

    # When dumping, use this dumper:
    with open(output_path, "w") as f:
        yaml.dump(output_yaml, f, Dumper=NoAliasDumper, sort_keys=False)

    print(f"Generated replication mapping YAML saved at: {output_path}")

    return output_path