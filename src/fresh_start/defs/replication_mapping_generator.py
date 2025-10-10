import yaml
from pathlib import Path
from typing import List, Optional, Set, Dict, Any
from sqlalchemy import text
from .resources import PostgresResource


def get_foreign_tables(pg_resource: PostgresResource, schema_name: str) -> List[str]:
    """
    Query PostgreSQL to get foreign table names in the given schema.
    
    Args:
        pg_resource: PostgresResource instance for DB access.
        schema_name: Name of the schema to query.

    Returns:
        List of foreign table names within the schema.
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
        tables = [row.foreign_table_name for row in result.fetchall()]
        return tables
    finally:
        session.close()


def generate_group_dict(
    group_name: str,
    tables: List[str],
    template_group: Dict[str, Any],
    table_template: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a group dictionary for replication mapping YAML using the template.

    Args:
        group_name: Name of the group (e.g., 'all', 'excluded').
        tables: List of table names for this group.
        template_group: Template dictionary of a group from the YAML.
        table_template: Template dictionary of a table from the YAML.

    Returns:
        Dictionary representing the group ready for YAML dumping.
    """
    group = {
        "name": group_name,
        "enabled": True,
        "schedule": template_group.get("schedule", "daily"),
        "dependency": template_group.get("dependency", None),
        "tables": []
    }

    for table_name in tables:
        table_entry = dict(table_template)  # shallow copy
        table_entry["table"] = table_name
        group["tables"].append(table_entry)

    return group


def generate_replication_mapping_groups_yaml(
    pg_resource: PostgresResource,
    schema: Optional[str],
    template_path: Path,
    output_dir: Path,
    output_filename: str = "replication_mapping_generated.yaml",
    excluded_tables: Optional[Set[str]] = None,
) -> Path:
    """
    Generate a replication_mapping YAML file with two groups: 'all' and 'excluded'.

    The schema can be provided or extracted from the template's 'source_schema' of the first table.

    Args:
        pg_resource: PostgresResource instance.
        schema: Optional; schema name to query foreign tables from. If None, extracted from template.
        template_path: Path to YAML template file.
        output_dir: Directory where the YAML file will be saved.
        output_filename: Name of the output YAML file.
        excluded_tables: Optional set of table names to exclude from 'all' group and include in 'excluded'.

    Returns:
        Path to the generated YAML file.
    """
    with open(template_path, "r") as f:
        template = yaml.safe_load(f)

    if not template or "groups" not in template or not template["groups"]:
        raise ValueError(f"Invalid or empty YAML template at {template_path}")

    template_group = template["groups"][0]

    tables_list = template_group.get("tables")
    if not tables_list or not isinstance(tables_list, list):
        raise ValueError(f"Template group does not contain a valid 'tables' list")

    table_template = tables_list[0]

    # Extract schema from template if not provided explicitly
    if not schema:
        schema = table_template.get("source_schema")
        if not schema:
            raise RuntimeError("Schema not provided and 'source_schema' missing in template")

    # Fetch all foreign tables for this schema
    all_tables = get_foreign_tables(pg_resource, schema)
    print(f"Foreign tables fetched from schema '{schema}': {all_tables}")

    excluded_tables = excluded_tables or set()

    # Partition tables into "all" and "excluded"
    all_group_tables = [t for t in all_tables if t not in excluded_tables]
    excluded_group_tables = [t for t in all_tables if t in excluded_tables]

    print(f"'all' group tables count: {len(all_group_tables)}")
    print(f"'excluded' group tables count: {len(excluded_group_tables)}")

    # Build the two groups dictionaries
    group_all = generate_group_dict("all", all_group_tables, template_group, table_template)
    group_excluded = generate_group_dict("excluded", excluded_group_tables, template_group, table_template)

    # Compose final output structure
    output_yaml = {"groups": [group_all, group_excluded]}

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_filename

    # Prevent YAML aliases/anchors for cleaner output
    class NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True

    with open(output_path, "w") as f:
        yaml.dump(output_yaml, f, Dumper=NoAliasDumper, sort_keys=False)

    print(f"Generated replication mapping YAML saved at: {output_path}")
    return output_path