from typing import List, Dict
from pathlib import Path
import yaml
from dagster import AssetsDefinition, AssetKey, asset, OpExecutionContext, AssetIn
from .asset_creators import (
    batch_iterator,
    execute_oracle_to_postgres_replication,
    execute_postgres_to_postgres_replication
)
from .util import load_enabled_groups


def build_assets_from_yaml(file_path: str, filtered_groups: List[str] = None) -> List[AssetsDefinition]:
    """
    Build batched Dagster assets from YAML.
    Each batch asset process 50 tables at once to reduce overhead.
    """
    yaml_path = Path(file_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"YAML file not found: {file_path}")

    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    if not config or "groups" not in config:
        return []

    groups = config["groups"]

    # Filter groups if requested
    if filtered_groups:
        groups = [g for g in groups if g.get("name") in filtered_groups]

    all_assets = []
    BATCH_SIZE = 50  # Number of tables per batch asset

    for group_config in groups:
        group_name = group_config.get("name")
        if not group_name:
            continue
        tables = group_config.get("tables", [])
        if not tables:
            continue
        
        source_db = group_config.get("source_db", "oracle")
        target_db = group_config.get("target_db", "postgres")

        batch_num = 1
        for table_batch in batch_iterator(tables, BATCH_SIZE):
            try:
                batch_assets = _create_batch_asset_pair(
                    group_name=group_name,
                    table_batch=list(table_batch),
                    batch_number=batch_num,
                    source_db=source_db,
                    target_db=target_db,
                )
                all_assets.extend(batch_assets)
                batch_num += 1
            except Exception as e:
                print(f"⚠️ Failed to create batch {batch_num} for group {group_name}: {e}")
                continue

    return all_assets


def _create_batch_asset_pair(
    group_name: str,
    table_batch: List[Dict],
    batch_number: int,
    source_db: str,
    target_db: str
) -> List[AssetsDefinition]:
    """
    Create two assets per batch: one source asset processing all tables,
    one target asset dependent on the source asset.
    """

    batch_id = f"batch_{batch_number:03d}"
    table_count = len(table_batch)

    @asset(
        key=AssetKey([group_name, f"{batch_id}__source"]),
        group_name=group_name,
        description=f"Source batch {batch_number} for {group_name} - {table_count} tables",
        tags={
            "stage": "source",
            "batch_number": str(batch_number),
            "table_count": str(table_count),
            "batch_stage": "extract",
        },
        required_resource_keys={"oracle", "postgres"},
    )
    def batch_source_asset(context: OpExecutionContext) -> Dict:
        results = {}
        successful_tables = []
        context.log.info(f"🚀 Starting batch {batch_number} source with {table_count} tables")

        for i, table_cfg in enumerate(table_batch, 1):
            table_name = table_cfg.get("table")
            if not table_name:
                continue
            
            source_db = table_cfg.get("source_db", "oracle").lower()
            target_db = table_cfg.get("target_db", "postgres").lower()
            
            try:
                context.log.info(f"📥 Processing source table {i}/{table_count}: {table_name} (src_db={source_db})")
                
                if source_db == "oracle" and target_db == "postgres":
                    # Oracle to Postgres replication
                    upstream_sql = f'SELECT * FROM "{table_cfg["source_schema"]}"."{table_name}"'
                    inserted_rows = execute_oracle_to_postgres_replication(
                        context,
                        upstream_sql,
                        table_cfg["source_schema"],
                        table_cfg["target_schema"],
                        table_name,
                        context.log,
                    )
                    
                elif source_db == "postgres" and target_db == "postgres":
                    # Postgres to Postgres replication
                    upstream_sql = f'SELECT * FROM "{table_cfg["source_schema"]}"."{table_name}"'
                    
                    inserted_rows = execute_postgres_to_postgres_replication(
                        context,
                        upstream_sql,
                        table_cfg["target_schema"],
                        table_name,
                        context.log,
                    )
                else:
                    raise RuntimeError(f"Unsupported source_db/target_db combination: {source_db} -> {target_db}")

                results[table_name] = inserted_rows
                successful_tables.append(table_name)
                context.log.info(f"✅ Successfully processed {table_name} inserted {inserted_rows} rows")

            except Exception as e:
                context.log.error(f"❌ Source batch failed for table {table_name}: {e}")
                continue
        
        context.log.info(f"🎯 Batch {batch_number} source completed: {len(successful_tables)}/{table_count} tables")
        return {
            "results": results,
            "successful_tables": successful_tables,
            "batch_number": batch_number,
            "table_count": table_count,
        }
    
    @asset(
        key=AssetKey([group_name, f"{batch_id}__target"]),
        group_name=group_name,
        description=f"Target batch {batch_number} for {group_name} - {table_count} tables",
        tags={
            "stage": "target",
            "batch_number": str(batch_number),
            "table_count": str(table_count),
            "batch_stage": "load",
        },
        required_resource_keys={"postgres"},
        ins={
            "source_data": AssetIn(key=AssetKey([group_name, f"{batch_id}__source"]))
        },
    )
    def batch_target_asset(context: OpExecutionContext, source_data: Dict) -> Dict:
        """
        Loads data into Postgres for all successfully source-replicated tables.
        """
        processed_tables = []
        successful_tables = source_data.get("successful_tables", [])
        source_results = source_data.get("results", {})

        context.log.info(f"🚀 Starting batch {batch_number} target with {len(successful_tables)} tables")

        for i, table_cfg in enumerate(table_batch, 1):
            table_name = table_cfg.get("table")
            if not table_name or table_name not in successful_tables:
                continue

            try:
                context.log.info(f"📤 Processing target table {i}/{len(successful_tables)}: {table_name}")
                target_schema = table_cfg["target_schema"]

                # Use the SQL string (or rows info) from source_results as input upstream_data
                # Here you may need to adapt depending on how execute_postgres_to_postgres_replication expects data
                upstream_sql = f'SELECT * FROM "{target_schema}"."{table_name}"'

                result = execute_postgres_to_postgres_replication(
                    context,
                    upstream_sql,
                    target_schema,
                    table_name,
                    context.log,
                )

                processed_tables.append(table_name)
                context.log.info(f"✅ Target replication completed for {table_name}")

            except Exception as e:
                context.log.error(f"❌ Target batch failed for table {table_name}: {e}")
                continue

        context.log.info(f"🎯 Batch {batch_number} target completed: {len(processed_tables)}/{len(successful_tables)} tables")
        return {
            "processed_tables": processed_tables,
            "batch_number": batch_number,
            "success_count": len(processed_tables),
        }

    return [batch_source_asset, batch_target_asset]