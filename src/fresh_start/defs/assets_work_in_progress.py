import yaml
from pathlib import Path
from typing import List, Dict, Optional
from dagster import asset, AssetKey, AssetsDefinition, AssetIn, AssetExecutionContext
from sqlalchemy.orm import Session
from sqlalchemy import text

# ────────────────────────────────────────────────────────────────────────────────
# Configuration variables
# ────────────────────────────────────────────────────────────────────────────────
BATCH_SIZE = 500000  # Process 500K records at a time
ENABLE_BATCHING = True  # Toggle batch processing
limit: Optional[int] = None  # Set to None or 0 for no limit (for testing)

def get_limit_clause() -> str:
    """
    Return the SQL limit clause string based on limit variable.
    Returns empty string if no limit specified.
    """
    if limit is None or limit <= 0:
        return ""  # No limit
    else:
        return f"LIMIT {limit}"


# ────────────────────────────────────────────────────────────────────────────────
# Load YAML configuration and filter by group names
# ────────────────────────────────────────────────────────────────────────────────
def load_yaml_groups(yaml_path: str | Path, groups_list: List[str]) -> List[Dict]:
    """
    Load YAML file and filter for specified group names.

    Args:
        yaml_path: Path to the YAML config file.
        groups_list: List of group names to include.

    Returns:
        List of group dictionaries matching the specified names.
    """
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)
    return [group for group in data.get("groups", []) if group.get("name") in groups_list]


# ────────────────────────────────────────────────────────────────────────────────
# Optimize PostgreSQL session for bulk operations
# ────────────────────────────────────────────────────────────────────────────────
def optimize_postgres_for_bulk_ops(session: Session) -> None:
    """Configure PostgreSQL session for optimal bulk operations."""
    optimization_sql = text("""
        -- Increase working memory for this session
        SET work_mem = '256MB';
        
        -- Increase maintenance memory for bulk operations
        SET maintenance_work_mem = '1GB';
        
        -- Optimize checkpoint settings for bulk operations
        SET checkpoint_completion_target = 0.9;
        
        -- Set reasonable timeout for long-running queries
        SET statement_timeout = '30min';
        
        -- Disable autovacuum during bulk inserts for better performance
        SET session_replication_role = 'replica';
        
        -- Use more aggressive parallelism
        SET max_parallel_workers_per_gather = 4;
        
        -- Reduce logging overhead
        SET log_statement = 'none';
        SET log_duration = 'off';
    """)
    session.execute(optimization_sql)
    session.commit()


def reset_postgres_settings(session: Session) -> None:
    """Reset PostgreSQL settings to default after bulk operations."""
    reset_sql = text("""
        -- Re-enable normal settings
        SET session_replication_role = 'origin';
        RESET work_mem;
        RESET maintenance_work_mem;
        RESET checkpoint_completion_target;
        RESET statement_timeout;
        RESET max_parallel_workers_per_gather;
        RESET log_statement;
        RESET log_duration;
    """)
    session.execute(reset_sql)
    session.commit()


# ────────────────────────────────────────────────────────────────────────────────
# Create a Dagster asset for a given stage: source, transform, or target
# ────────────────────────────────────────────────────────────────────────────────
def create_asset(
    group: str,
    table: Dict,
    stage: str,
    upstream_key: Optional[AssetKey] = None
) -> AssetsDefinition:
    """
    Dynamically create a Dagster asset for a given stage.

    Args:
        group: Name of the asset group.
        table: Table configuration dictionary from YAML.
        stage: One of "source", "transform", or "target".
        upstream_key: Optional dependency AssetKey.

    Returns:
        Dagster AssetsDefinition object.
    """
    table_name = table["table"]
    source_schema = table["source_schema"]
    target_schema = table["target_schema"]
    transformation = table.get("transformation", {})
    transformation_steps = transformation.get("steps", "")
    transformation_enabled = transformation.get("enabled", False)

    asset_key = AssetKey([group, table_name, stage])
    func_name = f"{group}_{table_name}_{stage}"

    sql_limit_clause = get_limit_clause()

    # ────────────────────────────────────────────────────────────────────────────
    # Source asset: returns a preview query with LIMIT (or no limit)
    # ────────────────────────────────────────────────────────────────────────────
    if stage == "source":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"source", group},
            description=f"Extract data from {source_schema}.{table_name}",
        )
        def source_asset() -> str:
            # Return SELECT with conditional LIMIT
            return f"SELECT * FROM {source_schema}.{table_name} {sql_limit_clause};"

        source_asset.__name__ = func_name
        return source_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Transform asset: applies optional transformation steps
    # ────────────────────────────────────────────────────────────────────────────
    elif stage == "transform":
        @asset(
            key=asset_key,
            group_name=group,
            kinds={"transform", group},
            description=f"Transform data for {table_name} with steps: {transformation_steps}",
            deps=[upstream_key] if upstream_key else [],
            ins={"source": AssetIn(key=upstream_key)} if upstream_key else {},
        )
        def transform_asset(source: str) -> str:
            if source is None:
                raise ValueError(f"Missing upstream source asset for: {group}.{table_name}.transform")

            # Apply transformation steps if defined
            return f"-- Transformed ({transformation_steps})\n{source}" if transformation_steps else source

        transform_asset.__name__ = func_name
        return transform_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Target asset: loads data into target schema WITH BATCH PROCESSING
    # ────────────────────────────────────────────────────────────────────────────
    elif stage == "target":
        upstream_stage = "transform" if transformation_enabled else "source"
        upstream_asset_key = AssetKey([group, table_name, upstream_stage])

        @asset(
            key=asset_key,
            group_name=group,
            kinds={"target", group},
            description=f"Load data into {target_schema}.{table_name} (batch size: {BATCH_SIZE:,})",
            deps=[upstream_asset_key],
            ins={upstream_stage: AssetIn(key=upstream_asset_key)},
            required_resource_keys={"postgres"},
        )
        def target_asset(context: AssetExecutionContext, **kwargs) -> str:
            upstream_data = kwargs.get(upstream_stage)
            if upstream_data is None:
                raise ValueError(f"Missing upstream asset for: {group}.{table_name}.target")

            # Remove trailing semicolon to safely reuse SQL
            cleaned_sql = upstream_data.strip().rstrip(";")
            
            # Get SQLAlchemy session from the postgres resource
            session: Session = context.resources.postgres.get_session()
            
            try:
                # Optimize PostgreSQL for bulk operations
                context.log.info(f"Optimizing PostgreSQL settings for bulk operations on {target_schema}.{table_name}")
                optimize_postgres_for_bulk_ops(session)
                
                # Create table structure if it doesn't exist
                create_structure_sql = f"""
                    CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}" AS
                    SELECT *, 
                           now() AS dl_inserteddate,
                           'system' AS dl_insertedby,
                           md5(CAST(row_to_json(t) AS text)) AS row_hash
                    FROM ({cleaned_sql} LIMIT 0) t
                """
                
                session.execute(text(create_structure_sql))
                context.log.info(f"Ensured table structure exists for {target_schema}.{table_name}")
                
                # Create unique index on row_hash for deduplication
                create_index_sql = f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_row_hash 
                    ON "{target_schema}"."{table_name}" (row_hash)
                """
                session.execute(text(create_index_sql))
                context.log.info(f"Created unique index on row_hash for {target_schema}.{table_name}")
                
                # Check if we should use batching
                if ENABLE_BATCHING and not sql_limit_clause:
                    # Get total count for progress tracking
                    count_sql = f"SELECT COUNT(*) FROM ({cleaned_sql}) t"
                    total_count_result = session.execute(text(count_sql)).scalar()
                    
                    # Handle None or 0 count
                    if not total_count_result or total_count_result == 0:
                        context.log.warning(f"No records to process for {target_schema}.{table_name}")
                        return f"No records to process for {target_schema}.{table_name}"
                    
                    total_count = int(total_count_result)  # Ensure it's an integer
                    context.log.info(f"Total records to process: {total_count:,}")
                    
                    # Process in batches
                    processed = 0
                    batch_num = 0
                    
                    while processed < total_count:  # Now type-safe
                        batch_num += 1
                        offset = processed
                        
                        # Batch insert with deduplication - using CTE for better counting
                        batch_insert_sql = f"""
                            WITH inserted AS (
                                INSERT INTO "{target_schema}"."{table_name}"
                                SELECT *, 
                                       now() AS dl_inserteddate,
                                       'system' AS dl_insertedby,
                                       md5(CAST(row_to_json(t) AS text)) AS row_hash
                                FROM (
                                    SELECT * FROM ({cleaned_sql}) base
                                    ORDER BY 1  -- Ensure consistent ordering
                                    LIMIT {BATCH_SIZE} OFFSET {offset}
                                ) t
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM "{target_schema}"."{table_name}" existing
                                    WHERE existing.row_hash = md5(CAST(row_to_json(t) AS text))
                                )
                                RETURNING 1
                            )
                            SELECT COUNT(*) FROM inserted
                        """
                        
                        # Execute batch insert and get the count directly
                        result = session.execute(text(batch_insert_sql))
                        inserted_count = result.scalar() or 0  # Get the count from SELECT
                        session.commit()  # Commit each batch to avoid long transactions
                        
                        processed += BATCH_SIZE
                        actual_processed = min(processed, total_count)
                        
                        context.log.info(
                            f"Batch {batch_num}: Processed {actual_processed:,}/{total_count:,} records "
                            f"({actual_processed/total_count*100:.1f}%) - "
                            f"Inserted {inserted_count:,} new records"
                        )
                        
                        # Break if we've processed all records
                        if offset + BATCH_SIZE >= total_count:
                            break

                    
                    # Run ANALYZE to update table statistics after bulk insert
                    analyze_sql = f'ANALYZE "{target_schema}"."{table_name}"'
                    session.execute(text(analyze_sql))
                    context.log.info(f"Updated table statistics for {target_schema}.{table_name}")
                    
                    message = f"Completed batch processing: Loaded {total_count:,} records into {target_schema}.{table_name}"
                
                else:
                    # Single insert operation (for small datasets or when LIMIT is set)
                    single_insert_sql = f"""
                        WITH inserted AS (
                            INSERT INTO "{target_schema}"."{table_name}"
                            SELECT *, 
                                now() AS dl_inserteddate,
                                'system' AS dl_insertedby,
                                md5(CAST(row_to_json(t) AS text)) AS row_hash
                            FROM ({cleaned_sql}) t
                            WHERE NOT EXISTS (
                                SELECT 1 FROM "{target_schema}"."{table_name}" existing
                                WHERE existing.row_hash = md5(CAST(row_to_json(t) AS text))
                            )
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM inserted
                    """

                    context.log.info(f"Executing single insert for {target_schema}.{table_name}")
                    result = session.execute(text(single_insert_sql))
                    inserted_count = result.scalar() or 0
                    session.commit()

                    # Update statistics
                    analyze_sql = f'ANALYZE "{target_schema}"."{table_name}"'
                    session.execute(text(analyze_sql))

                    message = f"Inserted {inserted_count:,} new records into {target_schema}.{table_name}"
                    context.log.info(message)

                
                # Reset PostgreSQL settings to default
                reset_postgres_settings(session)
                context.log.info(f"Reset PostgreSQL settings to default")
                
                return message
                
            except Exception as e:
                # Ensure we reset settings even on error
                try:
                    reset_postgres_settings(session)
                    session.rollback()
                except:
                    pass  # Ignore errors during cleanup
                
                # Log the error with context
                context.log.error(f"Failed to load data into {target_schema}.{table_name}: {str(e)}")
                raise RuntimeError(f"Failed to load data into {target_schema}.{table_name}: {e}")
            finally:
                # Ensure session is properly closed
                session.close()
            
        target_asset.__name__ = func_name
        return target_asset

    # ────────────────────────────────────────────────────────────────────────────
    # Invalid stage
    # ────────────────────────────────────────────────────────────────────────────
    raise ValueError(f"Unknown asset stage: {stage}")


# ────────────────────────────────────────────────────────────────────────────────
# Build Dagster assets from YAML config
# ────────────────────────────────────────────────────────────────────────────────
def build_assets_from_yaml(yaml_path: str | Path, groups_list: List[str]) -> List[AssetsDefinition]:
    """
    Build Dagster assets from YAML config, wiring dependencies:
    source → transform (optional) → target.

    Args:
        yaml_path: Path to the YAML asset config file.
        groups_list: List of group names to include.

    Returns:
        List of Dagster AssetsDefinition objects.
    """
    selected_groups = load_yaml_groups(yaml_path, groups_list)
    assets: List[AssetsDefinition] = []

    for group in selected_groups:
        group_name = group["name"]
        for table in group.get("tables", []):
            if not table.get("enabled", False):
                continue  # Skip disabled tables

            # Create source asset
            source_asset = create_asset(group_name, table, "source")
            assets.append(source_asset)
            source_key = AssetKey([group_name, table["table"], "source"])

            # Create transform asset if enabled
            if table.get("transformation", {}).get("enabled", False):
                transform_asset = create_asset(group_name, table, "transform", upstream_key=source_key)
                assets.append(transform_asset)
                upstream_key = AssetKey([group_name, table["table"], "transform"])
            else:
                upstream_key = source_key

            # Create target asset
            target_asset_obj = create_asset(group_name, table, "target", upstream_key=upstream_key)
            assets.append(target_asset_obj)

    return assets

