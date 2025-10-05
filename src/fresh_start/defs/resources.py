import dagster as dg
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# PostgreSQL resource using EnvVar
def postgres_resource_fn(init_context):
    db_user = init_context.resource_config["db_user"]
    db_password = init_context.resource_config["db_password"]
    db_host = init_context.resource_config["db_host"]
    db_port = init_context.resource_config["db_port"]
    db_name = init_context.resource_config["db_name"]

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

PostgresResource = dg.ResourceDefinition(
    resource_fn=postgres_resource_fn,
    config_schema={
        "db_user": dg.EnvVar("DB_USER"),
        "db_password": dg.EnvVar("DB_PASSWORD"),
        "db_host": dg.EnvVar("DB_HOST"),
        "db_port": dg.EnvVar("DB_PORT"),
        "db_name": dg.EnvVar("DB_NAME"),
    }
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "postgres": PostgresResource
        }
    )
