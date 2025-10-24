import os
from dagster import ConfigurableResource, resource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import oracledb

# Oracle Instant Client initialization (thick mode enabled)
oracledb.init_oracle_client(lib_dir="/etc/oracle_client/instantclient_23_26/")

# -----------------------------------
# Group Resource for Job-specific Group Name
# -----------------------------------
@resource(config_schema={"group_name": str})
def group_resource(context):
    """
    Provides the current job's assigned group name as a resource.
    """
    return context.resource_config["group_name"]


# -----------------------------------
# Postgres Resource
# -----------------------------------
class PostgresResource(ConfigurableResource):
    """
    Dagster resource for managing a PostgreSQL connection using SQLAlchemy.
    Config parameters are strings, typically loaded from environment variables.
    """

    db_user: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str

    def get_session(self) -> Session:
        """
        Create and return a SQLAlchemy session instance.

        Returns:
            A SQLAlchemy Session connected to the configured PostgreSQL database.
        """
        db_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        return SessionLocal()


# -----------------------------------
# Oracle Resource
# -----------------------------------
class OracleResource(ConfigurableResource):
    """
    Dagster resource for managing an Oracle database connection using SQLAlchemy and oracledb.
    """

    db_user: str
    db_password: str
    db_host: str
    db_port: str
    db_service: str  # Oracle typically uses service names

    def get_session(self) -> Session:
        """
        Create and return a SQLAlchemy session instance for Oracle.

        Returns:
            A SQLAlchemy Session connected to the configured Oracle database.
        """
        db_url = (
            f"oracle+oracledb://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/?service_name={self.db_service}"
        )
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        return SessionLocal()