from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import oracledb

# Enable thick mode by specifying the Instant Client directory
oracledb.init_oracle_client(lib_dir="/home/mguerman/oracle_client/instantclient_19_28")
print("Oracle client initialized successfully.")

class PostgresResource(ConfigurableResource):
    """
    Dagster resource for managing a PostgreSQL connection using SQLAlchemy.
    Each field is a string, resolved from environment variables or config.
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