# resources.py

from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

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
