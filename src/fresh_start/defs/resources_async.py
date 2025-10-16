from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from urllib.parse import quote_plus
import oracledb

class PostgresResource(ConfigurableResource):
    """
    Dagster resource for managing both synchronous and asynchronous PostgreSQL connections.
    """

    db_user: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str

    def get_session(self) -> Session:
        """Create and return a synchronous SQLAlchemy session for PostgreSQL."""
        db_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
        return SessionLocal()

    def get_async_session(self) -> sessionmaker:
        """Create and return an asynchronous SQLAlchemy session for PostgreSQL."""
        db_url = f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        engine = create_async_engine(db_url, echo=False)
        return sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class OracleResource(ConfigurableResource):
    """
    Dagster resource for managing an asynchronous Oracle connection using python-oracledb in thin mode.
    """

    db_user: str
    db_password: str
    db_host: str
    db_port: str
    db_service: str

    def _encode_password(self) -> str:
        return quote_plus(self.db_password)

    def _build_thin_url(self) -> str:
        base = "oracle+oracledb"
        # Use a full connect descriptor with SERVICE_NAME
        dsn = (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.db_host})(PORT={self.db_port}))"
            f"(CONNECT_DATA=(SERVICE_NAME={self.db_service})))"
        )
        return f"{base}://{self.db_user}:{self._encode_password()}@?dsn={dsn}"

    def get_async_session(self) -> sessionmaker:
        """Create and return an async SQLAlchemy session for Oracle using thin mode."""
        db_url = self._build_thin_url()

        # Ensure thin mode is used
        oracledb.defaults.fetch_lobs = False
        oracledb.defaults.thin_encoding = "UTF-8"
        oracledb.defaults.thin_mode = "async"

        engine = create_async_engine(db_url, echo=False)
        return sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
