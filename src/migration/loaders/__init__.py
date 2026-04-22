"""Target loaders — RDS Postgres + Redshift."""

from migration.loaders.postgres_loader import (
    LoadResult,
    PostgresDdlExecutor,
    PostgresLoader,
)
from migration.loaders.redshift_loader import (
    RedshiftDdlExecutor,
    RedshiftLoader,
    RedshiftLoadResult,
)

__all__ = [
    "LoadResult",
    "PostgresDdlExecutor",
    "PostgresLoader",
    "RedshiftDdlExecutor",
    "RedshiftLoader",
    "RedshiftLoadResult",
]
