import os
from pathlib import Path


DEFAULT_DB_PASSWORD_ENV_VAR = "DB_PASSWORD"
DEFAULT_POSTGRES_DRIVER = "org.postgresql.Driver"
DEFAULT_JDBC_URL_ENV_VAR = "JDBC_URL"
DEFAULT_DB_USER_ENV_VAR = "DB_USER"
DEFAULT_JDBC_JAR_ENV_VAR = "JDBC_JAR"
DEFAULT_DB_SCHEMA = "public"


def resolve_db_password(
    password: str | None = None,
    password_env_var: str | None = DEFAULT_DB_PASSWORD_ENV_VAR,
) -> str | None:
    if password:
        return password

    if password_env_var:
        return os.getenv(password_env_var)

    return None


def normalize_optional_path(path: str | Path | None) -> str | None:
    if path is None:
        return None

    return str(path)


def resolve_env_or_value(value: str | None, env_var: str | None) -> str | None:
    if value:
        return value

    if env_var:
        return os.getenv(env_var)

    return None


def normalize_table_name(table: str, default_schema: str = DEFAULT_DB_SCHEMA) -> str:
    if "." in table:
        return table

    return f"{default_schema}.{table}"
