from pathlib import Path

from ml_engineering_pyspark.utils.jdbc import (
    DEFAULT_DB_PASSWORD_ENV_VAR,
    DEFAULT_DB_SCHEMA,
    DEFAULT_DB_USER_ENV_VAR,
    DEFAULT_JDBC_JAR_ENV_VAR,
    DEFAULT_JDBC_URL_ENV_VAR,
    DEFAULT_POSTGRES_DRIVER,
    normalize_optional_path,
    normalize_table_name,
    resolve_db_password,
    resolve_env_or_value,
)
from ml_engineering_pyspark.utils.spark_session import get_spark


def run(
    jdbc_url: str,
    jdbc_input_table: str,
    user: str | None = None,
    password: str | None = None,
    password_env_var: str | None = DEFAULT_DB_PASSWORD_ENV_VAR,
    driver: str = DEFAULT_POSTGRES_DRIVER,
    jdbc_jar_path: str | Path | None = None,
    app_name: str = "jdbc-read-test",
    limit: int = 5,
) -> None:
    resolved_password = resolve_db_password(password, password_env_var)
    normalized_table = normalize_table_name(jdbc_input_table)

    spark = get_spark(app_name, jdbc_jar_path=jdbc_jar_path)

    df = spark.read.format("jdbc").options(
        url=jdbc_url,
        driver=driver,
        dbtable=normalized_table,
        user=user,
        password=resolved_password,
    ).load()

    df.show(limit, truncate=False)
    print("row count =", df.count())
    spark.stop()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Test reading rows from a JDBC source")
    parser.add_argument(
        "--jdbc-url",
        type=str,
        default=None,
        help="JDBC URL. Falls back to the JDBC_URL environment variable.",
    )
    parser.add_argument(
        "--jdbc-input-table",
        type=str,
        required=True,
        help="Input table name for JDBC. If schema is omitted, public.<table> is used.",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=None,
        help="Database user. Falls back to the DB_USER environment variable.",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=None,
        help="Database password",
    )
    parser.add_argument(
        "--db-password-env",
        type=str,
        default=DEFAULT_DB_PASSWORD_ENV_VAR,
        help="Environment variable name for database password when --db-password is omitted",
    )
    parser.add_argument(
        "--jdbc-driver",
        type=str,
        default=DEFAULT_POSTGRES_DRIVER,
        help="Fully qualified JDBC driver class name",
    )
    parser.add_argument(
        "--jdbc-jar",
        type=Path,
        default=None,
        help="Path to JDBC driver jar file. Falls back to the JDBC_JAR environment variable.",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="jdbc-read-test",
        help="Spark application name",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to display",
    )

    args = parser.parse_args()
    jdbc_url = resolve_env_or_value(args.jdbc_url, DEFAULT_JDBC_URL_ENV_VAR)
    db_user = resolve_env_or_value(args.db_user, DEFAULT_DB_USER_ENV_VAR)
    jdbc_jar_path = normalize_optional_path(
        resolve_env_or_value(normalize_optional_path(args.jdbc_jar), DEFAULT_JDBC_JAR_ENV_VAR)
    )

    if not jdbc_url:
        raise ValueError(
            f"JDBC URL not found. Pass --jdbc-url or export {DEFAULT_JDBC_URL_ENV_VAR}."
        )

    if not db_user:
        raise ValueError(
            f"Database user not found. Pass --db-user or export {DEFAULT_DB_USER_ENV_VAR}."
        )

    if not jdbc_jar_path:
        raise ValueError(
            f"JDBC jar path not found. Pass --jdbc-jar or export {DEFAULT_JDBC_JAR_ENV_VAR}."
        )

    run(
        jdbc_url=jdbc_url,
        jdbc_input_table=args.jdbc_input_table,
        user=db_user,
        password=args.db_password,
        password_env_var=args.db_password_env,
        driver=args.jdbc_driver,
        jdbc_jar_path=jdbc_jar_path,
        app_name=args.app_name,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
