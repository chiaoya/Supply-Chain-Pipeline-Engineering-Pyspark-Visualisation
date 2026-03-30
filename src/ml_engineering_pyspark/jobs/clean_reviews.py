from pathlib import Path

from pyspark.sql import functions as F

from ml_engineering_pyspark.transforms.cleaning import clean_reviews
from ml_engineering_pyspark.transforms.processing import add_text_features
from ml_engineering_pyspark.utils.jdbc import (
    DEFAULT_DB_PASSWORD_ENV_VAR,
    DEFAULT_POSTGRES_DRIVER,
    normalize_optional_path,
    resolve_db_password,
)
from ml_engineering_pyspark.utils.spark_session import get_spark

def run(
    input_path: Path | None = None,
    output_path: Path | None = None,
    app_name: str = "ml-engineering-pyspark",
    jdbc_url: str | None = None,
    jdbc_input_table: str | None = None,
    jdbc_output_table: str | None = None,
    user: str | None = None,
    password: str | None = None,
    password_env_var: str | None = DEFAULT_DB_PASSWORD_ENV_VAR,
    driver: str = DEFAULT_POSTGRES_DRIVER,
    jdbc_jar_path: str | Path | None = None,
) -> None:
    spark = get_spark(app_name, jdbc_jar_path=jdbc_jar_path)
    resolved_password = resolve_db_password(password, password_env_var)

    if jdbc_url and jdbc_input_table:
        df = spark.read.format("jdbc").options(
            url=jdbc_url,
            driver=driver,
            dbtable=jdbc_input_table,
            user=user,
            password=resolved_password,
        ).load()
    elif input_path:
        df = spark.read.option("header", True).csv(str(input_path))
    else:
        raise ValueError(
            "Provide either --input for a downloaded file source or both "
            "--jdbc-url and --jdbc-input-table for a PostgreSQL source."
        )

    cleaned_df = clean_reviews(df)
    processed_df = add_text_features(cleaned_df)

    processed_df.orderBy(F.col("review_id").cast("int")).show(truncate=False)

    if jdbc_url and jdbc_output_table:
        processed_df.write.mode("overwrite").format("jdbc").options(
            url=jdbc_url,
            driver=driver,
            dbtable=jdbc_output_table,
            user=user,
            password=resolved_password,
        ).save()
    elif output_path:
        processed_df.write.mode("overwrite").parquet(str(output_path))
    else:
        raise ValueError(
            "Provide either --output for a file sink or --jdbc-output-table for a PostgreSQL sink."
        )

    spark.stop()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run reviews data cleaning pipeline")
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input CSV path from GitHub, Kaggle, or another downloaded source",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output parquet path",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="ml-engineering-pyspark",
        help="Spark application name",
    )
    parser.add_argument(
        "--jdbc-url",
        type=str,
        default=None,
        help="JDBC URL (e.g. jdbc:postgresql://host:5432/db)",
    )
    parser.add_argument(
        "--jdbc-input-table",
        type=str,
        default=None,
        help="Input table name for JDBC",
    )
    parser.add_argument(
        "--jdbc-output-table",
        type=str,
        default=None,
        help="Output table name for JDBC",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=None,
        help="Database user",
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
        "--jdbc-jar",
        type=Path,
        default=None,
        help="Path to JDBC driver jar file",
    )

    args = parser.parse_args()
    run(
        input_path=args.input,
        output_path=args.output,
        app_name=args.app_name,
        jdbc_url=args.jdbc_url,
        jdbc_input_table=args.jdbc_input_table,
        jdbc_output_table=args.jdbc_output_table,
        user=args.db_user,
        password=args.db_password,
        password_env_var=args.db_password_env,
        jdbc_jar_path=normalize_optional_path(args.jdbc_jar),
    )


if __name__ == "__main__":
    main()
