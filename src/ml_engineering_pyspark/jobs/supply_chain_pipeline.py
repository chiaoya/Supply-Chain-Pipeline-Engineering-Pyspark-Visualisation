import argparse

from pyspark.sql import functions as F
from ml_engineering_pyspark.utils.config_loader import load_config
from ml_engineering_pyspark.utils.jdbc import DEFAULT_DB_PASSWORD_ENV_VAR, resolve_db_password
from ml_engineering_pyspark.utils.postgres_reader import read_table
from ml_engineering_pyspark.utils.spark_session import get_spark
from ml_engineering_pyspark.transforms.cleaning import clean_order_details
from ml_engineering_pyspark.transforms.processing import (
    add_revenue_features,
    aggregate_product_revenue,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the supply chain revenue pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/base.yaml",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional parquet output path override",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for preview output",
    )
    parser.add_argument(
        "--jdbc-jar",
        type=str,
        required=True,
        help="Path to PostgreSQL JDBC jar",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=None,
        help="Database password override",
    )
    parser.add_argument(
        "--db-password-env",
        type=str,
        default=DEFAULT_DB_PASSWORD_ENV_VAR,
        help="Environment variable to read the database password from",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    db_config = config["database"]
    spark_config = config["spark"]
    table_config = config["tables"]
    path_config = config["paths"]

    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['db']}"
    user = db_config["user"]
    password = resolve_db_password(args.db_password, args.db_password_env)

    if not password:
        raise ValueError(
            "Database password not found. Set --db-password or export the configured env var."
        )

    spark = get_spark(spark_config["app_name"], jdbc_jar_path=args.jdbc_jar)

    orders = read_table(spark, jdbc_url, table_config["orders"], user, password).alias("o")
    order_details = clean_order_details(
        read_table(
        spark, jdbc_url, table_config["order_details"], user, password
        )
    ).alias("od")
    products = read_table(spark, jdbc_url, table_config["products"], user, password).alias("p")

    joined_df = (
        order_details.join(orders, on="order_id", how="inner")
        .join(products, on="product_id", how="inner")
        .select("category_id", "product_id", "p.product_name", "od.unit_price", "od.quantity", "od.discount")
    )

    result = aggregate_product_revenue(add_revenue_features(joined_df))

    preview_df = result.limit(args.limit) if args.limit else result
    preview_df.show(20, truncate=False)

    output_path = args.output or path_config["output"]
    result.write.mode("overwrite").parquet(output_path)

    print(f"Saved to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
