import argparse

from ml_engineering_pyspark.transforms.demand_cleaning import (
    clean_order_details_for_demand,
    clean_orders_for_demand,
    clean_products_for_demand,
)
from ml_engineering_pyspark.transforms.demand_processing import (
    add_demand_revenue_features,
    aggregate_monthly_product_demand,
    build_product_demand_base,
)
from ml_engineering_pyspark.utils.config_loader import load_config
from ml_engineering_pyspark.utils.jdbc import DEFAULT_DB_PASSWORD_ENV_VAR, resolve_db_password
from ml_engineering_pyspark.utils.postgres_reader import read_table
from ml_engineering_pyspark.utils.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the monthly product demand summary")
    parser.add_argument("--config", type=str, default="configs/base.yaml", help="Config path")
    parser.add_argument("--output", type=str, default=None, help="Optional output path override")
    parser.add_argument("--limit", type=int, default=20, help="Preview row limit")
    parser.add_argument("--jdbc-jar", type=str, required=True, help="Path to PostgreSQL JDBC jar")
    parser.add_argument("--db-password", type=str, default=None, help="Database password override")
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
    table_config = config["tables"]
    output_path = args.output or config["paths"]["product_demand_monthly"]

    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['db']}"
    password = resolve_db_password(args.db_password, args.db_password_env)

    if not password:
        raise ValueError("Database password not found. Set --db-password or export DB_PASSWORD.")

    spark = get_spark("product-demand-monthly", jdbc_jar_path=args.jdbc_jar)

    orders_df = clean_orders_for_demand(
        read_table(spark, jdbc_url, table_config["orders"], db_config["user"], password)
    )
    order_details_df = clean_order_details_for_demand(
        read_table(spark, jdbc_url, table_config["order_details"], db_config["user"], password)
    )
    products_df = clean_products_for_demand(
        read_table(spark, jdbc_url, table_config["products"], db_config["user"], password)
    )

    base_df = build_product_demand_base(orders_df, order_details_df, products_df)
    featured_df = add_demand_revenue_features(base_df)
    monthly_df = aggregate_monthly_product_demand(featured_df)

    monthly_df.show(args.limit, truncate=False)
    monthly_df.write.mode("overwrite").parquet(output_path)

    print(f"Saved monthly product demand summary to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
