import argparse
from pathlib import Path

import plotly.express as px
from pyspark.sql import functions as F

from ml_engineering_pyspark.utils.config_loader import load_config
from ml_engineering_pyspark.utils.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create interactive charts from monthly product demand data")
    parser.add_argument("--config", type=str, default="configs/base.yaml", help="Config path")
    parser.add_argument("--input", type=str, default=None, help="Optional monthly parquet path override")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory for generated HTML charts",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Keep the top N products by total revenue for clearer charts",
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=None,
        help="Optional category filter",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    input_path = args.input or config["paths"]["product_demand_monthly"]
    output_dir = Path(args.output_dir or config["paths"]["product_demand_monthly_charts"])
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark("visualize-product-demand-monthly")
    monthly_df = spark.read.parquet(input_path)

    if args.category_id is not None:
        monthly_df = monthly_df.filter(monthly_df["category_id"] == args.category_id)

    totals_df = (
        monthly_df.groupBy("product_id", "product_name")
        .sum("monthly_revenue")
        .withColumnRenamed("sum(monthly_revenue)", "total_revenue")
        .orderBy(F.col("total_revenue").desc())
        .limit(args.top_n)
    )

    top_products = [row["product_id"] for row in totals_df.select("product_id").collect()]
    chart_df = monthly_df.filter(monthly_df["product_id"].isin(top_products)).orderBy(
        "order_year_month", "product_id"
    )
    pandas_df = chart_df.toPandas()

    revenue_fig = px.line(
        pandas_df,
        x="order_year_month",
        y="monthly_revenue",
        color="product_name",
        markers=True,
        title="Monthly Revenue Trend",
    )
    revenue_fig.write_html(output_dir / "monthly_revenue_trend.html")

    demand_fig = px.line(
        pandas_df,
        x="order_year_month",
        y="monthly_demand_units",
        color="product_name",
        markers=True,
        title="Monthly Demand Trend",
    )
    demand_fig.write_html(output_dir / "monthly_demand_trend.html")

    revenue_hist_fig = px.histogram(
        pandas_df,
        x="monthly_revenue",
        nbins=30,
        color="product_name",
        title="Distribution of Monthly Revenue",
    )
    revenue_hist_fig.write_html(output_dir / "monthly_revenue_histogram.html")

    demand_hist_fig = px.histogram(
        pandas_df,
        x="monthly_demand_units",
        nbins=30,
        color="product_name",
        title="Distribution of Monthly Demand Units",
    )
    demand_hist_fig.write_html(output_dir / "monthly_demand_histogram.html")

    scatter_fig = px.scatter(
        pandas_df,
        x="monthly_demand_units",
        y="monthly_revenue",
        color="product_name",
        animation_frame="order_year_month",
        hover_data=["product_id", "product_name", "monthly_order_count"],
        title="Monthly Revenue vs Demand",
    )
    scatter_fig.write_html(output_dir / "monthly_revenue_vs_demand.html")

    print(f"Saved charts to {output_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
