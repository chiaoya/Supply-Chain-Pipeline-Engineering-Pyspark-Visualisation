from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_product_demand_base(
    orders_df: DataFrame,
    order_details_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Create an order-line-level demand base table."""
    return (
        order_details_df.alias("od")
        .join(orders_df.alias("o"), on="order_id", how="inner")
        .join(products_df.alias("p"), on="product_id", how="inner")
        .select(
            "order_id",
            "o.order_date",
            "o.customer_id",
            "o.employee_id",
            "product_id",
            "p.product_name",
            "p.category_id",
            "p.supplier_id",
            "od.unit_price",
            "od.quantity",
            "od.discount",
        )
    )


def add_demand_revenue_features(df: DataFrame) -> DataFrame:
    """Add row-level demand, revenue, and monthly analysis fields."""
    return (
        df.withColumn("demand_units", F.col("quantity"))
        .withColumn(
            "revenue",
            F.col("unit_price") * F.col("quantity") * (F.lit(1) - F.col("discount")),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
    )


def aggregate_product_demand_summary(df: DataFrame) -> DataFrame:
    """Aggregate demand and revenue to a product summary."""
    return (
        df.groupBy("product_id", "product_name", "category_id")
        .agg(
            F.sum("demand_units").alias("total_demand_units"),
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def aggregate_monthly_product_demand(df: DataFrame) -> DataFrame:
    """Aggregate monthly demand and revenue to a product-month summary."""
    return (
        df.groupBy(
            "order_year",
            "order_month",
            "order_year_month",
            "product_id",
            "product_name",
            "category_id",
        )
        .agg(
            F.sum("demand_units").alias("monthly_demand_units"),
            F.sum("revenue").alias("monthly_revenue"),
            F.countDistinct("order_id").alias("monthly_order_count"),
        )
        .orderBy(F.col("order_year_month"), F.desc("monthly_revenue"))
    )
