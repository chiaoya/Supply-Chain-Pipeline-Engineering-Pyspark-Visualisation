from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_revenue_features(df: DataFrame) -> DataFrame:
    """Add row-level revenue using unit price, quantity, and discount."""
    return df.withColumn(
        "revenue",
        F.col("unit_price") * F.col("quantity") * (F.lit(1) - F.col("discount")),
    )


def aggregate_product_revenue(df: DataFrame) -> DataFrame:
    """Aggregate revenue and quantity to the product level."""
    return (
        # Group by product so we can produce one summary row per product.
        df.groupBy("product_id", "product_name")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
        )
        # Sort highest revenue first for quick inspection.
        .orderBy(F.desc("total_revenue"))
    )
