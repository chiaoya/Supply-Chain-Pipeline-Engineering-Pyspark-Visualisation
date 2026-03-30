from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_order_details_for_demand(df: DataFrame) -> DataFrame:
    """Prepare order detail rows for demand and revenue analysis."""
    return (
        df.dropDuplicates(["order_id", "product_id"])
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("discount", F.col("discount").cast("double"))
        .fillna({"unit_price": 0.0, "quantity": 0, "discount": 0.0})
    )


def clean_orders_for_demand(df: DataFrame) -> DataFrame:
    """Cast order fields used by the demand base table."""
    return (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("employee_id", F.col("employee_id").cast("int"))
        .withColumn("order_date", F.to_date("order_date"))
    )


def clean_products_for_demand(df: DataFrame) -> DataFrame:
    """Cast product fields used by the demand base table."""
    return (
        df.withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("supplier_id", F.col("supplier_id").cast("int"))
        .withColumn("category_id", F.col("category_id").cast("int"))
        .withColumn("product_name", F.trim(F.col("product_name")))
    )
