from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_order_details(df: DataFrame) -> DataFrame:
    """Normalize order detail fields before joins and aggregations.

    Expected input columns:
    - order_id
    - product_id
    - unit_price
    - quantity
    - discount
    """
    return (
        # Keep one row per order-product pair so downstream revenue is not double-counted.
        df.dropDuplicates(["order_id", "product_id"])
        # Cast numeric fields into stable Spark types for revenue calculations.
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("discount", F.col("discount").cast("double"))
        # Replace missing numeric values with safe defaults.
        .fillna({"unit_price": 0.0, "quantity": 0, "discount": 0.0})
    )
