from ml_engineering_pyspark.transforms.cleaning import clean_order_details
from ml_engineering_pyspark.transforms.processing import (
    add_revenue_features,
    aggregate_product_revenue,
)


def test_clean_order_details_casts_values_and_deduplicates(spark):
    source_df = spark.createDataFrame(
        [
            ("10248", "11", "14.0", "12", "0.0"),
            ("10248", "11", "14.0", "12", "0.0"),
            ("10249", "42", None, None, None),
        ],
        ["order_id", "product_id", "unit_price", "quantity", "discount"],
    )

    actual_rows = clean_order_details(source_df).orderBy("order_id", "product_id").collect()

    assert len(actual_rows) == 2
    assert actual_rows[0]["unit_price"] == 14.0
    assert actual_rows[0]["quantity"] == 12
    assert actual_rows[0]["discount"] == 0.0
    assert actual_rows[1]["unit_price"] == 0.0
    assert actual_rows[1]["quantity"] == 0
    assert actual_rows[1]["discount"] == 0.0


def test_add_revenue_features_adds_expected_column(spark):
    source_df = spark.createDataFrame(
        [("11", "Queso Cabrales", 14.0, 12, 0.25)],
        ["product_id", "product_name", "unit_price", "quantity", "discount"],
    )

    row = add_revenue_features(source_df).collect()[0]

    assert row["revenue"] == 126.0


def test_aggregate_product_revenue_sums_revenue_and_quantity(spark):
    source_df = spark.createDataFrame(
        [
            ("11", "Queso Cabrales", 126.0, 12),
            ("11", "Queso Cabrales", 70.0, 5),
            ("42", "Singaporean Hokkien Fried Mee", 100.0, 4),
        ],
        ["product_id", "product_name", "revenue", "quantity"],
    )

    rows = aggregate_product_revenue(source_df).collect()

    assert rows[0]["product_id"] == "11"
    assert rows[0]["product_name"] == "Queso Cabrales"
    assert rows[0]["total_revenue"] == 196.0
    assert rows[0]["total_quantity"] == 17
