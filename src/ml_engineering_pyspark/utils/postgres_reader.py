from pyspark.sql import DataFrame, SparkSession

from ml_engineering_pyspark.utils.jdbc import DEFAULT_POSTGRES_DRIVER


def read_table(
    spark: SparkSession,
    jdbc_url: str,
    table: str,
    user: str,
    password: str,
    driver: str = DEFAULT_POSTGRES_DRIVER,
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .load()
    )
