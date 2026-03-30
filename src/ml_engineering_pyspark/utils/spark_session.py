from pathlib import Path

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "ml-engineering-pyspark",
    jdbc_jar_path: str | Path | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
    )

    if jdbc_jar_path:
        builder = builder.config("spark.jars", str(jdbc_jar_path))

    return builder.getOrCreate()
