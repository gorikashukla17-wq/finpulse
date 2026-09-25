"""SparkSession factory with settings suited to a single-machine batch job."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "finpulse", master: str = "local[*]") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")      # small data: avoid 200 tiny tasks
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
