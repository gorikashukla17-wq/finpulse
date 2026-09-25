"""Staging layer: parse, validate and de-duplicate raw price files in PySpark.

Every raw row ends up in exactly one of two places:

* ``clean``   – typed, validated, one row per (ticker, trade_date), ready for the warehouse;
* ``rejects`` – the raw record, its source file and *every* rule it failed.

So bad source data is stopped here and never reaches downstream reporting, and each
rejected row can be traced back to the file it came from.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = ["ticker", "trade_date", "open", "high", "low", "close", "volume"]
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])
PRICE_COLS = ["open", "high", "low", "close"]


@dataclass
class StagingResult:
    clean: DataFrame
    rejects: DataFrame
    rows_read: int
    rows_clean: int
    rows_rejected: int
    reject_counts: dict
    source_files: list


def read_raw(spark: SparkSession, raw_dir: str | Path) -> DataFrame:
    """Reads every prices_*.csv as strings, so malformed values are caught by rules, not the parser."""
    paths = sorted(str(p) for p in Path(raw_dir).glob("prices_*.csv"))
    if not paths:
        raise FileNotFoundError(f"No prices_*.csv files in {raw_dir}")
    return (
        spark.read.option("header", True).schema(RAW_SCHEMA).csv(paths)
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("raw_record", F.concat_ws(",", *[F.coalesce(F.col(c), F.lit("")) for c in RAW_COLUMNS]))
    )


def read_instruments(spark: SparkSession, raw_dir: str | Path) -> DataFrame:
    return spark.read.option("header", True).csv(str(Path(raw_dir) / "instruments.csv"))


def validate(raw: DataFrame, instruments: DataFrame) -> StagingResult:
    typed = raw.select(
        F.upper(F.trim("ticker")).alias("ticker"),
        F.col("trade_date").alias("trade_date_raw"),
        F.expr("to_date(try_to_timestamp(trade_date, 'yyyy-MM-dd'))").alias("trade_date"),
        *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in PRICE_COLS],
        F.expr("try_cast(volume AS BIGINT)").alias("volume"),
        "source_file", "raw_record",
    )
    known = instruments.select(F.upper(F.trim("ticker")).alias("_known_ticker")).distinct()
    typed = typed.join(F.broadcast(known), typed.ticker == known._known_ticker, "left")

    rules = {
        "unparseable_date": F.col("trade_date").isNull(),
        "missing_value": F.greatest(*[F.col(c).isNull().cast("int") for c in PRICE_COLS + ["volume"]]) == 1,
        "non_positive_price": F.least(*[F.coalesce(F.col(c), F.lit(1.0)) for c in PRICE_COLS]) <= 0,
        "ohlc_inconsistent": (F.col("high") < F.col("low"))
                             | (F.col("high") < F.greatest("open", "close"))
                             | (F.col("low") > F.least("open", "close")),
        "negative_volume": F.col("volume") < 0,
        "unknown_instrument": F.col("_known_ticker").isNull(),
    }
    reason = F.concat_ws(";", *[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    checked = typed.withColumn("reject_reason", reason).drop("_known_ticker")
    checked = checked.cache()

    passed = checked.filter(F.col("reject_reason") == "")
    failed = checked.filter(F.col("reject_reason") != "")

    # Exact duplicates: keep one copy, record the extras.
    key_all = ["ticker", "trade_date"] + PRICE_COLS + ["volume"]
    w_exact = Window.partitionBy(*key_all).orderBy("source_file", "raw_record")
    ranked = passed.withColumn("_dup_rank", F.row_number().over(w_exact))
    exact_dupes = ranked.filter("_dup_rank > 1").withColumn("reject_reason", F.lit("exact_duplicate"))
    distinct_rows = ranked.filter("_dup_rank = 1")

    # Conflicting duplicates: same ticker+date, different values. Neither version can be
    # trusted, so all versions are rejected rather than silently picking one.
    w_key = Window.partitionBy("ticker", "trade_date")
    with_count = distinct_rows.withColumn("_versions", F.count(F.lit(1)).over(w_key))
    conflicts = with_count.filter("_versions > 1").withColumn("reject_reason", F.lit("conflicting_duplicate"))
    clean = with_count.filter("_versions = 1").drop("_dup_rank", "_versions", "reject_reason", "trade_date_raw",
                                                   "raw_record")

    reject_cols = ["source_file", "ticker", "trade_date_raw", "reject_reason", "raw_record"]
    rejects = (failed.select(*reject_cols)
               .unionByName(exact_dupes.select(*reject_cols))
               .unionByName(conflicts.select(*reject_cols)))

    clean = clean.cache()
    rejects = rejects.cache()
    rows_read = checked.count()
    rows_clean = clean.count()
    counts = {r["reason"]: r["n"] for r in rejects
              .withColumn("reason", F.explode(F.split("reject_reason", ";")))
              .groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()}
    files = sorted(r[0] for r in checked.select("source_file").distinct().collect())
    return StagingResult(clean, rejects, rows_read, rows_clean, rejects.count(), counts, files)
