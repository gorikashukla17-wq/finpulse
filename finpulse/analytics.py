"""Rolling analytics in PySpark window functions.

Every window is ``partitionBy("ticker").orderBy("trade_date")``, so each instrument's
history is processed independently and in parallel. The same job therefore runs unchanged
for one ticker or the whole universe; Spark just gets more partitions to spread.
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TRADING_DAYS = 252


def add_rolling_metrics(prices: DataFrame, short: int = 20, long: int = 50, vol_window: int = 20) -> DataFrame:
    """Adds daily_return, log_return, ma_<short>, ma_<long> and annualised rolling volatility.

    Rolling values are left NULL until the window is full; a 20-day average over 3 days
    would be misleading.
    """
    by_ticker = Window.partitionBy("ticker").orderBy("trade_date")
    prev_close = F.lag("close").over(by_ticker)
    df = (prices
          .withColumn("daily_return", F.col("close") / prev_close - 1)
          .withColumn("log_return", F.log(F.col("close") / prev_close))
          .withColumn("_n", F.row_number().over(by_ticker)))

    def rolling(n: int):
        return by_ticker.rowsBetween(-(n - 1), 0)

    df = (df
          .withColumn("ma_20", F.when(F.col("_n") >= short, F.avg("close").over(rolling(short))))
          .withColumn("ma_50", F.when(F.col("_n") >= long, F.avg("close").over(rolling(long))))
          # log_return is NULL on row 1, so a full window of returns needs vol_window + 1 rows.
          .withColumn("volatility_20d",
                      F.when(F.col("_n") > vol_window,
                             F.stddev_samp("log_return").over(rolling(vol_window)) * math.sqrt(TRADING_DAYS)))
          .drop("_n"))
    return df


def sector_daily(df: DataFrame) -> DataFrame:
    """Equal-weighted sector return per day (used for the dashboard's sector view)."""
    return (df.groupBy("sector", "trade_date")
            .agg(F.avg("daily_return").alias("sector_return"),
                 F.sum(F.col("close") * F.col("volume")).alias("traded_value"),
                 F.count(F.lit(1)).alias("instruments")))
