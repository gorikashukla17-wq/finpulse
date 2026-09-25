import datetime as dt
import math

import numpy as np
import pandas as pd

from finpulse.analytics import add_rolling_metrics


def _prices(spark, tickers=("AAA", "BBB"), n=80):
    rng = np.random.default_rng(0)
    rows = []
    for t in tickers:
        close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
        for i, c in enumerate(close):
            rows.append((t, dt.date(2024, 1, 1) + dt.timedelta(days=i), float(c)))
    return spark.createDataFrame(rows, "ticker string, trade_date date, close double"), pd.DataFrame(
        rows, columns=["ticker", "trade_date", "close"])


def test_metrics_match_pandas_reference(spark):
    sdf, pdf = _prices(spark)
    got = add_rolling_metrics(sdf).toPandas().sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    exp = pdf.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    g = exp.groupby("ticker")["close"]
    exp["daily_return"] = g.pct_change()
    exp["log_return"] = np.log(exp["close"] / g.shift())
    exp["ma_20"] = g.transform(lambda s: s.rolling(20).mean())
    exp["ma_50"] = g.transform(lambda s: s.rolling(50).mean())
    exp["volatility_20d"] = exp.groupby("ticker")["log_return"].transform(
        lambda s: s.rolling(20).std()) * math.sqrt(252)
    for col in ["daily_return", "log_return", "ma_20", "ma_50", "volatility_20d"]:
        pd.testing.assert_series_equal(got[col], exp[col], check_names=False, rtol=1e-9)


def test_windows_do_not_mix_instruments(spark):
    sdf, _ = _prices(spark)
    got = add_rolling_metrics(sdf).toPandas()
    first = got.sort_values("trade_date").groupby("ticker").head(1)
    assert first["daily_return"].isna().all()      # each ticker starts fresh


def test_same_job_scales_from_one_ticker_to_many(spark):
    one, _ = _prices(spark, tickers=("AAA",))
    many, _ = _prices(spark, tickers=("AAA", "BBB", "CCC"))
    a = add_rolling_metrics(one).toPandas().sort_values("trade_date").reset_index(drop=True)
    b = (add_rolling_metrics(many).filter("ticker = 'AAA'").toPandas()
         .sort_values("trade_date").reset_index(drop=True))
    pd.testing.assert_frame_equal(a, b)
