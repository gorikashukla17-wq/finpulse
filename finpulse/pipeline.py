"""FinPulse ETL: raw files → Spark staging/validation → Spark analytics → star-schema warehouse.

    python -m finpulse.pipeline                                   # SQLite warehouse at finpulse.db
    python -m finpulse.pipeline --db-url "mysql+pymysql://user:pw@localhost/finpulse"
    python -m finpulse.pipeline --tickers INFX,TCSN               # same job, subset of the universe

Every run gets a ``load_run_id``. Fact rows and rejected rows carry it, so every number on
the dashboard can be traced to the run (and source file) that produced it.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from pyspark.sql import functions as F

from . import warehouse as wh
from .analytics import add_rolling_metrics
from .spark import get_spark
from .staging import read_instruments, read_raw, validate


def run_pipeline(raw_dir: str | Path = "data/raw", db_url: str = "sqlite:///finpulse.db",
                 tickers: list[str] | None = None, spark=None) -> dict:
    t0 = time.time()
    spark = spark or get_spark()
    engine = wh.get_engine(db_url)
    wh.create_schema(engine)

    raw = read_raw(spark, raw_dir)
    instruments_sdf = read_instruments(spark, raw_dir)
    if tickers:
        wanted = [t.upper() for t in tickers]
        raw = raw.filter(F.upper(F.trim("ticker")).isin(wanted))
        instruments_sdf = instruments_sdf.filter(F.col("ticker").isin(wanted))

    files = sorted(p.name for p in Path(raw_dir).glob("prices_*.csv"))
    run_id = wh.start_run(engine, files)
    try:
        staged = validate(raw, instruments_sdf)

        instruments = instruments_sdf.toPandas()
        enriched = add_rolling_metrics(staged.clean).join(
            instruments_sdf.select("ticker", "sector"), on="ticker", how="left")
        facts = enriched.toPandas()

        wh.upsert_dimensions(engine, instruments, facts["trade_date"])
        loaded = wh.load_facts(engine, facts, run_id)
        wh.load_rejects(engine, staged.rejects.toPandas(), run_id)
        wh.finish_run(engine, run_id, "SUCCEEDED", staged.rows_read, loaded, staged.rows_rejected)
    except Exception as exc:
        wh.finish_run(engine, run_id, "FAILED", error_message=repr(exc)[:2000])
        raise

    return {"load_run_id": run_id, "rows_read": staged.rows_read, "rows_loaded": loaded,
            "rows_rejected": staged.rows_rejected, "reject_counts": staged.reject_counts,
            "source_files": staged.source_files, "seconds": round(time.time() - t0, 1)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--db-url", default="sqlite:///finpulse.db")
    ap.add_argument("--tickers", help="comma-separated subset, e.g. INFX,TCSN")
    ap.add_argument("--generate", action="store_true", help="generate synthetic source files first")
    a = ap.parse_args()

    if a.generate or not any(Path(a.raw_dir).glob("prices_*.csv")):
        from .generate import generate
        info = generate(a.raw_dir)
        print(f"Generated {info['rows']:,} raw rows in {a.raw_dir}")

    res = run_pipeline(a.raw_dir, a.db_url, a.tickers.split(",") if a.tickers else None)
    print(f"Load run #{res['load_run_id']} finished in {res['seconds']} s")
    print(f"  read {res['rows_read']:,} | loaded {res['rows_loaded']:,} | rejected {res['rows_rejected']:,}")
    for reason, n in sorted(res["reject_counts"].items(), key=lambda kv: -kv[1]):
        print(f"    {reason:<24}{n:>6,}")


if __name__ == "__main__":
    main()
