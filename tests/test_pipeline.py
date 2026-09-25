import pandas as pd
from sqlalchemy import text

from finpulse.generate import generate
from finpulse.pipeline import run_pipeline
from finpulse.warehouse import get_engine


def test_end_to_end_load_is_traceable_and_idempotent(spark, tmp_path):
    raw = tmp_path / "raw"
    info = generate(raw, start="2023-01-01", end="2023-12-31", seed=11)
    db = f"sqlite:///{tmp_path / 'wh.db'}"

    r1 = run_pipeline(raw, db, spark=spark)
    assert r1["rows_read"] == info["rows"]
    assert r1["rows_loaded"] + r1["rows_rejected"] == r1["rows_read"]
    assert r1["rows_rejected"] > 0

    r2 = run_pipeline(raw, db, spark=spark)
    engine = get_engine(db)
    with engine.connect() as c:
        facts = c.execute(text("SELECT COUNT(*) FROM fact_daily_price")).scalar()
        dup_keys = c.execute(text("SELECT COUNT(*) FROM (SELECT instrument_key, date_key FROM fact_daily_price "
                                  "GROUP BY instrument_key, date_key HAVING COUNT(*) > 1) x")).scalar()
        runs = pd.read_sql(text("SELECT * FROM vw_data_quality ORDER BY load_run_id"), c)
        fact_run = c.execute(text("SELECT DISTINCT load_run_id FROM fact_daily_price")).scalars().all()
        dims = c.execute(text("SELECT COUNT(*) FROM dim_instrument")).scalar()
        orphan = c.execute(text("SELECT COUNT(*) FROM fact_daily_price f LEFT JOIN dim_date d "
                                "ON d.date_key = f.date_key WHERE d.date_key IS NULL")).scalar()
        bad = c.execute(text("SELECT COUNT(*) FROM fact_daily_price WHERE close_price <= 0 "
                             "OR high_price < low_price OR volume < 0")).scalar()

    assert facts == r1["rows_loaded"] == r2["rows_loaded"]     # re-run replaced, not duplicated
    assert dup_keys == 0 and orphan == 0 and bad == 0
    assert list(runs["status"]) == ["SUCCEEDED", "SUCCEEDED"]
    assert fact_run == [r2["load_run_id"]]                      # every fact row points at its run
    assert dims == info["instruments"]


def test_ticker_subset_uses_same_job(spark, tmp_path):
    raw = tmp_path / "raw"
    generate(raw, start="2024-01-01", end="2024-06-30", seed=3)
    db = f"sqlite:///{tmp_path / 'wh.db'}"
    res = run_pipeline(raw, db, tickers=["INFX"], spark=spark)
    with get_engine(db).connect() as c:
        tickers = c.execute(text("SELECT DISTINCT i.ticker FROM fact_daily_price f JOIN dim_instrument i "
                                 "ON i.instrument_key = f.instrument_key")).scalars().all()
    assert tickers == ["INFX"] and res["rows_loaded"] > 100
