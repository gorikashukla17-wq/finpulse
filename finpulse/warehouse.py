"""Star-schema warehouse: DDL, dimension/fact loading and ETL run bookkeeping.

                    dim_date
                       │
    dim_sector ── fact_daily_price ── dim_instrument ── dim_sector
                       │
                  etl_load_run ── etl_rejected_row

Analytical queries join the narrow fact table to small dimensions on indexed integer keys,
instead of scanning a wide flat table and grouping on repeated strings.

Defined with SQLAlchemy Core, so the same code creates the schema on MySQL (production) or
SQLite (local runs and tests).
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
from sqlalchemy import (BigInteger, Column, Date, DateTime, Float, ForeignKey, Index, Integer, MetaData,
                        PrimaryKeyConstraint, String, Table, Text, create_engine, delete, insert, select,
                        text, update)
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateIndex, CreateTable

metadata = MetaData()

dim_sector = Table(
    "dim_sector", metadata,
    Column("sector_key", Integer, primary_key=True, autoincrement=True),
    Column("sector_name", String(60), nullable=False, unique=True),
)

dim_instrument = Table(
    "dim_instrument", metadata,
    Column("instrument_key", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(12), nullable=False, unique=True),
    Column("company_name", String(120), nullable=False),
    Column("exchange", String(10), nullable=False),
    Column("sector_key", Integer, ForeignKey("dim_sector.sector_key"), nullable=False),
    Column("listing_date", Date),
)

dim_date = Table(
    "dim_date", metadata,
    Column("date_key", Integer, primary_key=True, autoincrement=False),   # yyyymmdd
    Column("full_date", Date, nullable=False, unique=True),
    Column("year", Integer, nullable=False),
    Column("quarter", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("month_name", String(9), nullable=False),
    Column("week_of_year", Integer, nullable=False),
    Column("day_of_week", Integer, nullable=False),          # 1 = Monday
    Column("day_name", String(9), nullable=False),
    Column("is_month_end", Integer, nullable=False),
)

etl_load_run = Table(
    "etl_load_run", metadata,
    Column("load_run_id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime, nullable=False),
    Column("finished_at", DateTime),
    Column("status", String(12), nullable=False),               # RUNNING / SUCCEEDED / FAILED
    Column("source_files", Text),
    Column("rows_read", Integer),
    Column("rows_loaded", Integer),
    Column("rows_rejected", Integer),
    Column("error_message", Text),
)

fact_daily_price = Table(
    "fact_daily_price", metadata,
    Column("instrument_key", Integer, ForeignKey("dim_instrument.instrument_key"), nullable=False),
    Column("date_key", Integer, ForeignKey("dim_date.date_key"), nullable=False),
    Column("sector_key", Integer, ForeignKey("dim_sector.sector_key"), nullable=False),
    Column("open_price", Float, nullable=False),
    Column("high_price", Float, nullable=False),
    Column("low_price", Float, nullable=False),
    Column("close_price", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("daily_return", Float),
    Column("log_return", Float),
    Column("ma_20", Float),
    Column("ma_50", Float),
    Column("volatility_20d", Float),
    Column("load_run_id", Integer, ForeignKey("etl_load_run.load_run_id"), nullable=False),
    PrimaryKeyConstraint("instrument_key", "date_key"),
)
Index("ix_fact_date", fact_daily_price.c.date_key)
Index("ix_fact_sector_date", fact_daily_price.c.sector_key, fact_daily_price.c.date_key)

etl_rejected_row = Table(
    "etl_rejected_row", metadata,
    Column("reject_id", Integer, primary_key=True, autoincrement=True),
    Column("load_run_id", Integer, ForeignKey("etl_load_run.load_run_id"), nullable=False),
    Column("source_file", String(255), nullable=False),
    Column("ticker", String(12)),
    Column("trade_date_raw", String(20)),
    Column("reject_reason", String(200), nullable=False),
    Column("raw_record", Text),
)
Index("ix_reject_run", etl_rejected_row.c.load_run_id)

VIEWS = {
    # Latest analytics per instrument (dashboard "instrument" table)
    "vw_instrument_latest": """
        SELECT i.ticker, i.company_name, s.sector_name, d.full_date, f.close_price, f.daily_return,
               f.ma_20, f.ma_50, f.volatility_20d
        FROM fact_daily_price f
        JOIN dim_instrument i ON i.instrument_key = f.instrument_key
        JOIN dim_sector s ON s.sector_key = f.sector_key
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.date_key = (SELECT MAX(f2.date_key) FROM fact_daily_price f2
                            WHERE f2.instrument_key = f.instrument_key)""",
    # Sector x month roll-up (dashboard drill-down: year > quarter > month > sector > ticker)
    "vw_sector_monthly": """
        SELECT s.sector_name, d.year, d.quarter, d.month,
               AVG(f.daily_return) AS avg_daily_return,
               AVG(f.volatility_20d) AS avg_volatility,
               SUM(f.close_price * f.volume) AS traded_value,
               COUNT(*) AS price_rows
        FROM fact_daily_price f
        JOIN dim_sector s ON s.sector_key = f.sector_key
        JOIN dim_date d ON d.date_key = f.date_key
        GROUP BY s.sector_name, d.year, d.quarter, d.month""",
    # Data-quality panel: one row per load run
    "vw_data_quality": """
        SELECT r.load_run_id, r.started_at, r.finished_at, r.status, r.rows_read, r.rows_loaded,
               r.rows_rejected,
               CASE WHEN r.rows_read > 0 THEN 1.0 * r.rows_rejected / r.rows_read END AS reject_rate
        FROM etl_load_run r""",
}


def get_engine(db_url: str) -> Engine:
    return create_engine(db_url, future=True)


def create_schema(engine: Engine) -> None:
    metadata.create_all(engine)
    with engine.begin() as conn:
        for name, sql in VIEWS.items():
            conn.execute(text(f"DROP VIEW IF EXISTS {name}"))
            conn.execute(text(f"CREATE VIEW {name} AS {sql}"))


def ddl_for(dialect_name: str) -> str:
    """Renders the schema as SQL for a given dialect (used to produce sql/schema_mysql.sql)."""
    from sqlalchemy.dialects import mysql, sqlite
    dialect = {"mysql": mysql.dialect(), "sqlite": sqlite.dialect()}[dialect_name]
    parts = []
    for table in metadata.sorted_tables:
        parts.append(str(CreateTable(table).compile(dialect=dialect)).strip() + ";")
        for idx in table.indexes:
            parts.append(str(CreateIndex(idx).compile(dialect=dialect)).strip() + ";")
    for name, sql in VIEWS.items():
        parts.append(f"CREATE OR REPLACE VIEW {name} AS{sql};" if dialect_name == "mysql"
                     else f"CREATE VIEW {name} AS{sql};")
    return "\n\n".join(parts) + "\n"


# ---------------------------------------------------------------- load runs

def start_run(engine: Engine, source_files: list[str]) -> int:
    with engine.begin() as conn:
        res = conn.execute(insert(etl_load_run).values(
            started_at=dt.datetime.now(), status="RUNNING", source_files=",".join(source_files)))
        return int(res.inserted_primary_key[0])


def finish_run(engine: Engine, run_id: int, status: str, rows_read=None, rows_loaded=None,
               rows_rejected=None, error_message=None) -> None:
    with engine.begin() as conn:
        conn.execute(update(etl_load_run).where(etl_load_run.c.load_run_id == run_id).values(
            finished_at=dt.datetime.now(), status=status, rows_read=rows_read, rows_loaded=rows_loaded,
            rows_rejected=rows_rejected, error_message=error_message))


# ---------------------------------------------------------------- dimensions

def upsert_dimensions(engine: Engine, instruments: pd.DataFrame, dates: pd.Series) -> None:
    """Inserts sectors, instruments and calendar days that are not in the warehouse yet."""
    with engine.begin() as conn:
        have = set(conn.execute(select(dim_sector.c.sector_name)).scalars())
        new = sorted(set(instruments["sector"]) - have)
        if new:
            conn.execute(insert(dim_sector), [{"sector_name": s} for s in new])
        sector_keys = dict(conn.execute(select(dim_sector.c.sector_name, dim_sector.c.sector_key)).all())

        have = set(conn.execute(select(dim_instrument.c.ticker)).scalars())
        rows = [{"ticker": r.ticker, "company_name": r.company_name, "exchange": r.exchange,
                 "sector_key": sector_keys[r.sector],
                 "listing_date": pd.to_datetime(r.listing_date).date() if pd.notna(r.listing_date) else None}
                for r in instruments.itertuples() if r.ticker not in have]
        if rows:
            conn.execute(insert(dim_instrument), rows)

        have = set(conn.execute(select(dim_date.c.date_key)).scalars())
        days = pd.to_datetime(pd.Series(sorted(set(dates))))
        rows = []
        for d in days:
            key = int(d.strftime("%Y%m%d"))
            if key in have:
                continue
            rows.append({"date_key": key, "full_date": d.date(), "year": d.year, "quarter": d.quarter,
                         "month": d.month, "month_name": d.strftime("%B"), "week_of_year": int(d.isocalendar()[1]),
                         "day_of_week": d.isoweekday(), "day_name": d.strftime("%A"),
                         "is_month_end": int(d.is_month_end)})
        if rows:
            conn.execute(insert(dim_date), rows)


def instrument_keys(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(select(dim_instrument.c.ticker, dim_instrument.c.instrument_key,
                                  dim_instrument.c.sector_key), conn)


# ---------------------------------------------------------------- facts

def load_facts(engine: Engine, facts: pd.DataFrame, run_id: int, chunk: int = 5_000) -> int:
    """Idempotent load: rows for the same (instrument, date) are replaced, never duplicated."""
    keys = instrument_keys(engine)
    df = facts.merge(keys, on="ticker", how="inner")
    df["date_key"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d").astype(int)
    df = df.rename(columns={"open": "open_price", "high": "high_price", "low": "low_price", "close": "close_price"})
    cols = ["instrument_key", "date_key", "sector_key", "open_price", "high_price", "low_price", "close_price",
            "volume", "daily_return", "log_return", "ma_20", "ma_50", "volatility_20d"]
    df = df[cols].astype(object).where(df[cols].notna(), None)
    df["load_run_id"] = run_id
    records = df.to_dict("records")

    with engine.begin() as conn:
        for inst, part in df.groupby("instrument_key"):
            conn.execute(delete(fact_daily_price).where(
                (fact_daily_price.c.instrument_key == int(inst))
                & fact_daily_price.c.date_key.between(int(part["date_key"].min()), int(part["date_key"].max()))))
        for i in range(0, len(records), chunk):
            conn.execute(insert(fact_daily_price), records[i:i + chunk])
    return len(records)


def load_rejects(engine: Engine, rejects: pd.DataFrame, run_id: int) -> int:
    if rejects.empty:
        return 0
    rows = rejects.assign(load_run_id=run_id)[
        ["load_run_id", "source_file", "ticker", "trade_date_raw", "reject_reason", "raw_record"]]
    rows = rows.astype(object).where(rows.notna(), None).to_dict("records")
    with engine.begin() as conn:
        conn.execute(insert(etl_rejected_row), rows)
    return len(rows)
