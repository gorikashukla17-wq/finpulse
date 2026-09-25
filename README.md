# FinPulse: Market Data Warehouse & Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![PySpark](https://img.shields.io/badge/PySpark-ETL-E25A1C) ![MySQL 8](https://img.shields.io/badge/MySQL-8-blue) ![Tests](https://img.shields.io/badge/tests-8%20passing-brightgreen) ![License: MIT](https://img.shields.io/badge/license-MIT-green)

An end-to-end ETL pipeline that ingests multi-year equity price and volume files,
**validates and de-duplicates them in a PySpark staging layer**, computes **rolling analytics
with Spark window functions**, and loads a **star-schema warehouse** (MySQL, or SQLite
locally). The warehouse feeds a Power BI dashboard with sector drill-down and a
**data-quality panel**.

**Stack:** Python · PySpark · SQLAlchemy · MySQL 8 / SQLite · Pandas · Power BI (DAX) · Matplotlib · pytest

## Highlights

- Loads **47,000+ rows across 6 years and 31 instruments** in about 23 seconds.
- **8 validation rules** in a PySpark staging layer. Every rejected row is kept with its reasons, so nothing is lost silently.
- A **star-schema warehouse** with idempotent loads, where every fact row traces back to its load run.
- Rolling returns, moving averages and volatility computed with **Spark window functions**, feeding a **Power BI** dashboard.

## Contents

[Pipeline](#pipeline) · [Staging](#1-staging--validation-finpulsestagingpy) · [Star schema](#2-star-schema-finpulsewarehousepy-sqlschema_mysqlsql) · [Analytics](#3-rolling-analytics-finpulseanalyticspy) · [Dashboard](#4-dashboard-powerbi) · [Running it](#running-it) · [Project layout](#project-layout) · [Author](#author) · [License](#license)

![Dashboard preview](docs/dashboard_preview.png)

---

## Pipeline

```
data/raw/prices_2019..2024.csv  ─┐
data/raw/instruments.csv        ─┤
                                 ▼
            ┌──────────────── STAGING (PySpark) ─────────────────┐
            │ read every column as string (the parser never drops │
            │ bad rows silently) → typed with try_cast            │
            │ 6 validation rules → reject_reason lists every rule │
            │ exact duplicates → keep one; conflicting → reject   │
            └──────────┬────────────────────────────┬─────────────┘
                 clean rows                    rejected rows
                       ▼                             ▼
          ANALYTICS (Spark windows)           etl_rejected_row
          partitionBy(ticker)                 (source file + raw record)
          returns, MA20/50, volatility
                       ▼
            WAREHOUSE (star schema, idempotent load)
            dim_date · dim_instrument · dim_sector · fact_daily_price
                       ▼
            Power BI dashboard  +  data-quality panel (etl_load_run)
```

A full run over 6 years × 31 instruments:

```
Load run #1 finished in 23.0 s
  read 47,733 | loaded 46,823 | rejected 910
    missing_value              190
    ohlc_inconsistent          189
    conflicting_duplicate      188
    exact_duplicate            187
    non_positive_price          95
    negative_volume             63
    unknown_instrument          47
    unparseable_date            47
```

`read = loaded + rejected` holds on every run, so no row is silently lost.

## 1. Staging & validation: `finpulse/staging.py`

Every rule is evaluated on every row, and a rejected row records **all** the rules it broke
(e.g. `unknown_instrument;non_positive_price`), together with its source file and the original
raw text. Nothing that fails a rule can reach reporting.

| rule | catches |
|---|---|
| `unparseable_date` | `31/13/2021` and other malformed dates |
| `missing_value` | null or non-numeric OHLCV fields |
| `non_positive_price` | zero or negative prices |
| `ohlc_inconsistent` | `high < low`, or high/low not bracketing open and close |
| `negative_volume` | volume < 0 |
| `unknown_instrument` | ticker not in the instrument reference file |
| `exact_duplicate` | identical row seen again: one copy kept, extras recorded |
| `conflicting_duplicate` | same ticker + date with *different* values: **all** versions rejected, because picking one silently would be a guess |

## 2. Star schema: `finpulse/warehouse.py`, [`sql/schema_mysql.sql`](sql/schema_mysql.sql)

```
             dim_date (date_key yyyymmdd, year, quarter, month, …)
                    │
dim_sector ── fact_daily_price ── dim_instrument ── dim_sector
                    │  PK (instrument_key, date_key)
                    │  OHLCV + daily_return, log_return, ma_20, ma_50, volatility_20d
                    │  load_run_id
               etl_load_run ── etl_rejected_row
```

**Why a star schema instead of one flat table?** Analytical queries ("average return by sector
by quarter") filter and group on small dimensions and join to the fact table on **indexed
integer keys** (`ix_fact_sector_date`, `ix_fact_date`). A flat table would repeat sector and
company strings on every row and force scans and grouping on text. The calendar dimension also
gives Power BI a ready-made Year → Quarter → Month hierarchy.

- **Idempotent loads:** re-running replaces rows for the same `(instrument, date)` and never duplicates them. A test runs the pipeline twice and checks the row counts.
- **Traceability:** every fact row carries `load_run_id`, and `etl_load_run` records start and finish times, source files, and rows read / loaded / rejected. Every dashboard figure traces back to a specific load run.
- **Views for BI:** `vw_sector_monthly`, `vw_instrument_latest`, `vw_data_quality`.
- The schema is defined once with SQLAlchemy Core, so the same code creates it on MySQL or SQLite.

## 3. Rolling analytics: `finpulse/analytics.py`

```python
by_ticker = Window.partitionBy("ticker").orderBy("trade_date")
daily_return   = close / lag(close) - 1
ma_20, ma_50   = avg(close) over rowsBetween(-19, 0) / (-49, 0)
volatility_20d = stddev(log_return) over 20 rows × √252
```

Because every window is **partitioned by instrument**, each ticker's history is computed
independently and in parallel. The same job runs unchanged for one ticker (`--tickers INFX`)
or the whole universe; there's a test for exactly that. Rolling values stay NULL until their
window is full, because a "20-day average" over 3 days would be misleading. The results are
also checked against a pandas reference implementation.

## 4. Dashboard: [`powerbi/`](powerbi/)

- [`powerbi/README.md`](powerbi/README.md): how to connect (live MySQL or CSV extracts), the relationships, and the page layout.
- [`powerbi/measures.dax`](powerbi/measures.dax): annualised return, volatility, traded value, latest run, reject rate.
- `python -m finpulse.export` writes the CSV extracts and regenerates `docs/dashboard_preview.png` from the warehouse.

The data-quality panel shows rejected-row counts by rule and the latest load timestamp, so
anyone reading the dashboard can see how clean the data behind it is.

## Running it

Requires Python 3.10+ and Java 17+ (for Spark).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m finpulse.pipeline --generate      # generate source files, run ETL → finpulse.db (SQLite)
python -m finpulse.export                   # Power BI extracts + dashboard preview
pytest -q                                   # 8 tests (Spark runs locally)
```

With MySQL:

```bash
docker compose up -d
python -m finpulse.pipeline --db-url "mysql+pymysql://finpulse:finpulse@localhost:3306/finpulse"
```

Example analytical SQL is in [`sql/example_queries.sql`](sql/example_queries.sql) (sector
returns, golden crosses, most volatile names, traceability, reject drill-down).

## Project layout

```
finpulse/
  generate.py     synthetic multi-year OHLCV files with realistic defects
  spark.py        SparkSession factory
  staging.py      typed parsing, validation rules, de-duplication, reject capture
  analytics.py    window-function returns, moving averages, rolling volatility
  warehouse.py    star schema (SQLAlchemy Core), views, idempotent loads, run bookkeeping
  pipeline.py     CLI orchestrating a load run
  export.py       Power BI extracts + dashboard preview
sql/              generated MySQL DDL, example queries
powerbi/          connection guide, model, DAX measures
tests/            staging rules, analytics vs pandas, end-to-end + idempotency
```

## Notes

- The source data is synthetic: geometric Brownian motion with a sector-level common factor, so sectors move together as they do in real markets. The tickers are fictional.
- Each run processes the files it is given in full. An incremental mode would load only new dates, and would need a 50-row look-back from the warehouse so the moving averages stay correct.

## Author

**Gorika Shukla** · GitHub [@gorikashukla17-wq](https://github.com/gorikashukla17-wq)

## License

Released under the [MIT License](LICENSE).
