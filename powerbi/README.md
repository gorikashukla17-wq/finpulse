# Power BI dashboard

The warehouse is designed to be the **only** source for the dashboard, so every figure can be
traced to a load run.

## Connect

**Option A: live MySQL.** *Get Data → MySQL database* → server `localhost`, database `finpulse`.
Import `dim_sector`, `dim_instrument`, `dim_date`, `fact_daily_price`, `etl_load_run`,
`etl_rejected_row` (and optionally the `vw_*` views).

**Option B: CSV extracts (no server).** Run `python -m finpulse.export`, then *Get Data → Text/CSV*
on each file in `powerbi/extracts/`.

## Model

| From (1) | To (*) | Key |
|---|---|---|
| dim_instrument | fact_daily_price | instrument_key |
| dim_sector | fact_daily_price | sector_key |
| dim_sector | dim_instrument | sector_key |
| dim_date | fact_daily_price | date_key |
| etl_load_run | fact_daily_price | load_run_id |
| etl_load_run | etl_rejected_row | load_run_id |

Mark `dim_date` as the date table (column `full_date`). Paste the measures from
[`measures.dax`](measures.dax).

## Pages

1. **Market overview**: line chart of the sector index (Annualised Return by `full_date`, legend
   = `sector_name`); bar chart of Annualised Return by sector, with a **drill-down hierarchy
   Sector → Instrument** and date slicer Year → Quarter → Month.
2. **Instrument**: `close_price`, `ma_20`, `ma_50` over time for the selected ticker, plus a
   Volatility (20d) card.
3. **Data quality**: cards for *Latest Run Id*, *Latest Load Time*, *Rows Rejected*,
   *Reject Rate*; bar chart of `etl_rejected_row[reject_reason]` counts; table of rejected rows
   with `source_file` and `raw_record` for investigation.

A static preview of these pages, rendered from the same warehouse with matplotlib, is in
[`../docs/dashboard_preview.png`](../docs/dashboard_preview.png). The `.pbix` file itself isn't
committed because it's a binary built in Power BI Desktop.
