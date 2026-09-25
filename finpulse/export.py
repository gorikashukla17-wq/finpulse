"""Power BI hand-off: CSV extracts of the star schema plus a static dashboard preview image.

    python -m finpulse.export                     # reads finpulse.db, writes powerbi/extracts + docs/
    python -m finpulse.export --ticker INFX       # choose the drill-down instrument in the preview

Power BI Desktop can connect straight to the MySQL warehouse (see powerbi/README.md). The
CSV extracts let you build the dashboard without a database server.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from .warehouse import get_engine  # noqa: E402

TABLES = ["dim_sector", "dim_instrument", "dim_date", "fact_daily_price", "etl_load_run", "etl_rejected_row"]

# Fixed categorical order (colour follows the sector, never its rank).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"


def export_extracts(db_url: str, out_dir: str | Path = "powerbi/extracts") -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    engine = get_engine(db_url)
    paths = []
    with engine.connect() as conn:
        for t in TABLES:
            p = out / f"{t}.csv"
            pd.read_sql(text(f"SELECT * FROM {t}"), conn).to_csv(p, index=False)
            paths.append(p)
    return paths


def _style(ax, title):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=11, color=INK, fontweight="bold", pad=10)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8.5)
    ax.grid(color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def dashboard_preview(db_url: str, out_path: str | Path = "docs/dashboard_preview.png", ticker: str | None = None) -> Path:
    engine = get_engine(db_url)
    with engine.connect() as conn:
        fact = pd.read_sql(text("""
            SELECT i.ticker, s.sector_name, d.full_date, f.close_price, f.daily_return, f.ma_20, f.ma_50,
                   f.volatility_20d
            FROM fact_daily_price f
            JOIN dim_instrument i ON i.instrument_key = f.instrument_key
            JOIN dim_sector s ON s.sector_key = f.sector_key
            JOIN dim_date d ON d.date_key = f.date_key"""), conn, parse_dates=["full_date"])
        run = pd.read_sql(text("SELECT * FROM etl_load_run WHERE status = 'SUCCEEDED' "
                               "ORDER BY load_run_id DESC LIMIT 1"), conn).iloc[0]
        rejects = pd.read_sql(text("SELECT reject_reason FROM etl_rejected_row WHERE load_run_id = :r"),
                              conn, params={"r": int(run.load_run_id)})

    sectors = sorted(fact["sector_name"].unique())
    colour = {s: SERIES[i] for i, s in enumerate(sectors)}

    fig = plt.figure(figsize=(15, 9.5), facecolor="#f9f9f7")
    gs = fig.add_gridspec(2, 2, hspace=0.38, wspace=0.22, left=0.06, right=0.97, top=0.87, bottom=0.07)
    fig.suptitle("FinPulse – market analytics (preview of the Power BI dashboard)", x=0.06, ha="left",
                 fontsize=15, fontweight="bold", color=INK)
    fig.text(0.06, 0.925, f"Load run #{int(run.load_run_id)} · finished {str(run.finished_at)[:19]} · "
             f"{int(run.rows_loaded):,} rows loaded · {int(run.rows_rejected):,} rejected",
             fontsize=10, color=INK2)

    # 1. Sector index, rebased to 100 (equal-weighted daily returns compounded)
    ax = fig.add_subplot(gs[0, 0])
    sec = (fact.groupby(["sector_name", "full_date"])["daily_return"].mean().fillna(0)
           .groupby(level=0).apply(lambda s: 100 * (1 + s).cumprod()).reset_index(level=0, drop=True))
    for s in sectors:
        series = sec.loc[s]
        ax.plot(series.index, series.values, color=colour[s], lw=1.6, label=s)
    ax.axhline(100, color=MUTED, lw=0.8, ls="--")
    _style(ax, "Sector performance, indexed to 100")
    ax.legend(fontsize=8, frameon=False, ncol=2, labelcolor=INK2, loc="upper left")

    # 2. Annualised return vs volatility by sector
    ax = fig.add_subplot(gs[0, 1])
    stats = fact.groupby("sector_name").agg(ret=("daily_return", "mean"), vol=("volatility_20d", "mean"))
    stats["ret"] *= 252
    stats = stats.sort_values("ret")
    ax.barh(stats.index, stats["ret"] * 100, color=[colour[s] for s in stats.index], height=0.6)
    for y, (r, v) in enumerate(zip(stats["ret"], stats["vol"])):
        ax.text(r * 100 + (0.4 if r >= 0 else -0.4), y, f"{r:+.1%}  (vol {v:.0%})", va="center",
                ha="left" if r >= 0 else "right", fontsize=8.5, color=INK2)
    ax.axvline(0, color=MUTED, lw=0.8)
    ax.set_xlabel("Average annualised return, %", color=MUTED, fontsize=9)
    ax.set_xlim(right=stats["ret"].max() * 100 * 1.6)
    _style(ax, "Return by sector (drill-down: sector → instrument)")
    ax.grid(axis="y", visible=False)

    # 3. Instrument drill-down: close with moving averages, last 12 months
    ax = fig.add_subplot(gs[1, 0])
    ticker = ticker or fact["ticker"].iloc[0]
    inst = fact[fact["ticker"] == ticker].sort_values("full_date")
    inst = inst[inst["full_date"] >= inst["full_date"].max() - pd.Timedelta(days=365)]
    for col, label, c, lw in [("close_price", "Close", SERIES[0], 1.4), ("ma_20", "20-day MA", SERIES[1], 2),
                              ("ma_50", "50-day MA", SERIES[2], 2)]:
        ax.plot(inst["full_date"], inst[col], color=c, lw=lw, label=label)
        ax.annotate(label, (inst["full_date"].iloc[-1], inst[col].iloc[-1]), xytext=(4, 0),
                    textcoords="offset points", fontsize=8.5, color=INK2, va="center")
    sector_name = inst["sector_name"].iloc[0]
    _style(ax, f"{ticker} ({sector_name}): close with 20/50-day moving averages")
    ax.legend(fontsize=8, frameon=False, labelcolor=INK2, loc="upper left")
    ax.margins(x=0.08)

    # 4. Data-quality panel
    ax = fig.add_subplot(gs[1, 1])
    counts = rejects["reject_reason"].str.split(";").explode().value_counts().sort_values()
    ax.barh(counts.index, counts.values, color="#256abf", height=0.6)
    for y, v in enumerate(counts.values):
        ax.text(v + counts.max() * 0.01, y, f"{v:,}", va="center", fontsize=8.5, color=INK2)
    rate = run.rows_rejected / run.rows_read
    _style(ax, f"Data quality: {int(run.rows_rejected):,} rows rejected in staging ({rate:.1%})")
    ax.set_xlabel("Rejected rows by rule (a row can fail several)", color=MUTED, fontsize=9)
    ax.grid(axis="y", visible=False)

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-url", default="sqlite:///finpulse.db")
    ap.add_argument("--ticker", default="INFX")
    ap.add_argument("--no-csv", action="store_true")
    a = ap.parse_args()
    if not a.no_csv:
        for p in export_extracts(a.db_url):
            print("wrote", p)
    print("wrote", dashboard_preview(a.db_url, ticker=a.ticker))


if __name__ == "__main__":
    main()
