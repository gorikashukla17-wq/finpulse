"""Generates realistic multi-year equity source files, including the defects real feeds have.

Output (``data/raw`` by default):

* ``instruments.csv`` – reference data: ticker, company, sector, exchange, listing date
* ``prices_<year>.csv`` – one file per year of daily OHLCV rows

Prices follow a geometric Brownian motion with a sector-level common factor, so sectors move
together as they do in real markets. A small share of rows is deliberately corrupted so the
staging layer has something to catch: nulls, negative or zero prices, high < low, unknown
tickers, malformed dates, negative volumes, exact duplicates and *conflicting* duplicates
(same ticker and date, different values).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

SECTORS = {
    "Information Technology": ["INFX", "TCSN", "WIPR", "HCLT", "TEKM"],
    "Financials": ["HDFK", "ICIC", "SBIN", "KOTK", "AXSB"],
    "Energy": ["RELI", "ONGC", "IOCL", "BPCL"],
    "Consumer Staples": ["HUVL", "ITCL", "NEST", "BRIT", "DABR"],
    "Healthcare": ["SUNP", "DRRD", "CIPL", "DIVI"],
    "Industrials": ["LTCO", "SIEM", "ABBI", "BHEL"],
    "Materials": ["TSTL", "JSWS", "ULTC", "HNDL"],
}
SECTOR_DRIFT = {"Information Technology": 0.14, "Financials": 0.11, "Energy": 0.07, "Consumer Staples": 0.09,
                "Healthcare": 0.10, "Industrials": 0.12, "Materials": 0.06}
SECTOR_VOL = {"Information Technology": 0.24, "Financials": 0.26, "Energy": 0.30, "Consumer Staples": 0.16,
              "Healthcare": 0.22, "Industrials": 0.25, "Materials": 0.33}


def generate(out_dir: str | Path = "data/raw", start: str = "2019-01-01", end: str = "2024-12-31",
             defect_rate: float = 0.004, seed: int = 7) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(start, end)
    n = len(days)
    dt = 1 / 252

    instruments, frames = [], []
    for sector, tickers in SECTORS.items():
        sector_shock = rng.normal(0, 1, n)
        for t in tickers:
            listed = days[0] if rng.random() < 0.85 else days[rng.integers(n // 5, n // 2)]
            instruments.append({"ticker": t, "company_name": f"{t.title()} Ltd", "sector": sector,
                                "exchange": "NSE" if rng.random() < 0.8 else "BSE",
                                "listing_date": listed.date().isoformat()})
            mu, sigma = SECTOR_DRIFT[sector] + rng.normal(0, 0.03), SECTOR_VOL[sector] * rng.uniform(0.8, 1.3)
            shocks = 0.6 * sector_shock + 0.8 * rng.normal(0, 1, n)
            log_ret = (mu - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * shocks
            close = rng.uniform(80, 3_000) * np.exp(np.cumsum(log_ret))
            open_ = close * np.exp(rng.normal(0, 0.006, n))
            high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.008, n)))
            low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.008, n)))
            volume = (rng.lognormal(13, 0.5, n) * (1 + 5 * np.abs(log_ret))).astype(np.int64)
            f = pd.DataFrame({"ticker": t, "trade_date": days.strftime("%Y-%m-%d"),
                              "open": open_.round(2), "high": high.round(2), "low": low.round(2),
                              "close": close.round(2), "volume": volume})
            frames.append(f[pd.to_datetime(f["trade_date"]) >= listed])

    prices = pd.concat(frames, ignore_index=True).astype({"open": object, "high": object, "low": object,
                                                          "close": object, "volume": object})
    prices, defects = _inject_defects(prices, defect_rate, rng)

    pd.DataFrame(instruments).to_csv(out / "instruments.csv", index=False)
    files = []
    years = prices["trade_date"].str[:4]
    for year in sorted(y for y in years.unique() if y.isdigit()):
        part = prices[years == year].sample(frac=1, random_state=int(year))   # feeds are rarely sorted
        path = out / f"prices_{year}.csv"
        part.to_csv(path, index=False)
        files.append(path.name)
    bad_year = prices[~years.str.isdigit()]
    if len(bad_year):   # malformed dates can't be bucketed by year; ship them in the latest file
        with open(out / files[-1], "a") as fh:
            bad_year.to_csv(fh, index=False, header=False)
    return {"rows": len(prices), "files": files, "instruments": len(instruments), "defects": defects}


def _inject_defects(prices: pd.DataFrame, rate: float, rng: np.random.Generator):
    n = len(prices)
    k = max(1, int(n * rate))
    defects = {}

    def pick(m):
        return rng.choice(n, size=m, replace=False)

    idx = pick(k)
    prices.loc[idx, "close"] = None
    defects["null_close"] = k
    idx = pick(k // 2)
    prices.loc[idx, "close"] = -pd.to_numeric(prices.loc[idx, "close"]).abs().fillna(1)
    defects["negative_price"] = len(idx)
    idx = pick(k // 2)
    prices.loc[idx, ["high", "low"]] = prices.loc[idx, ["low", "high"]].to_numpy()
    defects["high_below_low"] = len(idx)
    idx = pick(k // 3)
    prices.loc[idx, "volume"] = -1
    defects["negative_volume"] = len(idx)
    idx = pick(k // 4)
    prices.loc[idx, "ticker"] = "ZZZZ"
    defects["unknown_ticker"] = len(idx)
    idx = pick(k // 4)
    prices.loc[idx, "trade_date"] = "31/13/2021"
    defects["bad_date"] = len(idx)

    exact = prices.iloc[pick(k)].copy()
    conflicting = prices.iloc[pick(k // 2)].copy()
    for c in ("open", "high", "low", "close"):     # internally consistent, just different
        conflicting[c] = (pd.to_numeric(conflicting[c], errors="coerce") * 1.05).round(2)
    defects["exact_duplicates"] = len(exact)
    defects["conflicting_duplicates"] = len(conflicting)
    return pd.concat([prices, exact, conflicting], ignore_index=True), defects


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic equity source files")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default="2024-12-31")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    info = generate(a.out, a.start, a.end, seed=a.seed)
    print(f"Wrote {info['rows']:,} rows for {info['instruments']} instruments to {a.out}: {', '.join(info['files'])}")
    print("Injected defects:", info["defects"])


if __name__ == "__main__":
    main()
