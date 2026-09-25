-- Example analytical queries against the FinPulse star schema.
-- Each one joins the narrow fact table to small dimensions on indexed integer keys.

-- 1. Annualised return and average volatility by sector and year
SELECT s.sector_name, d.year,
       AVG(f.daily_return) * 252      AS annualised_return,
       AVG(f.volatility_20d)          AS avg_volatility
FROM fact_daily_price f
JOIN dim_sector s ON s.sector_key = f.sector_key
JOIN dim_date   d ON d.date_key   = f.date_key
GROUP BY s.sector_name, d.year
ORDER BY d.year, annualised_return DESC;

-- 2. Golden crosses: days the 20-day MA closed above the 50-day MA for the first time
SELECT i.ticker, d.full_date, f.close_price, f.ma_20, f.ma_50
FROM fact_daily_price f
JOIN dim_instrument i ON i.instrument_key = f.instrument_key
JOIN dim_date d       ON d.date_key = f.date_key
JOIN fact_daily_price p ON p.instrument_key = f.instrument_key
                       AND p.date_key = (SELECT MAX(x.date_key) FROM fact_daily_price x
                                         WHERE x.instrument_key = f.instrument_key AND x.date_key < f.date_key)
WHERE f.ma_20 > f.ma_50 AND p.ma_20 <= p.ma_50
ORDER BY d.full_date DESC
LIMIT 20;

-- 3. Most volatile instruments last quarter in the data
SELECT i.ticker, s.sector_name, AVG(f.volatility_20d) AS avg_vol
FROM fact_daily_price f
JOIN dim_instrument i ON i.instrument_key = f.instrument_key
JOIN dim_sector s     ON s.sector_key = f.sector_key
JOIN dim_date d       ON d.date_key = f.date_key
WHERE d.year = 2024 AND d.quarter = 4
GROUP BY i.ticker, s.sector_name
ORDER BY avg_vol DESC
LIMIT 10;

-- 4. Traceability: which load run and source file does a number come from?
SELECT f.load_run_id, r.finished_at, r.source_files
FROM fact_daily_price f
JOIN etl_load_run r ON r.load_run_id = f.load_run_id
JOIN dim_instrument i ON i.instrument_key = f.instrument_key
WHERE i.ticker = 'INFX' AND f.date_key = 20241231;

-- 5. Data-quality drill-down: rejected rows for the latest run, by rule and file
SELECT source_file, reject_reason, COUNT(*) AS n
FROM etl_rejected_row
WHERE load_run_id = (SELECT MAX(load_run_id) FROM etl_load_run WHERE status = 'SUCCEEDED')
GROUP BY source_file, reject_reason
ORDER BY n DESC;
