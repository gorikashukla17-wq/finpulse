-- FinPulse star schema for MySQL 8 (generated from finpulse/warehouse.py)
-- The pipeline creates this automatically; kept here for review and DBA use.

CREATE TABLE dim_date (
	date_key INTEGER NOT NULL, 
	full_date DATE NOT NULL, 
	year INTEGER NOT NULL, 
	quarter INTEGER NOT NULL, 
	month INTEGER NOT NULL, 
	month_name VARCHAR(9) NOT NULL, 
	week_of_year INTEGER NOT NULL, 
	day_of_week INTEGER NOT NULL, 
	day_name VARCHAR(9) NOT NULL, 
	is_month_end INTEGER NOT NULL, 
	PRIMARY KEY (date_key), 
	UNIQUE (full_date)
);

CREATE TABLE dim_sector (
	sector_key INTEGER NOT NULL AUTO_INCREMENT, 
	sector_name VARCHAR(60) NOT NULL, 
	PRIMARY KEY (sector_key), 
	UNIQUE (sector_name)
);

CREATE TABLE etl_load_run (
	load_run_id INTEGER NOT NULL AUTO_INCREMENT, 
	started_at DATETIME NOT NULL, 
	finished_at DATETIME, 
	status VARCHAR(12) NOT NULL, 
	source_files TEXT, 
	rows_read INTEGER, 
	rows_loaded INTEGER, 
	rows_rejected INTEGER, 
	error_message TEXT, 
	PRIMARY KEY (load_run_id)
);

CREATE TABLE dim_instrument (
	instrument_key INTEGER NOT NULL AUTO_INCREMENT, 
	ticker VARCHAR(12) NOT NULL, 
	company_name VARCHAR(120) NOT NULL, 
	exchange VARCHAR(10) NOT NULL, 
	sector_key INTEGER NOT NULL, 
	listing_date DATE, 
	PRIMARY KEY (instrument_key), 
	UNIQUE (ticker), 
	FOREIGN KEY(sector_key) REFERENCES dim_sector (sector_key)
);

CREATE TABLE etl_rejected_row (
	reject_id INTEGER NOT NULL AUTO_INCREMENT, 
	load_run_id INTEGER NOT NULL, 
	source_file VARCHAR(255) NOT NULL, 
	ticker VARCHAR(12), 
	trade_date_raw VARCHAR(20), 
	reject_reason VARCHAR(200) NOT NULL, 
	raw_record TEXT, 
	PRIMARY KEY (reject_id), 
	FOREIGN KEY(load_run_id) REFERENCES etl_load_run (load_run_id)
);

CREATE INDEX ix_reject_run ON etl_rejected_row (load_run_id);

CREATE TABLE fact_daily_price (
	instrument_key INTEGER NOT NULL, 
	date_key INTEGER NOT NULL, 
	sector_key INTEGER NOT NULL, 
	open_price FLOAT NOT NULL, 
	high_price FLOAT NOT NULL, 
	low_price FLOAT NOT NULL, 
	close_price FLOAT NOT NULL, 
	volume BIGINT NOT NULL, 
	daily_return FLOAT, 
	log_return FLOAT, 
	ma_20 FLOAT, 
	ma_50 FLOAT, 
	volatility_20d FLOAT, 
	load_run_id INTEGER NOT NULL, 
	PRIMARY KEY (instrument_key, date_key), 
	FOREIGN KEY(instrument_key) REFERENCES dim_instrument (instrument_key), 
	FOREIGN KEY(date_key) REFERENCES dim_date (date_key), 
	FOREIGN KEY(sector_key) REFERENCES dim_sector (sector_key), 
	FOREIGN KEY(load_run_id) REFERENCES etl_load_run (load_run_id)
);

CREATE INDEX ix_fact_date ON fact_daily_price (date_key);

CREATE INDEX ix_fact_sector_date ON fact_daily_price (sector_key, date_key);

CREATE OR REPLACE VIEW vw_instrument_latest AS
        SELECT i.ticker, i.company_name, s.sector_name, d.full_date, f.close_price, f.daily_return,
               f.ma_20, f.ma_50, f.volatility_20d
        FROM fact_daily_price f
        JOIN dim_instrument i ON i.instrument_key = f.instrument_key
        JOIN dim_sector s ON s.sector_key = f.sector_key
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.date_key = (SELECT MAX(f2.date_key) FROM fact_daily_price f2
                            WHERE f2.instrument_key = f.instrument_key);

CREATE OR REPLACE VIEW vw_sector_monthly AS
        SELECT s.sector_name, d.year, d.quarter, d.month,
               AVG(f.daily_return) AS avg_daily_return,
               AVG(f.volatility_20d) AS avg_volatility,
               SUM(f.close_price * f.volume) AS traded_value,
               COUNT(*) AS price_rows
        FROM fact_daily_price f
        JOIN dim_sector s ON s.sector_key = f.sector_key
        JOIN dim_date d ON d.date_key = f.date_key
        GROUP BY s.sector_name, d.year, d.quarter, d.month;

CREATE OR REPLACE VIEW vw_data_quality AS
        SELECT r.load_run_id, r.started_at, r.finished_at, r.status, r.rows_read, r.rows_loaded,
               r.rows_rejected,
               CASE WHEN r.rows_read > 0 THEN 1.0 * r.rows_rejected / r.rows_read END AS reject_rate
        FROM etl_load_run r;
