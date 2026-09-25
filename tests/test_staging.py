from pathlib import Path

from finpulse.staging import read_instruments, read_raw, validate

HEADER = "ticker,trade_date,open,high,low,close,volume\n"


def _write(tmp_path: Path, rows: str) -> Path:
    (tmp_path / "instruments.csv").write_text(
        "ticker,company_name,sector,exchange,listing_date\nAAA,Aaa Ltd,Energy,NSE,2020-01-01\n"
        "BBB,Bbb Ltd,Financials,NSE,2020-01-01\n")
    (tmp_path / "prices_2024.csv").write_text(HEADER + rows)
    return tmp_path


def test_each_rule_rejects_and_good_rows_pass(spark, tmp_path):
    d = _write(tmp_path, "\n".join([
        "AAA,2024-01-01,10,11,9,10.5,100",      # good
        "AAA,2024-01-02,10,11,9,,100",          # missing close
        "AAA,2024-01-03,10,11,9,-3,100",        # non-positive price
        "AAA,2024-01-04,10,9,11,10,100",        # high < low
        "AAA,2024-01-05,10,11,9,10,-1",         # negative volume
        "ZZZ,2024-01-06,10,11,9,10,100",        # unknown instrument
        "AAA,31/13/2024,10,11,9,10,100",        # bad date
        "AAA,2024-01-08,abc,11,9,10,100",       # non-numeric -> missing
        "bbb ,2024-01-09,5,6,4,5.5,10",         # ticker normalised: good
    ]) + "\n")
    res = validate(read_raw(spark, d), read_instruments(spark, d))
    assert res.rows_read == 9
    assert res.rows_clean == 2
    assert res.rows_clean + res.rows_rejected == res.rows_read
    for rule in ["missing_value", "non_positive_price", "ohlc_inconsistent", "negative_volume",
                 "unknown_instrument", "unparseable_date"]:
        assert res.reject_counts.get(rule, 0) >= 1, rule
    assert {r.ticker for r in res.clean.collect()} == {"AAA", "BBB"}
    assert all(r.source_file == "prices_2024.csv" for r in res.rejects.collect())


def test_row_failing_several_rules_lists_all_reasons(spark, tmp_path):
    d = _write(tmp_path, "ZZZ,2024-01-01,10,9,11,-1,-5\n")
    res = validate(read_raw(spark, d), read_instruments(spark, d))
    reasons = set(res.rejects.collect()[0].reject_reason.split(";"))
    assert {"unknown_instrument", "non_positive_price", "negative_volume", "ohlc_inconsistent"} <= reasons


def test_exact_duplicates_kept_once_conflicts_rejected_entirely(spark, tmp_path):
    d = _write(tmp_path, "\n".join([
        "AAA,2024-01-01,10,11,9,10.5,100",
        "AAA,2024-01-01,10,11,9,10.5,100",      # exact duplicate -> one kept
        "AAA,2024-01-02,10,11,9,10.5,100",
        "AAA,2024-01-02,10,11,9,10.6,100",      # conflicting -> both rejected
    ]) + "\n")
    res = validate(read_raw(spark, d), read_instruments(spark, d))
    clean = [(r.ticker, str(r.trade_date)) for r in res.clean.collect()]
    assert clean == [("AAA", "2024-01-01")]
    assert res.reject_counts == {"exact_duplicate": 1, "conflicting_duplicate": 2}
