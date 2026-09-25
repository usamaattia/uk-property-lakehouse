"""End-to-end on DuckDB: landing zone -> dbt (bronze/silver/gold) -> dashboard export.

Uses 40 real Price Paid rows, then lands a monthly change file with a correction, a
deletion and a new sale, and checks silver and gold react correctly.
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path

import duckdb
import pytest
from conftest import ROOT

from lakehouse.export_site import export
from lakehouse.ingest import land_file
from lakehouse.landing import LocalLanding

dbt_main = pytest.importorskip("dbt.cli.main")


class Warehouse:
    def __init__(self, tmp: Path, monkeypatch):
        self.tmp = tmp
        self.db = tmp / "wh.duckdb"
        self.landing = LocalLanding(tmp / "landing")
        monkeypatch.setenv("DUCKDB_PATH", str(self.db))
        monkeypatch.setenv("LANDING_URI", str(tmp / "landing"))
        monkeypatch.setenv("DBT_TARGET", "dev")

    def dbt(self, *args: str) -> None:
        res = dbt_main.dbtRunner().invoke(
            [
                *args,
                "--project-dir", str(ROOT / "transform"),
                "--profiles-dir", str(ROOT / "transform"),
                "--target-path", str(self.tmp / "target"),
                "--log-path", str(self.tmp / "logs"),
                "--quiet",
            ]
        )  # fmt: skip
        assert res.success, res.exception or [str(r.message) for r in (res.result or [])]

    def sql(self, q: str):
        con = duckdb.connect(str(self.db))  # same config as the in-process dbt connection
        try:
            return con.sql(q).fetchall()
        finally:
            con.close()


def _write(path: Path, rows: list[list[str]]) -> Path:
    with path.open("w", newline="") as f:
        csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(rows)
    return path


@pytest.fixture
def wh(tmp_path, monkeypatch):
    return Warehouse(tmp_path, monkeypatch)


def test_real_sample_end_to_end_with_cdc(wh, real_sample, tmp_path):
    rows = list(csv.reader(real_sample.open()))
    land_file(wh.landing, real_sample, "pp-2026.csv", "yearly", "fixture", "20260801T000000Z")
    lag0 = '{"registration_lag_months": 0}'
    wh.dbt("build", "--vars", lag0)

    # Silver: one row per transaction, braces stripped, typed
    assert wh.sql(
        "select count(*), count(distinct transaction_id) from silver.silver_transactions"
    ) == [(40, 40)]
    tx, price, d, pc, ptype = wh.sql(
        "select transaction_id, price, date_of_transfer, postcode_district, property_type "
        "from silver.silver_transactions "
        "where transaction_id = '5834E4E9-48A3-29C7-E063-4804A8C015BC'"
    )[0]
    assert (price, str(d), pc, ptype) == (1450000, "2026-07-17", "GU15", "Detached")

    # Gold: median over category-A sales equals an independent Python calculation
    standard = [int(r[1]) for r in rows if r[14] == "A"]
    (sales, median) = wh.sql(
        "select sales_12m, median_price_12m from gold.gold_district_summary "
        "where district = 'SURREY HEATH'"
    )[0]
    assert sales == len(standard) == 36
    assert median == round(statistics.median(standard))

    # --- Next month: the change file corrects one sale, deletes one and adds one --------
    corrected = list(rows[0])
    corrected[1] = "1400000"
    corrected[15] = "C"
    deleted = list(rows[1])
    deleted[15] = "D"
    added = list(rows[2])
    added[0] = "{00000000-0000-0000-0000-000000000001}"
    added[1] = "500000"
    update = _write(tmp_path / "update.csv", [corrected, deleted, added])
    land_file(
        wh.landing,
        update,
        "pp-monthly-update-new-version.csv",
        "monthly",
        "fixture",
        "20260901T000000Z",
    )
    wh.dbt("build", "--vars", lag0)  # incremental: only the new load is processed

    assert wh.sql("select count(*), sum(is_deleted::int) from silver.silver_transactions") == [
        (41, 1)
    ]
    assert wh.sql(
        "select price, record_status from silver.silver_transactions "
        "where transaction_id = '5834E4E9-48A3-29C7-E063-4804A8C015BC'"
    ) == [(1400000, "C")]
    expected = [int(r[1]) for r in (corrected, *rows[2:], added) if r[14] == "A"]
    assert wh.sql("select sales_12m, median_price_12m from gold.gold_district_summary") == [
        (len(expected), round(statistics.median(expected)))
    ]

    # Re-running with nothing new changes nothing (idempotent)
    before = wh.sql("select * from silver.silver_transactions order by transaction_id")
    wh.dbt("build", "--vars", lag0)
    assert wh.sql("select * from silver.silver_transactions order by transaction_id") == before

    # Dashboard export
    meta = export(wh.db, tmp_path / "site", read_only=False)
    assert meta["transactions"] == 40 and meta["deleted_records"] == 1
    assert meta["is_synthetic"] is False
    summary = json.loads((tmp_path / "site" / "district_summary.json").read_text())
    assert summary[0]["district"] == "SURREY HEATH"


def test_synthetic_demo_builds_and_is_flagged(wh, tmp_path):
    from lakehouse.synth import generate

    files = generate(tmp_path / "gen", scale=0.05, seed=3)
    for f in sorted(files, key=lambda p: "monthly" in p.name):
        land_file(
            wh.landing,
            f,
            f.name,
            "synthetic",
            "test",
            f"2026080{1 if 'monthly' not in f.name else 2}T000000Z",
        )
    wh.dbt("build")
    assert wh.sql("select bool_and(is_synthetic) from silver.silver_transactions") == [(True,)]
    assert wh.sql("select count(*) from gold.gold_district_summary")[0][0] == 24
    meta = export(wh.db, tmp_path / "site", read_only=False)
    assert meta["is_synthetic"] is True
