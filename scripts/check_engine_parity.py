"""Prove the gold layer is engine-independent: DuckDB and Apache Spark must agree.

Build the same landing data with both targets first:

    dbt build --project-dir transform --profiles-dir transform                       # DuckDB
    DBT_TARGET=spark dbt build --project-dir transform --profiles-dir transform --full-refresh
    python scripts/check_engine_parity.py

Every gold table is read from both engines, sorted by its key, and compared value by value
(numbers within a relative tolerance of 1e-9). Exits non-zero on any difference.
"""

from __future__ import annotations

import math
import os
import sys
from datetime import date, datetime
from decimal import Decimal

import duckdb
from pyspark.sql import SparkSession

TABLES = {
    "gold_monthly_prices": "month, district, property_type_code",
    "gold_national_monthly": "month, property_type_code",
    "gold_district_summary": "district",
    "gold_new_build_premium": "county",
}


def norm(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float | Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()[:10]
    return str(value)


def same(a: list[tuple], b: list[tuple]) -> bool:
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b, strict=True):
        for x, y in zip(ra, rb, strict=True):
            if isinstance(x, float) and isinstance(y, float):
                if not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9):
                    return False
            elif x != y:
                return False
    return True


def main() -> int:
    duck = duckdb.connect(
        os.environ.get("DUCKDB_PATH", "warehouse/lakehouse.duckdb"), read_only=True
    )
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    failed = False
    for table, key in TABLES.items():
        q = f"select * from gold.{table} order by {key}"
        a = [tuple(norm(v) for v in row) for row in duck.sql(q).fetchall()]
        b = [tuple(norm(v) for v in row) for row in spark.sql(q).collect()]
        if same(a, b):
            print(f"OK    {table:28} {len(a):>7} rows identical")
            continue
        failed = True
        print(f"DIFF  {table:28} duckdb={len(a)} spark={len(b)} rows")
        for ra, rb in zip(a, b, strict=False):
            if not same([ra], [rb]):
                print(f"      duckdb: {ra}\n      spark:  {rb}")
                break
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
