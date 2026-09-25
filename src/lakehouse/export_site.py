"""Export gold tables to small JSON files for the static dashboard in ``site/``.

The dashboard is plain HTML + JS served by GitHub Pages, so it costs nothing to host and
never touches the warehouse at view time.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb


def _default(o):
    if isinstance(o, date | datetime):
        return o.isoformat()[:10]
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(type(o))


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def export(duckdb_path: Path, out_dir: Path, read_only: bool = True) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path), read_only=read_only)

    stats = _rows(
        con,
        """
        select
            max(month) as latest_sale_month,
            count(*) filter (where not is_deleted) as transactions,
            count(*) filter (where is_deleted) as deleted_records,
            bool_or(is_synthetic) as is_synthetic
        from silver.silver_transactions
        """,
    )[0]
    districts = _rows(con, "select * from gold.gold_district_summary order by district")
    as_of = districts[0]["as_of_month"] if districts else None

    national = _rows(
        con,
        """
        select month, property_type_code, sales, median_price
        from gold.gold_national_monthly order by month, property_type_code
        """,
    )
    premium = _rows(
        con, "select * from gold.gold_new_build_premium order by new_build_premium desc"
    )
    district_monthly = _rows(
        con,
        """
        select district, month, median_price, sales
        from gold.gold_monthly_prices
        where property_type_code = 'ALL'
        order by district, month
        """,
    )

    meta = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "as_of_month": as_of,
        **stats,
    }
    files = {
        "meta.json": meta,
        "national_monthly.json": national,
        "district_summary.json": districts,
        "district_monthly.json": district_monthly,
        "new_build_premium.json": premium,
    }
    for name, payload in files.items():
        (out_dir / name).write_text(json.dumps(payload, default=_default, separators=(",", ":")))
    return json.loads(json.dumps(meta, default=_default))
