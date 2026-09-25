"""Command line entry point.

lakehouse monthly                                 # scheduled run (backfill if empty + update)
lakehouse ingest --years 2025 2026 --monthly     # real data from HM Land Registry
lakehouse demo                                    # synthetic data, no network needed
lakehouse export-site                             # gold tables -> site/data/*.json
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import sources
from .ingest import DATASET, ingest, land_file, new_load_id
from .landing import open_landing

DEFAULT_LANDING = os.environ.get("LANDING_URI", "data/landing")
DEFAULT_DUCKDB = os.environ.get("DUCKDB_PATH", "warehouse/lakehouse.duckdb")


def cmd_ingest(args) -> None:
    landing = open_landing(args.landing)
    this_year = datetime.now(UTC).year
    years = args.years or [this_year - 1, this_year]
    files = [sources.yearly(y) for y in years]
    if args.monthly:
        files.append(sources.monthly_update())
    for r in ingest(landing, files):
        print(f"{r.status:8} {r.name:40} {r.rows:>10,} rows")


def cmd_monthly(args) -> None:
    from .pipeline import run_monthly

    for r in run_monthly(open_landing(args.landing), backfill_years=args.backfill_years):
        print(f"{r.status:8} {r.name:40} {r.rows:>10,} rows")


def cmd_demo(args) -> None:
    """Land synthetic yearly files (load 1) then the monthly update (load 2)."""
    from .synth import generate

    landing = open_landing(args.landing)
    t0 = datetime.now(UTC)
    with tempfile.TemporaryDirectory() as tmp:
        files = generate(tmp, scale=args.scale, seed=args.seed)
        yearly = [f for f in files if "monthly" not in f.name]
        monthly = [f for f in files if "monthly" in f.name]
        for batch, load_id in (
            (yearly, new_load_id(t0)),
            (monthly, new_load_id(t0 + timedelta(seconds=1))),
        ):
            for f in batch:
                r = land_file(landing, f, f.name, "synthetic", "lakehouse.synth", load_id)
                print(f"{r.status:8} {r.name:40} {r.rows:>10,} rows")


def cmd_export_site(args) -> None:
    from .export_site import export

    meta = export(Path(args.duckdb), Path(args.out))
    print(f"exported site data as of {meta['as_of_month']} to {args.out}")


def cmd_log(args) -> None:
    for e in open_landing(args.landing).read_log(DATASET):
        print(f"{e['load_id']}  {e['file']:40} {e['rows']:>10,} rows  {e['kind']}")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(
        prog="lakehouse", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--landing",
        default=DEFAULT_LANDING,
        help="landing zone: a local folder or /Volumes/<catalog>/<schema>/<volume>",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("ingest", help="download real Price Paid Data")
    p.add_argument("--years", type=int, nargs="*", help="yearly files (default: last + this)")
    p.add_argument("--monthly", action="store_true", help="also land the monthly A/C/D update")
    p.set_defaults(fn=cmd_ingest)

    p = sub.add_parser("monthly", help="scheduled run: backfill if empty, then monthly update")
    p.add_argument("--backfill-years", type=int, default=3)
    p.set_defaults(fn=cmd_monthly)

    p = sub.add_parser("demo", help="land SYNTHETIC data (no network)")
    p.add_argument("--scale", type=float, default=0.25)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(fn=cmd_demo)

    p = sub.add_parser("export-site", help="write dashboard JSON from gold tables")
    p.add_argument("--duckdb", default=DEFAULT_DUCKDB)
    p.add_argument("--out", default="site/data")
    p.set_defaults(fn=cmd_export_site)

    p = sub.add_parser("log", help="show what has been landed")
    p.set_defaults(fn=cmd_log)

    args = ap.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
