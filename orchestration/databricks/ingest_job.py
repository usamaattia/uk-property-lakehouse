"""Databricks job entry point: land this month's Price Paid files in a Unity Catalog Volume.

Runs on serverless compute. Volumes are mounted at /Volumes, so the landing zone is written
like a local folder. The bundle passes the synced repo path so the `lakehouse` package can
be imported without building a wheel.
"""

import argparse
import logging
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--src", required=True, help="path to the bundle's src/ folder")
ap.add_argument("--landing", required=True, help="/Volumes/<catalog>/<schema>/<volume>")
ap.add_argument("--backfill-years", type=int, default=3)
args = ap.parse_args()

sys.path.insert(0, args.src)
logging.basicConfig(level=logging.INFO)

from lakehouse.landing import open_landing  # noqa: E402
from lakehouse.pipeline import run_monthly  # noqa: E402

for r in run_monthly(open_landing(args.landing), backfill_years=args.backfill_years):
    print(f"{r.status:8} {r.name:40} {r.rows:>10,} rows")
