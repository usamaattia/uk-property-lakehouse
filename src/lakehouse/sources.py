"""Where HM Land Registry publishes Price Paid Data (PPD).

Docs: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

* Yearly files (``pp-2025.csv``) hold every standard record for that year. Use them to
  backfill history.
* The monthly update file holds the *changes* since last month: new records (A), changed
  records (C) and deleted records (D). It is published around the 20th working day of each
  month. Applying it correctly is a change-data-capture problem; see
  ``transform/models/silver/silver_transactions.sql``.

The CSV files have no header row; the fixed column order is ``PPD_COLUMNS`` below.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

BASE_URL = os.environ.get(
    "PPD_BASE_URL",
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com",
)

PPD_COLUMNS = [
    "transaction_id",  # {GUID}
    "price",
    "date_of_transfer",  # "2026-06-01 00:00"
    "postcode",
    "property_type",  # D S T F O
    "old_new",  # Y = new build
    "duration",  # F freehold, L leasehold, U unknown
    "paon",  # primary addressable object name (house number/name)
    "saon",  # secondary (flat number)
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category",  # A standard, B additional (repossessions, company buyers, ...)
    "record_status",  # A added, C changed, D deleted (monthly file only)
]


@dataclass(frozen=True)
class PpdFile:
    name: str  # e.g. "pp-2025.csv"
    url: str
    kind: str  # "yearly" | "monthly" | "complete"


def yearly(year: int) -> PpdFile:
    name = f"pp-{year}.csv"
    return PpdFile(name, f"{BASE_URL}/{name}", "yearly")


def monthly_update() -> PpdFile:
    name = "pp-monthly-update-new-version.csv"
    return PpdFile(name, f"{BASE_URL}/{name}", "monthly")


def complete() -> PpdFile:
    """Every transaction since 1995 (~5 GB). Only needed for a full historical backfill."""
    name = "pp-complete.csv"
    return PpdFile(name, f"{BASE_URL}/{name}", "complete")
