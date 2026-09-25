"""The monthly ingestion routine shared by the CLI, Airflow and the Databricks job.

* First run (empty landing zone): backfill yearly files for the last N years.
* Every run: land the monthly change file (A/C/D records) as a *separate, later* load, so
  silver always applies it after the yearly data it corrects.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from . import sources
from .ingest import DATASET, IngestResult, ingest, new_load_id
from .landing import Landing

log = logging.getLogger(__name__)


def run_monthly(
    landing: Landing, backfill_years: int = 3, now: datetime | None = None, client=None
) -> list[IngestResult]:
    now = now or datetime.now(UTC)
    results: list[IngestResult] = []
    if not landing.read_log(DATASET):
        years = list(range(now.year - backfill_years + 1, now.year + 1))
        log.info("empty landing zone: backfilling %s", years)
        results += ingest(
            landing, [sources.yearly(y) for y in years], load_id=new_load_id(now), client=client
        )
    results += ingest(
        landing,
        [sources.monthly_update()],
        load_id=new_load_id(now + timedelta(seconds=1)),
        client=client,
    )
    return results


def anything_new(results: list[IngestResult]) -> bool:
    return any(r.status == "landed" for r in results)
