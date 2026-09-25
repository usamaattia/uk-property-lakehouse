"""Download Price Paid Data files into the landing zone.

Idempotent: every file is hashed, and a file whose SHA-256 has already been landed is
skipped. Re-running a failed or duplicate schedule is harmless, and bronze never gets the
same file twice.
"""

from __future__ import annotations

import hashlib
import logging
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from .landing import Landing
from .sources import PpdFile

log = logging.getLogger(__name__)
DATASET = "ppd"


@dataclass
class IngestResult:
    name: str
    status: str  # "landed" | "skipped"
    sha256: str
    rows: int
    bytes: int
    path: str | None = None


def new_load_id(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")


def _download(url: str, dest: Path, client: httpx.Client, retries: int = 3) -> None:
    """Stream ``url`` to ``dest`` (files can be hundreds of MB, so never hold in memory)."""
    for attempt in range(retries + 1):
        try:
            with client.stream("GET", url) as resp, dest.open("wb") as f:
                resp.raise_for_status()
                for chunk in resp.iter_bytes(1 << 20):
                    f.write(chunk)
            return
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            if attempt == retries or (
                isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500
            ):
                raise
            wait = 5 * 2**attempt
            log.warning("download of %s failed (%s); retrying in %ss", url, exc, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def land_file(
    landing: Landing, local_file: Path, name: str, kind: str, source: str, load_id: str
) -> IngestResult:
    """Hash a local file and put it in the landing zone unless already present."""
    sha, rows, size = hashlib.sha256(), 0, 0
    with local_file.open("rb") as f:
        while chunk := f.read(1 << 20):
            sha.update(chunk)
            rows += chunk.count(b"\n")
            size += len(chunk)
    digest = sha.hexdigest()

    if any(e["sha256"] == digest for e in landing.read_log(DATASET)):
        log.info("skip %s: identical file already landed", name)
        return IngestResult(name, "skipped", digest, rows, size)

    path = landing.put(DATASET, load_id, name, local_file)
    landing.append_log(
        DATASET,
        {
            "load_id": load_id,
            "file": name,
            "kind": kind,
            "source": source,
            "sha256": digest,
            "rows": rows,
            "bytes": size,
            "landed_at": datetime.now(UTC).isoformat(),
        },
    )
    log.info("landed %s (%s rows) -> %s", name, f"{rows:,}", path)
    return IngestResult(name, "landed", digest, rows, size, path)


def ingest(
    landing: Landing,
    files: Iterable[PpdFile],
    load_id: str | None = None,
    client: httpx.Client | None = None,
) -> list[IngestResult]:
    load_id = load_id or new_load_id()
    own_client = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(60, read=300), follow_redirects=True)
    results = []
    try:
        with tempfile.TemporaryDirectory() as tmp:
            for f in files:
                local = Path(tmp) / f.name
                log.info("downloading %s", f.url)
                _download(f.url, local, client)
                results.append(land_file(landing, local, f.name, f.kind, f.url, load_id))
    finally:
        if own_client:
            client.close()
    return results
