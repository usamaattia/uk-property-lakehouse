"""The landing zone: raw files exactly as downloaded, never modified.

Layout (identical locally and on Databricks):

    <root>/ppd/load_id=20260925T101500Z/pp-2026.csv
    <root>/ppd/_ingest_log.jsonl       one JSON line per landed file

``load_id`` is a sortable UTC timestamp. The bronze model records it on every row, and
silver uses it to decide which version of a record is newest.

Two implementations share one interface:
* ``LocalLanding``  - a folder on disk (dev, CI, GitHub Actions).
* ``VolumeLanding`` - a Unity Catalog Volume on Databricks, via the Files API.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Protocol

LOG_NAME = "_ingest_log.jsonl"


class Landing(Protocol):
    uri: str

    def read_log(self, dataset: str) -> list[dict]: ...
    def put(self, dataset: str, load_id: str, name: str, local_file: Path) -> str: ...
    def append_log(self, dataset: str, entry: dict) -> None: ...


class LocalLanding:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.uri = str(self.root)

    def _log(self, dataset: str) -> Path:
        return self.root / dataset / LOG_NAME

    def read_log(self, dataset: str) -> list[dict]:
        path = self._log(dataset)
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def put(self, dataset: str, load_id: str, name: str, local_file: Path) -> str:
        dest = self.root / dataset / f"load_id={load_id}" / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        shutil.copyfile(local_file, tmp)
        tmp.replace(dest)  # atomic: readers never see a half-written file
        return str(dest)

    def append_log(self, dataset: str, entry: dict) -> None:
        path = self._log(dataset)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps(entry) + "\n")


class VolumeLanding:
    """Unity Catalog Volume, e.g. ``/Volumes/workspace/uk_property/landing``.

    Auth comes from the standard Databricks env vars / config profile
    (DATABRICKS_HOST + DATABRICKS_TOKEN, or ``databricks auth login``).
    """

    def __init__(self, volume_path: str, client=None) -> None:
        if client is None:
            from databricks.sdk import WorkspaceClient  # optional dependency

            client = WorkspaceClient()
        self.w = client
        self.root = volume_path.rstrip("/")
        self.uri = self.root

    def read_log(self, dataset: str) -> list[dict]:
        path = f"{self.root}/{dataset}/{LOG_NAME}"
        try:
            data = self.w.files.download(path).contents.read().decode()
        except Exception as exc:  # NotFound
            if "not found" in str(exc).lower() or "404" in str(exc):
                return []
            raise
        return [json.loads(line) for line in data.splitlines() if line.strip()]

    def put(self, dataset: str, load_id: str, name: str, local_file: Path) -> str:
        dest = f"{self.root}/{dataset}/load_id={load_id}/{name}"
        with local_file.open("rb") as f:
            self.w.files.upload(dest, f, overwrite=True)
        return dest

    def append_log(self, dataset: str, entry: dict) -> None:
        import io

        lines = [json.dumps(e) for e in self.read_log(dataset)] + [json.dumps(entry)]
        body = io.BytesIO(("\n".join(lines) + "\n").encode())
        self.w.files.upload(f"{self.root}/{dataset}/{LOG_NAME}", body, overwrite=True)


def open_landing(uri: str) -> Landing:
    """Pick the right implementation for a landing URI.

    Inside Databricks, Volumes are mounted at /Volumes and behave like a local folder.
    Outside Databricks, a /Volumes/... URI is reached through the Files API.
    """
    if uri.startswith("/Volumes/") and not os.path.isdir("/Volumes"):
        return VolumeLanding(uri)
    return LocalLanding(uri)
