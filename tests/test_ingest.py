from datetime import UTC, datetime

import httpx

from lakehouse import landing as landing_mod
from lakehouse.ingest import DATASET, ingest, land_file
from lakehouse.landing import LocalLanding, VolumeLanding, open_landing
from lakehouse.pipeline import anything_new, run_monthly
from lakehouse.sources import monthly_update, yearly


def test_land_file_layout_and_log(tmp_path, real_sample):
    lz = LocalLanding(tmp_path)
    r = land_file(lz, real_sample, "pp-2026.csv", "yearly", "test", "20260801T000000Z")
    assert r.status == "landed" and r.rows == 40
    assert (tmp_path / "ppd" / "load_id=20260801T000000Z" / "pp-2026.csv").read_bytes() == (
        real_sample.read_bytes()
    )
    (entry,) = lz.read_log(DATASET)
    assert entry["sha256"] == r.sha256 and entry["load_id"] == "20260801T000000Z"


def test_identical_file_is_not_landed_twice(tmp_path, real_sample):
    lz = LocalLanding(tmp_path)
    land_file(lz, real_sample, "pp-2026.csv", "yearly", "test", "20260801T000000Z")
    again = land_file(lz, real_sample, "pp-2026.csv", "yearly", "test", "20260901T000000Z")
    assert again.status == "skipped"
    assert not (tmp_path / "ppd" / "load_id=20260901T000000Z").exists()
    assert len(lz.read_log(DATASET)) == 1


def _mock_client(body: bytes, calls: list):
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, content=body)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_ingest_downloads_and_lands(tmp_path, real_sample):
    calls = []
    client = _mock_client(real_sample.read_bytes(), calls)
    res = ingest(LocalLanding(tmp_path), [yearly(2026)], load_id="20260801T000000Z", client=client)
    assert calls == ["/pp-2026.csv"]
    assert [r.status for r in res] == ["landed"]


def test_ingest_retries_server_errors(tmp_path, real_sample, monkeypatch):
    monkeypatch.setattr("lakehouse.ingest.time.sleep", lambda s: None)
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, content=real_sample.read_bytes())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    res = ingest(LocalLanding(tmp_path), [monthly_update()], client=client)
    assert attempts["n"] == 3 and res[0].status == "landed"


def test_run_monthly_backfills_only_when_empty(tmp_path, real_sample):
    calls = []
    lz = LocalLanding(tmp_path)
    now = datetime(2026, 9, 25, tzinfo=UTC)

    first = run_monthly(lz, backfill_years=2, now=now, client=_mock_client(b"x\n", calls))
    assert calls == ["/pp-2025.csv", "/pp-2026.csv", "/pp-monthly-update-new-version.csv"]
    # All three bodies are identical here, so only the first is new.
    assert [r.status for r in first] == ["landed", "skipped", "skipped"]
    loads = sorted(e["load_id"] for e in lz.read_log(DATASET))
    assert loads == ["20260925T000000Z"]

    calls.clear()
    second = run_monthly(lz, backfill_years=2, now=now, client=_mock_client(b"y\n", calls))
    assert calls == ["/pp-monthly-update-new-version.csv"]  # no second backfill
    assert anything_new(second)
    # the monthly update is always a later load than the backfill
    assert lz.read_log(DATASET)[-1]["load_id"] == "20260925T000001Z"


def test_open_landing_picks_implementation(tmp_path, monkeypatch):
    assert isinstance(open_landing(str(tmp_path)), LocalLanding)
    monkeypatch.setattr(landing_mod.os.path, "isdir", lambda p: False)
    monkeypatch.setattr(landing_mod, "VolumeLanding", lambda uri: ("volume", uri))
    assert open_landing("/Volumes/workspace/uk_property/landing") == (
        "volume",
        "/Volumes/workspace/uk_property/landing",
    )


def test_volume_landing_uses_files_api(real_sample):
    class FakeFiles:
        def __init__(self):
            self.store = {}

        def upload(self, path, f, overwrite=False):
            self.store[path] = f.read()

        def download(self, path):
            if path not in self.store:
                raise RuntimeError("404 Not Found")
            import io

            return type("R", (), {"contents": io.BytesIO(self.store[path])})()

    client = type("W", (), {"files": FakeFiles()})()
    lz = VolumeLanding("/Volumes/c/s/landing", client=client)
    r = land_file(lz, real_sample, "pp-2026.csv", "yearly", "test", "20260801T000000Z")
    assert r.path == "/Volumes/c/s/landing/ppd/load_id=20260801T000000Z/pp-2026.csv"
    assert len(lz.read_log(DATASET)) == 1
    assert (
        land_file(lz, real_sample, "x.csv", "yearly", "t", "20260901T000000Z").status == "skipped"
    )
