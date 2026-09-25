import csv
from datetime import date

from lakehouse.sources import PPD_COLUMNS
from lakehouse.synth import generate


def test_synthetic_files_match_the_real_bulk_format(tmp_path, real_sample):
    files = generate(tmp_path, start=date(2025, 1, 1), end=date(2025, 6, 1), scale=0.05, seed=1)
    real = next(csv.reader(real_sample.open()))
    for f in files:
        assert f.name.startswith("synthetic-")
        for row in csv.reader(f.open()):
            assert len(row) == len(PPD_COLUMNS) == len(real)
            assert row[0].startswith("{") and row[0].endswith("}")
            assert row[2].endswith(" 00:00") and len(row[2]) == len(real[2])
            assert row[4] in "DSTFO" and row[14] in "AB" and row[15] in "ACD"


def test_monthly_update_contains_changes_and_deletes(tmp_path):
    files = generate(tmp_path, start=date(2025, 1, 1), end=date(2025, 6, 1), scale=0.05, seed=1)
    update = next(f for f in files if "monthly" in f.name)
    statuses = {row[15] for row in csv.reader(update.open())}
    assert statuses == {"A", "C", "D"}


def test_generation_is_deterministic(tmp_path):
    a = generate(tmp_path / "a", start=date(2025, 1, 1), end=date(2025, 2, 1), scale=0.05, seed=7)
    b = generate(tmp_path / "b", start=date(2025, 1, 1), end=date(2025, 2, 1), scale=0.05, seed=7)
    assert [f.read_bytes() for f in a] == [f.read_bytes() for f in b]
