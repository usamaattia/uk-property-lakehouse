"""SYNTHETIC Price Paid Data for demos, CI and offline development.

The real files are hundreds of MB and need network access. This generator writes files in
the exact same format (no header, 16 columns, braced GUIDs, "YYYY-MM-DD 00:00" dates) so
the whole pipeline can run anywhere in seconds.

The numbers are made up. District names are real, so the output looks familiar, but
prices, volumes and trends are random. Every synthetic file name starts with
``synthetic-``; the pipeline carries that through to the dashboard, which shows a banner.
"""

from __future__ import annotations

import csv
import math
import random
import uuid
from datetime import date
from pathlib import Path

# district, county, outward-code prefixes, rough base price (GBP), flat share, sales/month
DISTRICTS = [
    ("SURREY HEATH", "SURREY", ["GU15", "GU16", "GU18", "GU19"], 480_000, 0.20, 70),
    ("GUILDFORD", "SURREY", ["GU1", "GU2", "GU4"], 560_000, 0.25, 110),
    ("WOKING", "SURREY", ["GU21", "GU22"], 470_000, 0.28, 95),
    ("CITY OF WESTMINSTER", "GREATER LONDON", ["W1", "SW1", "W2"], 1_050_000, 0.85, 160),
    ("CAMDEN", "GREATER LONDON", ["NW1", "NW3", "NW5"], 850_000, 0.78, 150),
    ("TOWER HAMLETS", "GREATER LONDON", ["E1", "E3", "E14"], 490_000, 0.80, 190),
    ("CROYDON", "GREATER LONDON", ["CR0", "CR2", "SE25"], 420_000, 0.45, 230),
    ("BRISTOL", "CITY OF BRISTOL", ["BS1", "BS3", "BS6"], 350_000, 0.35, 330),
    ("MANCHESTER", "GREATER MANCHESTER", ["M1", "M14", "M20"], 245_000, 0.40, 380),
    ("LEEDS", "WEST YORKSHIRE", ["LS1", "LS6", "LS17"], 240_000, 0.25, 520),
    ("BIRMINGHAM", "WEST MIDLANDS", ["B1", "B15", "B29"], 230_000, 0.30, 700),
    ("LIVERPOOL", "MERSEYSIDE", ["L1", "L15", "L18"], 185_000, 0.28, 420),
    ("NEWCASTLE UPON TYNE", "TYNE AND WEAR", ["NE1", "NE3", "NE6"], 195_000, 0.25, 260),
    ("SHEFFIELD", "SOUTH YORKSHIRE", ["S1", "S10", "S11"], 215_000, 0.18, 450),
    ("NOTTINGHAM", "CITY OF NOTTINGHAM", ["NG1", "NG5", "NG7"], 195_000, 0.20, 250),
    ("CAMBRIDGE", "CAMBRIDGESHIRE", ["CB1", "CB2", "CB4"], 520_000, 0.30, 110),
    ("OXFORD", "OXFORDSHIRE", ["OX1", "OX2", "OX4"], 510_000, 0.28, 100),
    ("BRIGHTON AND HOVE", "BRIGHTON AND HOVE", ["BN1", "BN2", "BN3"], 440_000, 0.45, 230),
    ("CARDIFF", "CARDIFF", ["CF10", "CF14", "CF24"], 275_000, 0.22, 300),
    ("SWANSEA", "SWANSEA", ["SA1", "SA2", "SA3"], 200_000, 0.12, 190),
    ("CORNWALL", "CORNWALL", ["TR1", "PL25", "TR18"], 310_000, 0.12, 520),
    ("YORK", "YORK", ["YO1", "YO10", "YO24"], 320_000, 0.15, 170),
    ("MILTON KEYNES", "MILTON KEYNES", ["MK1", "MK9", "MK10"], 330_000, 0.18, 230),
    ("HULL", "CITY OF KINGSTON UPON HULL", ["HU1", "HU5", "HU8"], 145_000, 0.08, 240),
]

PROPERTY_MULTIPLIER = {"D": 1.65, "S": 1.05, "T": 0.90, "F": 0.70, "O": 1.10}
STREETS = [
    "HIGH STREET",
    "STATION ROAD",
    "CHURCH LANE",
    "VICTORIA ROAD",
    "PARK AVENUE",
    "MILL LANE",
    "THE GREEN",
    "KINGS ROAD",
    "QUEENS ROAD",
    "LONDON ROAD",
]


def _months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield date(y, m, 1)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _row(rng: random.Random, d: date, district, record_status="A", tx_id=None) -> list[str]:
    name, county, outwards, base, flat_share, _ = district
    rest = 1 - flat_share
    ptype = rng.choices("DSTFO", [rest * 0.30, rest * 0.35, rest * 0.33, flat_share, rest * 0.02])[
        0
    ]
    new_build = rng.random() < 0.09
    category = "B" if rng.random() < 0.08 else "A"
    # Price model: base x type x trend x season x noise (clearly synthetic)
    years = (d.year - 2020) + d.month / 12
    growth = 1 + 0.035 * years + 0.02 * math.sin(years * 1.3 + len(name))
    season = 1 + 0.02 * math.sin((d.month - 3) / 12 * 2 * math.pi)
    price = base * PROPERTY_MULTIPLIER[ptype] * growth * season * rng.lognormvariate(0, 0.28)
    if new_build:
        price *= 1.14
    if category == "B":
        price *= 0.8
    price = int(round(price, -3 if price > 100_000 else -2))
    day = rng.randint(1, 28)
    outward = rng.choice(outwards)
    letters = "ABDEFGHJLNPQRSTUWXYZ"
    postcode = f"{outward} {rng.randint(1, 9)}{rng.choice(letters)}{rng.choice(letters)}"
    flat = ptype == "F"
    return [
        tx_id or "{" + str(uuid.UUID(int=rng.getrandbits(128))).upper() + "}",
        str(price),
        f"{d.year:04d}-{d.month:02d}-{day:02d} 00:00",
        postcode,
        ptype,
        "Y" if new_build else "N",
        "L" if flat else rng.choices("FL", [0.93, 0.07])[0],
        str(rng.randint(1, 180)),
        f"FLAT {rng.randint(1, 40)}" if flat else "",
        rng.choice(STREETS),
        "",
        "LONDON" if county == "GREATER LONDON" else name,
        name,
        county,
        category,
        record_status,
    ]


def generate(
    out_dir: str | Path,
    start: date = date(2021, 1, 1),
    end: date = date(2026, 7, 1),
    scale: float = 0.25,
    seed: int = 42,
) -> list[Path]:
    """Write synthetic yearly files plus one monthly update file (with A/C/D records).

    ``scale`` multiplies sales volumes (1.0 ~ 190k rows over 5.5 years).
    """
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    by_year: dict[int, list[list[str]]] = {}
    months = list(_months(start, end))
    update_months = set(months[-2:])  # recent months arrive in the monthly update
    update_rows: list[list[str]] = []
    for d in months:
        for district in DISTRICTS:
            n = max(1, int(rng.gauss(district[5] * scale, district[5] * scale * 0.15)))
            for _ in range(n):
                row = _row(rng, d, district)
                (update_rows if d in update_months else by_year.setdefault(d.year, [])).append(row)

    # The monthly update also corrects and deletes some records already published.
    history = [r for rows in by_year.values() for r in rows[-2000:]]
    for r in rng.sample(history, min(40, len(history))):  # corrections (C)
        fixed = list(r)
        fixed[1] = str(int(int(r[1]) * rng.choice([0.9, 1.1])))
        fixed[15] = "C"
        update_rows.append(fixed)
    for r in rng.sample(history, min(15, len(history))):  # deletions (D)
        gone = list(r)
        gone[15] = "D"
        update_rows.append(gone)

    written = []
    for year, rows in sorted(by_year.items()):
        written.append(_write(out / f"synthetic-pp-{year}.csv", rows))
    written.append(_write(out / "synthetic-pp-monthly-update.csv", update_rows))
    return written


def _write(path: Path, rows: list[list[str]]) -> Path:
    with path.open("w", newline="") as f:
        csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(rows)
    return path
