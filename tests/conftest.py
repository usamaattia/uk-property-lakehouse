from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def real_sample() -> Path:
    """40 real Price Paid rows (Surrey Heath, Jun-Jul 2026) converted to bulk-file format."""
    return FIXTURES / "pp-sample-surrey-heath-2026.csv"
