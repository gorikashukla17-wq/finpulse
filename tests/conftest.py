import pytest

from finpulse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("finpulse-tests", "local[2]")
    yield s
    s.stop()
