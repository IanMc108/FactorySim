import pytest

from factorysim.config import SimConfig
from factorysim.db import create_database
from factorysim.simulation import run_simulation


@pytest.fixture(scope="session")
def baseline():
    """The default 365-day run, shared read-only across tests."""
    config = SimConfig()
    conn = create_database()
    summary = run_simulation(conn, config)
    yield conn, config, summary
    conn.close()
