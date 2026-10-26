import pytest

from factorysim.config import SimConfig
from factorysim.db import create_database
from factorysim.report import build_etl_results
from factorysim.simulation import run_simulation


@pytest.fixture(scope="session")
def baseline():
    """The default 365-day run, shared read-only across tests."""
    config = SimConfig()
    conn = create_database()
    summary = run_simulation(conn, config)
    yield conn, config, summary
    conn.close()


@pytest.fixture(scope="session")
def full_year(tmp_path_factory):
    root = tmp_path_factory.mktemp("year")
    results = build_etl_results(root)
    return root, results, results["evaluation"]
