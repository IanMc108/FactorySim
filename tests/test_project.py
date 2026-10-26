"""Checks that keep the published artefacts honest."""

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from factorysim.analysis import reference_model
from factorysim.cli import main
from factorysim.config import SimConfig
from factorysim.db import create_database, validate_reference_data
from factorysim.report import (
    ETL_BEGIN,
    ETL_END,
    README_BEGIN,
    README_END,
    render_etl_block,
    render_readme_block,
)

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def results():
    return json.loads((ROOT / "docs" / "results.json").read_text(encoding="utf-8"))


def test_readme_numbers_match_results_json(results):
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    start, end = readme.index(README_BEGIN), readme.index(README_END) + len(README_END)
    assert readme[start:end] == render_readme_block(results), (
        "README results block is stale: run `factorysim report`"
    )


def test_results_json_matches_current_model(results):
    config = SimConfig()
    assert results["config"] == asdict(config)
    model = reference_model(config)
    assert results["model"]["optimal_interval"] == model.optimal_interval()
    for key in ("baseline", "optimal", "original_claim"):
        row = results[key]
        loss, spend, events = model.horizon_cost(row["interval"], config.days)
        assert (row["decay_loss"], row["maintenance_spend"], row["events"]) == (loss, spend, events)


def test_results_json_baseline_run_is_reproducible(results, baseline):
    _, _, summary = baseline
    assert results["baseline_run"]["revenue"] == summary.revenue
    assert results["baseline_run"]["closing_cash"] == summary.closing_cash


def test_charts_exist():
    for name in ("daily_revenue.png", "interval_cost.png"):
        assert (ROOT / "docs" / "img" / name).stat().st_size > 10_000


def test_reference_data_must_be_interpretable():
    conn = create_database()
    conn.execute("UPDATE products SET base_yield = 0.5 WHERE sku = 'MESH-AIM'")
    with pytest.raises(ValueError, match="sums to"):
        validate_reference_data(conn)


def test_create_database_will_not_clobber_files(tmp_path):
    target = tmp_path / "factory.db"
    target.write_text("not a database")
    with pytest.raises(FileExistsError):
        create_database(target)
    create_database(target, overwrite=True).close()


def test_cli_simulate(tmp_path, capsys):
    db = tmp_path / "out.db"
    main(["simulate", "--days", "20", "--interval", "none", "--db", str(db)])
    out = capsys.readouterr().out
    assert "Simulated 20 days" in out and "0 services" in out
    assert db.exists()


def _block(begin: str, end: str) -> str:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    return readme[readme.index(begin) : readme.index(end) + len(end)]


def test_readme_etl_numbers_match_etl_results_json():
    results = json.loads((ROOT / "docs" / "etl_results.json").read_text(encoding="utf-8"))
    assert _block(ETL_BEGIN, ETL_END) == render_etl_block(results), (
        "README ETL block is stale: run `factorysim report --section etl`"
    )


def test_etl_results_json_matches_a_fresh_run(full_year):
    _, fresh, _ = full_year
    published = json.loads((ROOT / "docs" / "etl_results.json").read_text(encoding="utf-8"))
    assert published == fresh, (
        "docs/etl_results.json is stale: run `factorysim report --section etl`"
    )


def test_samples_exist():
    for name in (
        "mes_production_v1.csv",
        "mes_production_v2.csv",
        "erp_invoices.csv",
        "bank_statement.csv",
        "cmms_work_orders.jsonl",
        "erp_goods_receipts.csv",
    ):
        assert (ROOT / "docs" / "samples" / name).stat().st_size > 100
