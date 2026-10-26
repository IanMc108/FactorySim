"""FactorySim command line.

simulate   run one simulation into a SQLite database
export     simulate, then write messy source-system files and the defect manifest
etl        run the pipeline over the landing zone (incrementally)
evaluate   score the lake against the truth database and defect manifest
demo       export + etl + evaluate in a fresh build directory
report     regenerate docs/ results, charts and the README's generated blocks
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import date
from pathlib import Path

from .config import SimConfig
from .db import create_database
from .simulation import run_simulation


def _interval(value: str) -> int | None:
    return None if value.lower() == "none" else int(value)


def _export(build: Path, seed: int, defect_seed: int) -> None:
    from .sources.export import export_sources

    for sub in ("landing", "truth", "lake"):
        shutil.rmtree(build / sub, ignore_errors=True)
    conn = create_database(build / "truth" / "factory.db", overwrite=True)
    try:
        run_simulation(conn, SimConfig(seed=seed))
        summary = export_sources(
            conn, build / "landing", build / "truth" / "defects.jsonl", seed=defect_seed
        )
    finally:
        conn.close()
    print(
        f"Wrote {summary.files_written} files ({summary.records_written} records) "
        f"to {build / 'landing'}"
    )
    for defect, count in summary.defects.items():
        print(f"  {defect:24} {count}")


def _etl(landing: Path, lake: Path, through: date | None) -> None:
    from .etl.pipeline import run_pipeline

    s = run_pipeline(landing, lake, through)
    print(
        f"Batch {s.batch_id}: loaded {s.files_loaded} files, skipped {s.duplicate_files} "
        f"re-delivered files; watermark {s.watermark}"
    )
    print(f"  {'entity':20} {'records':>8} {'rejected':>8} {'deduped':>8} {'rows':>8}")
    for e in s.entities:
        print(f"  {e.entity:20} {e.records:8} {e.rejected:8} {e.duplicates_removed:8} {e.rows:8}")
    print(
        f"  {'reconciliation':24} {'rows':>6} {'match':>6} {'expl.':>6} {'open':>6} "
        f"{'review':>6} {'exc.':>6}"
    )
    for name, r in s.recon.items():
        print(
            f"  {name:24} {r['rows']:6} {r['matched']:6} {r['explained']:6} {r['open']:6} "
            f"{r['needs_review']:6} {r['exceptions']:6}"
        )


def _evaluate(build: Path) -> dict:
    from .evaluation import evaluate

    results = evaluate(
        build / "truth" / "factory.db", build / "truth" / "defects.jsonl", build / "lake"
    )
    print(f"  {'defect':20} {'injected':>8} {'handled':>8}")
    for d in results["defects"]:
        print(f"  {d['defect']:20} {d['injected']:8} {d['handled']:8}")
    q = results["quarantine"]
    print(
        f"Quarantine: {q['quarantined']} records, precision {q['precision']:.0%}, "
        f"recall {q['recall']:.0%}"
    )
    r = results["reconciliation"]
    print(
        f"Reconciliation: {r['exceptions']} exceptions, {r['business_errors_flagged']}/"
        f"{r['expected_business_errors']} injected business errors flagged"
    )
    for name, c in results["gold_vs_truth"].items():
        print(
            f"  {name:22} {c['cells']:6} cells: {c['exact']} exact, "
            f"{c['explained_by_injected_defects']} explained, {c['unexplained']} unexplained"
        )
    return results


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="factorysim", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    sim = sub.add_parser("simulate", help="run one simulation into a SQLite database")
    sim.add_argument("--days", type=int, default=SimConfig.days)
    sim.add_argument("--seed", type=int, default=SimConfig.seed)
    sim.add_argument(
        "--interval",
        type=_interval,
        default=SimConfig.maintenance_interval_days,
        help="maintenance interval in days, or 'none'",
    )
    sim.add_argument("--db", type=Path, default=Path("build/factory.db"))
    sim.add_argument(
        "--overwrite",
        action="store_true",
        help="replace --db if it exists (always allowed for the default path)",
    )

    for name, help_text in (
        ("export", "write messy source files and the defect manifest"),
        ("demo", "export, run the pipeline and evaluate it"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--build", type=Path, default=Path("build"))
        p.add_argument("--seed", type=int, default=SimConfig.seed, help="simulation seed")
        p.add_argument("--defect-seed", type=int, default=7, help="defect injection seed")

    etl = sub.add_parser("etl", help="run the pipeline over newly landed files")
    etl.add_argument("--landing", type=Path, default=Path("build/landing"))
    etl.add_argument("--lake", type=Path, default=Path("build/lake"))
    etl.add_argument(
        "--through",
        type=date.fromisoformat,
        help="only process files that arrived on or before this date",
    )

    ev = sub.add_parser("evaluate", help="score the lake against the truth and manifest")
    ev.add_argument("--build", type=Path, default=Path("build"))
    ev.add_argument("--json", type=Path, help="also write the full results here")

    rep = sub.add_parser("report", help="regenerate docs/ and the README's generated blocks")
    rep.add_argument("--section", choices=("all", "analysis", "etl"), default="all")
    rep.add_argument(
        "--table-seeds",
        type=int,
        default=200,
        help="seeds per interval for the README cross-check table",
    )
    rep.add_argument(
        "--sweep-seeds", type=int, default=50, help="seeds per interval for the cost-curve chart"
    )
    rep.add_argument("--root", type=Path, default=Path("."))

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s %(message)s"
    )

    if args.command == "simulate":
        config = SimConfig(days=args.days, seed=args.seed, maintenance_interval_days=args.interval)
        overwrite = args.overwrite or args.db == Path("build/factory.db")
        conn = create_database(args.db, overwrite=overwrite)
        try:
            summary = run_simulation(conn, config)
        finally:
            conn.close()
        print(f"Simulated {summary.days} days into {args.db}")
        print(f"  revenue            ${summary.revenue:,.0f}")
        print(f"  material purchases ${summary.material_spend:,.0f}")
        print(
            f"  maintenance        ${summary.maintenance_spend:,.0f} "
            f"({summary.maintenance_events} services)"
        )
        print(f"  closing cash       ${summary.closing_cash:,.0f}")
        print(f"  station stoppages  {summary.station_stoppages}")
    elif args.command == "export":
        _export(args.build, args.seed, args.defect_seed)
    elif args.command == "etl":
        _etl(args.landing, args.lake, args.through)
    elif args.command == "evaluate":
        results = _evaluate(args.build)
        if args.json:
            args.json.write_text(json.dumps(results, indent=2, default=str) + "\n")
    elif args.command == "demo":
        _export(args.build, args.seed, args.defect_seed)
        _etl(args.build / "landing", args.build / "lake", None)
        _evaluate(args.build)
    else:
        from .report import write_etl_report, write_report

        if args.section in ("all", "analysis"):
            results = write_report(
                args.root,
                SimConfig(),
                list(range(args.table_seeds)),
                list(range(args.sweep_seeds)),
                range(5, 41),
            )
            opt = results["optimal"]
            print(
                f"Optimal service interval: {opt['interval']} days "
                f"(expected annual cost ${opt['total_cost']:,.0f})"
            )
        if args.section in ("all", "etl"):
            write_etl_report(args.root)
            print("Wrote docs/etl_results.json, docs/samples/ and the README ETL block")


if __name__ == "__main__":
    main()
