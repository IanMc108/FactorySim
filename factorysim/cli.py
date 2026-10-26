"""Command-line entry point: ``python -m factorysim {simulate,report}``."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import SimConfig
from .db import create_database
from .report import write_report
from .simulation import run_simulation


def _interval(value: str) -> int | None:
    return None if value.lower() == "none" else int(value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="factorysim", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="log every simulated day")
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

    rep = sub.add_parser("report", help="regenerate docs/results.json, charts and README numbers")
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
    else:
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
        print("Wrote docs/results.json, docs/img/*.png and the README results block")


if __name__ == "__main__":
    main()
