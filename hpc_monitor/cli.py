"""Command-line entry point. Run with `python3 -m hpc_monitor`."""

from __future__ import annotations

import argparse
import random
import sys
import webbrowser
from pathlib import Path

from .config import load_config
from .pipeline import build_report
from .render import render_html


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hpc_monitor",
        description="Generate an HPC cluster health dashboard.")
    parser.add_argument("--config", type=Path, default=None,
                        help="Path to TOML config file (default: search cwd)")
    parser.add_argument("--source", choices=["sim", "slurm", "auto"], default=None,
                        help="Data source (overrides config)")
    parser.add_argument("--nodes", type=int, default=None,
                        help="Simulated node count (overrides config)")
    parser.add_argument("--cluster-name", default=None,
                        help="Cluster name (overrides config)")
    parser.add_argument("--output", type=Path, default=Path("cluster_health.html"),
                        help="Output HTML file path")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible simulation")
    parser.add_argument("--no-history", action="store_true",
                        help="Skip writing this run to the history database")
    parser.add_argument("--open", action="store_true",
                        help="Open the report in the default browser")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    if args.source is not None:
        cfg.source = args.source
    if args.nodes is not None:
        cfg.sim_nodes = args.nodes
    if args.cluster_name is not None:
        cfg.cluster_name = args.cluster_name

    if args.seed is not None:
        random.seed(args.seed)

    print(f"Collecting metrics for cluster '{cfg.cluster_name}' "
          f"(source: {cfg.source})...")

    report = build_report(cfg, write_history=not args.no_history)

    print(f"  - {len(report.nodes)} nodes")
    print(f"  - {len(report.jobs)} jobs "
          f"({sum(1 for j in report.jobs if j.state == 'RUNNING')} running)")
    print(f"  - {len(report.filesystems)} filesystems")
    print(f"  - {len(report.ib_links)} IB links")
    print(f"  - {len(report.alerts)} alerts")
    if not args.no_history:
        print(f"  - history: {len(report.history)} snapshot(s) in window")

    args.output.write_text(render_html(report, cfg.history_display_days),
                           encoding="utf-8")
    print(f"\nReport written to {args.output.resolve()}")

    if args.open:
        webbrowser.open(args.output.resolve().as_uri())

    return 0


if __name__ == "__main__":
    sys.exit(main())
