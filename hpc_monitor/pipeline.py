"""Pipeline that ties collectors, alerting, and history together."""

from __future__ import annotations

import datetime as dt
import sqlite3
import sys

from .alerts import build_snapshot, evaluate_alerts
from .collectors import filesystems as fs_real
from .collectors import sim
from .collectors import slurm
from .config import Config
from .history import open_history, prune_history, read_history, write_snapshot
from .models import ClusterReport


def collect_report(cfg: Config, source: str) -> ClusterReport:
    """Build a ClusterReport using the requested source."""
    resolved = source
    if source == "auto":
        resolved = "slurm" if slurm.slurm_available() else "sim"

    if resolved == "slurm":
        if not slurm.slurm_available():
            print("error: --source slurm requested but sinfo/squeue not on PATH",
                  file=sys.stderr)
            sys.exit(2)
        nodes = slurm.collect_compute_nodes()
        jobs = slurm.collect_jobs()
    elif resolved == "sim":
        nodes = sim.collect_compute_nodes(cfg.sim_nodes)
        jobs = sim.collect_jobs(cfg.sim_nodes)
    else:
        print(f"error: unknown source '{source}'", file=sys.stderr)
        sys.exit(2)

    if cfg.partitions_include:
        keep = set(cfg.partitions_include)
        nodes = [n for n in nodes if n.partition in keep]
        jobs = [j for j in jobs if j.partition in keep]

    # Filesystem collection: in slurm/real mode, try the real parsers
    # (lfs df, df -hT, beegfs-df). If none of those commands are available
    # on this host, fall back to simulation so the dashboard still renders.
    # InfiniBand collection is still simulation-only — phase 3 adds real.
    if resolved == "slurm":
        filesystems = fs_real.collect_all()
        if not filesystems:
            filesystems = sim.collect_filesystems()
    else:
        filesystems = sim.collect_filesystems()

    return ClusterReport(
        generated_at=dt.datetime.now(),
        cluster_name=cfg.cluster_name,
        source=resolved,
        nodes=nodes,
        jobs=jobs,
        filesystems=filesystems,
        ib_links=sim.collect_infiniband(),
    )


def build_report(cfg: Config, write_history: bool = True) -> ClusterReport:
    """End-to-end: collect, evaluate alerts, persist + load history."""
    report = collect_report(cfg, cfg.source)
    report.alerts = evaluate_alerts(report, cfg.thresholds)

    if write_history:
        try:
            conn = open_history(cfg.history_db)
            snap = build_snapshot(report)
            write_snapshot(conn, cfg.cluster_name, snap)
            prune_history(conn, cfg.history_retention_days)
            report.history = read_history(
                conn, cfg.cluster_name, cfg.history_display_days)
            conn.close()
        except sqlite3.Error as e:
            print(f"warning: history database error: {e}", file=sys.stderr)

    return report
