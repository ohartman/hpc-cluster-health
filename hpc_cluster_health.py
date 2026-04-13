#!/usr/bin/env python3
"""
hpc_cluster_health.py — HPC cluster health monitoring and reporting tool.

Collects (or simulates) operational metrics from a high performance computing
environment and produces a dark-themed HTML dashboard suitable for an
operations center display.

The script models the data sources a real HPC administrator would query:
    - Slurm job scheduler            (squeue, sinfo, scontrol show node)
    - Parallel filesystem health     (lfs df -h, beegfs-ctl --listtargets)
    - InfiniBand fabric              (ibstat, ibdiagnet)
    - Node-level metrics             (uptime, free -m, sensors)

In this implementation the data is synthesized so the tool can be demonstrated
on a workstation without access to a live cluster. Each data-collection
function is structured so the simulated block can be swapped for a real
subprocess call to the corresponding cluster command.

Usage:
    python3 hpc_cluster_health.py
    python3 hpc_cluster_health.py --nodes 128 --output cluster_report.html
    python3 hpc_cluster_health.py --seed 42 --open

Author: Owen Hartman
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import html
import random
import sys
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Thresholds:
    """Alerting thresholds. In production these would live in a config file
    (YAML/TOML) and be tunable per cluster partition."""
    load_warning: float = 0.85          # 85% of core count
    load_critical: float = 1.10         # oversubscribed
    memory_warning: float = 0.85
    memory_critical: float = 0.95
    storage_warning: float = 0.80
    storage_critical: float = 0.92
    queue_wait_warning_hours: float = 4.0
    queue_wait_critical_hours: float = 12.0


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ComputeNode:
    name: str
    partition: str
    state: str          # idle, allocated, mixed, down, drain, maint
    cores_total: int
    cores_alloc: int
    mem_total_gb: int
    mem_used_gb: float
    load_1min: float
    load_5min: float
    load_15min: float
    gpu_count: int
    gpu_alloc: int
    uptime_days: int
    reason: str = ""    # populated when state is down/drain

    @property
    def load_ratio(self) -> float:
        return self.load_5min / max(self.cores_total, 1)

    @property
    def mem_ratio(self) -> float:
        return self.mem_used_gb / max(self.mem_total_gb, 1)


@dataclass
class Job:
    job_id: int
    user: str
    account: str
    partition: str
    name: str
    state: str          # PENDING, RUNNING, COMPLETING, FAILED
    nodes: int
    cores: int
    submit_time: dt.datetime
    start_time: dt.datetime | None
    time_limit_hours: float
    reason: str = ""

    @property
    def wait_hours(self) -> float:
        end = self.start_time or dt.datetime.now()
        return max((end - self.submit_time).total_seconds() / 3600.0, 0.0)

    @property
    def runtime_hours(self) -> float:
        if not self.start_time:
            return 0.0
        return (dt.datetime.now() - self.start_time).total_seconds() / 3600.0


@dataclass
class Filesystem:
    name: str
    mount: str
    fs_type: str        # lustre, beegfs, nfs, gpfs
    total_tb: float
    used_tb: float
    inodes_used_pct: float
    read_gbps: float
    write_gbps: float
    osts_total: int = 0     # Object Storage Targets (Lustre concept)
    osts_down: int = 0

    @property
    def used_ratio(self) -> float:
        return self.used_tb / max(self.total_tb, 1)


@dataclass
class InfiniBandLink:
    switch: str
    port: str
    speed_gbps: int
    state: str          # Active, Down, Polling
    error_count: int


@dataclass
class Alert:
    severity: str       # critical, warning, info
    component: str
    message: str


@dataclass
class ClusterReport:
    generated_at: dt.datetime
    cluster_name: str
    nodes: list[ComputeNode]
    jobs: list[Job]
    filesystems: list[Filesystem]
    ib_links: list[InfiniBandLink]
    alerts: list[Alert] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Data collection (simulated)
# ---------------------------------------------------------------------------
#
# Each collect_* function below mirrors what a production version would do
# against a real cluster. The shape of the returned data matches what you'd
# get after parsing the corresponding command output.

PARTITIONS = ["compute", "gpu", "bigmem", "debug"]
USERS = ["jchen", "mpatel", "kowalski", "nasser", "rodriguez", "okonkwo",
         "tanaka", "fitzgerald", "ahmadi", "vasquez", "lindberg", "park"]
ACCOUNTS = ["physics", "biochem", "cs-ml", "astro", "climate", "engineering"]
JOB_NAMES = ["lammps_run", "vasp_relax", "gromacs_md", "tf_train",
             "openfoam_sim", "mpi_solve", "namd_protein", "ansys_cfd"]


def collect_compute_nodes(count: int) -> list[ComputeNode]:
    """Simulate `sinfo -N -o '%N %P %T %C %m %O'` + `scontrol show node`.

    In production:
        result = subprocess.run(
            ["sinfo", "-N", "-h", "-o", "%N|%P|%T|%C|%m|%O|%e"],
            capture_output=True, text=True, check=True,
        )
        return [parse_sinfo_line(line) for line in result.stdout.splitlines()]
    """
    nodes: list[ComputeNode] = []
    for i in range(count):
        partition = random.choices(
            PARTITIONS, weights=[60, 25, 10, 5], k=1
        )[0]

        # Realistic node sizing per partition
        if partition == "gpu":
            cores_total = 64
            mem_total = 512
            gpu_count = 4
        elif partition == "bigmem":
            cores_total = 96
            mem_total = 1536
            gpu_count = 0
        elif partition == "debug":
            cores_total = 32
            mem_total = 192
            gpu_count = 0
        else:
            cores_total = 48
            mem_total = 384
            gpu_count = 0

        # State distribution: most healthy, a few problems
        state = random.choices(
            ["allocated", "mixed", "idle", "drain", "down", "maint"],
            weights=[55, 20, 18, 4, 2, 1],
            k=1,
        )[0]

        if state in ("down", "drain", "maint"):
            cores_alloc = 0
            mem_used = 0.0
            load = 0.0
            gpu_alloc = 0
            reason = random.choice([
                "Not responding",
                "kernel panic 04:23 UTC",
                "Scheduled maintenance window",
                "IB port flapping",
                "GPU ECC errors",
                "DIMM failure slot 3",
            ])
        else:
            if state == "allocated":
                util = random.uniform(0.85, 1.05)
            elif state == "mixed":
                util = random.uniform(0.40, 0.85)
            else:  # idle
                util = random.uniform(0.0, 0.10)

            cores_alloc = min(cores_total, int(cores_total * util))
            mem_used = round(mem_total * util * random.uniform(0.7, 1.05), 1)
            mem_used = min(mem_used, mem_total * 0.99)
            load = round(cores_total * util * random.uniform(0.9, 1.1), 2)
            gpu_alloc = min(gpu_count, int(gpu_count * util)) if gpu_count else 0
            reason = ""

        nodes.append(ComputeNode(
            name=f"cn{i+1:03d}",
            partition=partition,
            state=state,
            cores_total=cores_total,
            cores_alloc=cores_alloc,
            mem_total_gb=mem_total,
            mem_used_gb=mem_used,
            load_1min=round(load * random.uniform(0.95, 1.05), 2),
            load_5min=load,
            load_15min=round(load * random.uniform(0.92, 1.03), 2),
            gpu_count=gpu_count,
            gpu_alloc=gpu_alloc,
            uptime_days=random.randint(1, 180),
            reason=reason,
        ))
    return nodes


def collect_jobs(node_count: int) -> list[Job]:
    """Simulate `squeue -o '%i|%u|%a|%P|%j|%T|%D|%C|%V|%S|%l|%r'`.

    In production:
        result = subprocess.run(
            ["squeue", "-h", "-o", "%i|%u|%a|%P|%j|%T|%D|%C|%V|%S|%l|%r"],
            capture_output=True, text=True, check=True,
        )
    """
    now = dt.datetime.now()
    jobs: list[Job] = []
    job_count = max(20, node_count * 2)

    for i in range(job_count):
        state = random.choices(
            ["RUNNING", "PENDING", "COMPLETING", "FAILED"],
            weights=[55, 38, 5, 2],
            k=1,
        )[0]

        nodes = random.choices(
            [1, 2, 4, 8, 16, 32], weights=[35, 25, 20, 12, 6, 2], k=1
        )[0]
        cores_per_node = random.choice([16, 24, 32, 48])
        submit_offset_hours = random.uniform(0.1, 36.0)
        submit_time = now - dt.timedelta(hours=submit_offset_hours)

        if state == "RUNNING":
            start_offset = random.uniform(0.0, submit_offset_hours - 0.05)
            start_time = now - dt.timedelta(
                hours=submit_offset_hours - start_offset
            )
            reason = "None"
        elif state == "PENDING":
            start_time = None
            reason = random.choices(
                ["Resources", "Priority", "QOSMaxCpuPerUserLimit",
                 "ReqNodeNotAvail", "Dependency"],
                weights=[50, 30, 8, 8, 4],
                k=1,
            )[0]
        else:
            start_time = now - dt.timedelta(hours=random.uniform(0.5, 12))
            reason = "None"

        jobs.append(Job(
            job_id=1_000_000 + i,
            user=random.choice(USERS),
            account=random.choice(ACCOUNTS),
            partition=random.choices(
                PARTITIONS, weights=[60, 25, 10, 5], k=1
            )[0],
            name=random.choice(JOB_NAMES),
            state=state,
            nodes=nodes,
            cores=nodes * cores_per_node,
            submit_time=submit_time,
            start_time=start_time,
            time_limit_hours=random.choice([1, 4, 12, 24, 48, 72]),
            reason=reason,
        ))
    return jobs


def collect_filesystems() -> list[Filesystem]:
    """Simulate `lfs df -h` for Lustre and `df -h` for the rest.

    In production each fs_type would have its own collection function:
        - Lustre:  `lfs df -h /scratch` plus `lctl get_param osc.*.state`
        - BeeGFS:  `beegfs-df` and `beegfs-ctl --listtargets --state`
        - GPFS:    `mmlsfs` and `mmhealth node show`
        - NFS:     `df -h` and `nfsstat`
    """
    return [
        Filesystem(
            name="scratch",
            mount="/scratch",
            fs_type="lustre",
            total_tb=2048.0,
            used_tb=round(random.uniform(1400, 1850), 1),
            inodes_used_pct=round(random.uniform(40, 75), 1),
            read_gbps=round(random.uniform(45, 88), 1),
            write_gbps=round(random.uniform(30, 70), 1),
            osts_total=64,
            osts_down=random.choices([0, 0, 0, 1, 2], weights=[60, 20, 10, 8, 2], k=1)[0],
        ),
        Filesystem(
            name="home",
            mount="/home",
            fs_type="nfs",
            total_tb=128.0,
            used_tb=round(random.uniform(70, 115), 1),
            inodes_used_pct=round(random.uniform(55, 85), 1),
            read_gbps=round(random.uniform(2, 8), 2),
            write_gbps=round(random.uniform(1, 5), 2),
        ),
        Filesystem(
            name="projects",
            mount="/projects",
            fs_type="beegfs",
            total_tb=1024.0,
            used_tb=round(random.uniform(600, 920), 1),
            inodes_used_pct=round(random.uniform(30, 60), 1),
            read_gbps=round(random.uniform(20, 55), 1),
            write_gbps=round(random.uniform(15, 40), 1),
            osts_total=32,
            osts_down=0,
        ),
        Filesystem(
            name="archive",
            mount="/archive",
            fs_type="gpfs",
            total_tb=4096.0,
            used_tb=round(random.uniform(2800, 3500), 1),
            inodes_used_pct=round(random.uniform(20, 40), 1),
            read_gbps=round(random.uniform(8, 20), 1),
            write_gbps=round(random.uniform(4, 12), 1),
        ),
    ]


def collect_infiniband() -> list[InfiniBandLink]:
    """Simulate `ibstat` / `ibdiagnet --pc` summary.

    In production this would parse `ibdiagnet` output for symbol errors,
    link downed counts, and port state, then aggregate per-switch.
    """
    links: list[InfiniBandLink] = []
    for switch_idx in range(4):
        for port_idx in range(8):
            state = random.choices(
                ["Active", "Active", "Active", "Active", "Polling", "Down"],
                weights=[80, 8, 5, 3, 3, 1],
                k=1,
            )[0]
            links.append(InfiniBandLink(
                switch=f"ib-sw{switch_idx+1:02d}",
                port=f"{port_idx+1}/1",
                speed_gbps=200,  # HDR InfiniBand
                state=state,
                error_count=random.choices(
                    [0, 0, 0, random.randint(1, 50), random.randint(100, 800)],
                    weights=[70, 15, 8, 5, 2],
                    k=1,
                )[0],
            ))
    return links


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def evaluate_alerts(report: ClusterReport, t: Thresholds) -> list[Alert]:
    """Apply thresholds across the collected data and produce alert objects."""
    alerts: list[Alert] = []

    # Node-level
    for node in report.nodes:
        if node.state == "down":
            alerts.append(Alert(
                "critical", f"node/{node.name}",
                f"Node DOWN — {node.reason or 'no reason reported'}"
            ))
        elif node.state == "drain":
            alerts.append(Alert(
                "warning", f"node/{node.name}",
                f"Node draining — {node.reason or 'no reason reported'}"
            ))
        elif node.state in ("allocated", "mixed", "idle"):
            if node.load_ratio >= t.load_critical:
                alerts.append(Alert(
                    "critical", f"node/{node.name}",
                    f"Load {node.load_5min:.1f} on {node.cores_total} cores "
                    f"({node.load_ratio:.0%} of capacity)"
                ))
            elif node.load_ratio >= t.load_warning:
                alerts.append(Alert(
                    "warning", f"node/{node.name}",
                    f"High load: {node.load_5min:.1f} "
                    f"({node.load_ratio:.0%} of capacity)"
                ))
            if node.mem_ratio >= t.memory_critical:
                alerts.append(Alert(
                    "critical", f"node/{node.name}",
                    f"Memory pressure {node.mem_ratio:.0%} "
                    f"({node.mem_used_gb:.0f}/{node.mem_total_gb} GB)"
                ))
            elif node.mem_ratio >= t.memory_warning:
                alerts.append(Alert(
                    "warning", f"node/{node.name}",
                    f"Memory at {node.mem_ratio:.0%}"
                ))

    # Filesystem
    for fs in report.filesystems:
        if fs.used_ratio >= t.storage_critical:
            alerts.append(Alert(
                "critical", f"fs/{fs.name}",
                f"{fs.fs_type.upper()} {fs.mount} at {fs.used_ratio:.0%} "
                f"({fs.used_tb:.0f}/{fs.total_tb:.0f} TB)"
            ))
        elif fs.used_ratio >= t.storage_warning:
            alerts.append(Alert(
                "warning", f"fs/{fs.name}",
                f"{fs.fs_type.upper()} {fs.mount} at {fs.used_ratio:.0%}"
            ))
        if fs.osts_down > 0:
            alerts.append(Alert(
                "critical", f"fs/{fs.name}",
                f"{fs.osts_down} of {fs.osts_total} OSTs offline"
            ))

    # Job queue
    pending = [j for j in report.jobs if j.state == "PENDING"]
    long_waits = [j for j in pending if j.wait_hours >= t.queue_wait_critical_hours]
    medium_waits = [
        j for j in pending
        if t.queue_wait_warning_hours <= j.wait_hours < t.queue_wait_critical_hours
    ]
    if long_waits:
        alerts.append(Alert(
            "warning", "scheduler",
            f"{len(long_waits)} job(s) pending over "
            f"{t.queue_wait_critical_hours:.0f}h"
        ))
    if medium_waits:
        alerts.append(Alert(
            "info", "scheduler",
            f"{len(medium_waits)} job(s) pending over "
            f"{t.queue_wait_warning_hours:.0f}h"
        ))

    failed = [j for j in report.jobs if j.state == "FAILED"]
    if failed:
        alerts.append(Alert(
            "warning", "scheduler",
            f"{len(failed)} job(s) in FAILED state"
        ))

    # InfiniBand
    down_links = [l for l in report.ib_links if l.state == "Down"]
    error_links = [l for l in report.ib_links if l.error_count > 100]
    if down_links:
        alerts.append(Alert(
            "critical", "infiniband",
            f"{len(down_links)} IB link(s) DOWN"
        ))
    if error_links:
        alerts.append(Alert(
            "warning", "infiniband",
            f"{len(error_links)} IB link(s) with elevated error counts"
        ))

    # Sort: critical first, then warning, then info
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: severity_order.get(a.severity, 99))
    return alerts


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

CSS = """
:root {
    --bg: #0d1117;
    --bg-elev: #161b22;
    --bg-row: #1c2128;
    --border: #30363d;
    --text: #c9d1d9;
    --text-dim: #8b949e;
    --accent: #58a6ff;
    --ok: #3fb950;
    --warn: #d29922;
    --crit: #f85149;
    --info: #58a6ff;
}
* { box-sizing: border-box; }
body {
    margin: 0;
    font-family: -apple-system, "SF Pro Text", "Segoe UI", system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
}
header {
    background: var(--bg-elev);
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex;
    justify-content: space-between;
    align-items: baseline;
}
header h1 { margin: 0; font-size: 22px; font-weight: 600; }
header .meta { color: var(--text-dim); font-size: 13px; }
main { padding: 24px 32px; max-width: 1600px; margin: 0 auto; }
section { margin-bottom: 32px; }
section h2 {
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-dim);
    border-bottom: 1px solid var(--border);
    padding-bottom: 8px;
    margin: 0 0 16px;
    font-weight: 600;
}
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
}
.kpi {
    background: var(--bg-elev);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
}
.kpi .label {
    color: var(--text-dim);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.kpi .value {
    font-size: 28px;
    font-weight: 600;
    margin-top: 6px;
    font-variant-numeric: tabular-nums;
}
.kpi .sub { color: var(--text-dim); font-size: 12px; margin-top: 4px; }
table {
    width: 100%;
    border-collapse: collapse;
    background: var(--bg-elev);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
    font-variant-numeric: tabular-nums;
}
th, td {
    text-align: left;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
}
th {
    background: var(--bg-row);
    color: var(--text-dim);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.05em;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg-row); }
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
.badge-ok { background: rgba(63, 185, 80, 0.15); color: var(--ok); }
.badge-warn { background: rgba(210, 153, 34, 0.15); color: var(--warn); }
.badge-crit { background: rgba(248, 81, 73, 0.15); color: var(--crit); }
.badge-info { background: rgba(88, 166, 255, 0.15); color: var(--info); }
.badge-dim { background: rgba(139, 148, 158, 0.15); color: var(--text-dim); }
.bar {
    background: var(--bg-row);
    border-radius: 3px;
    height: 6px;
    overflow: hidden;
    margin-top: 4px;
}
.bar-fill { height: 100%; transition: width 0.3s; }
.bar-ok { background: var(--ok); }
.bar-warn { background: var(--warn); }
.bar-crit { background: var(--crit); }
.alert {
    background: var(--bg-elev);
    border: 1px solid var(--border);
    border-left: 3px solid var(--text-dim);
    border-radius: 4px;
    padding: 10px 14px;
    margin-bottom: 8px;
    display: flex;
    gap: 12px;
    align-items: baseline;
}
.alert-critical { border-left-color: var(--crit); }
.alert-warning { border-left-color: var(--warn); }
.alert-info { border-left-color: var(--info); }
.alert .component {
    color: var(--text-dim);
    font-family: "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    min-width: 180px;
}
.two-col {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 24px;
}
@media (max-width: 1100px) { .two-col { grid-template-columns: 1fr; } }
footer {
    text-align: center;
    color: var(--text-dim);
    font-size: 12px;
    padding: 24px;
    border-top: 1px solid var(--border);
    margin-top: 32px;
}
.mono { font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 12px; }
"""


def severity_class(sev: str) -> str:
    return {"critical": "crit", "warning": "warn", "info": "info"}.get(sev, "dim")


def state_badge(state: str) -> str:
    cls = {
        "idle": "ok",
        "allocated": "info",
        "mixed": "info",
        "drain": "warn",
        "down": "crit",
        "maint": "dim",
        "RUNNING": "ok",
        "PENDING": "warn",
        "COMPLETING": "info",
        "FAILED": "crit",
        "Active": "ok",
        "Polling": "warn",
        "Down": "crit",
    }.get(state, "dim")
    return f'<span class="badge badge-{cls}">{html.escape(state)}</span>'


def utilization_bar(ratio: float) -> str:
    pct = max(0.0, min(ratio, 1.0)) * 100
    if ratio >= 0.92:
        cls = "crit"
    elif ratio >= 0.80:
        cls = "warn"
    else:
        cls = "ok"
    return (
        f'<div class="bar"><div class="bar-fill bar-{cls}" '
        f'style="width: {pct:.1f}%"></div></div>'
    )


def render_kpis(report: ClusterReport) -> str:
    nodes = report.nodes
    total_cores = sum(n.cores_total for n in nodes)
    alloc_cores = sum(n.cores_alloc for n in nodes)
    healthy = sum(1 for n in nodes if n.state in ("idle", "allocated", "mixed"))
    down = sum(1 for n in nodes if n.state == "down")
    drain = sum(1 for n in nodes if n.state == "drain")

    running = sum(1 for j in report.jobs if j.state == "RUNNING")
    pending = sum(1 for j in report.jobs if j.state == "PENDING")

    total_storage = sum(f.total_tb for f in report.filesystems)
    used_storage = sum(f.used_tb for f in report.filesystems)

    util_pct = (alloc_cores / total_cores * 100) if total_cores else 0

    kpis = [
        ("Cluster utilization", f"{util_pct:.1f}%",
         f"{alloc_cores:,} / {total_cores:,} cores allocated"),
        ("Healthy nodes", f"{healthy}",
         f"{down} down, {drain} draining"),
        ("Jobs running", f"{running}",
         f"{pending} pending in queue"),
        ("Storage used", f"{used_storage/1024:.1f} PB",
         f"of {total_storage/1024:.1f} PB total"),
    ]
    cards = "".join(
        f'<div class="kpi"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div>'
        f'<div class="sub">{html.escape(sub)}</div></div>'
        for label, value, sub in kpis
    )
    return f'<div class="kpi-grid">{cards}</div>'


def render_alerts(alerts: list[Alert]) -> str:
    if not alerts:
        return ('<div class="alert alert-info">'
                '<span class="component">cluster</span>'
                '<span>All systems nominal — no active alerts.</span></div>')
    rows = []
    for a in alerts[:25]:
        rows.append(
            f'<div class="alert alert-{a.severity}">'
            f'<span class="component">{html.escape(a.component)}</span>'
            f'<span class="badge badge-{severity_class(a.severity)}">'
            f'{a.severity}</span>'
            f'<span>{html.escape(a.message)}</span></div>'
        )
    if len(alerts) > 25:
        rows.append(
            f'<div class="alert"><span>... and {len(alerts) - 25} more</span></div>'
        )
    return "".join(rows)


def render_node_table(nodes: list[ComputeNode]) -> str:
    # Show problem nodes first, then a sample of healthy ones
    problem = [n for n in nodes if n.state in ("down", "drain", "maint")
               or n.load_ratio >= 0.85 or n.mem_ratio >= 0.85]
    healthy = [n for n in nodes if n not in problem][:15]
    display = problem + healthy

    rows = []
    for n in display:
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(n.name)}</td>"
            f"<td>{html.escape(n.partition)}</td>"
            f"<td>{state_badge(n.state)}</td>"
            f"<td>{n.cores_alloc}/{n.cores_total}{utilization_bar(n.cores_alloc/n.cores_total)}</td>"
            f"<td>{n.mem_used_gb:.0f}/{n.mem_total_gb} GB{utilization_bar(n.mem_ratio)}</td>"
            f"<td>{n.load_5min:.2f}</td>"
            f"<td>{n.gpu_alloc}/{n.gpu_count}</td>"
            f"<td>{n.uptime_days}d</td>"
            f"<td class='mono' style='color:var(--text-dim)'>{html.escape(n.reason)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Node</th><th>Partition</th><th>State</th>"
        "<th>Cores</th><th>Memory</th><th>Load</th>"
        "<th>GPU</th><th>Uptime</th><th>Reason</th>"
        "</tr></thead><tbody>"
        + "".join(rows) +
        "</tbody></table>"
    )


def render_filesystem_table(filesystems: list[Filesystem]) -> str:
    rows = []
    for fs in filesystems:
        ost_info = (f"{fs.osts_total - fs.osts_down}/{fs.osts_total}"
                    if fs.osts_total else "—")
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(fs.mount)}</td>"
            f"<td><span class='badge badge-dim'>{fs.fs_type.upper()}</span></td>"
            f"<td>{fs.used_tb:.0f}/{fs.total_tb:.0f} TB"
            f"{utilization_bar(fs.used_ratio)}</td>"
            f"<td>{fs.inodes_used_pct:.0f}%</td>"
            f"<td>{fs.read_gbps:.1f} GB/s</td>"
            f"<td>{fs.write_gbps:.1f} GB/s</td>"
            f"<td>{ost_info}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Mount</th><th>Type</th><th>Capacity</th>"
        "<th>Inodes</th><th>Read</th><th>Write</th><th>OSTs</th>"
        "</tr></thead><tbody>"
        + "".join(rows) +
        "</tbody></table>"
    )


def render_queue_summary(jobs: list[Job]) -> str:
    by_state: dict[str, int] = {}
    by_partition: dict[str, dict[str, int]] = {}
    for j in jobs:
        by_state[j.state] = by_state.get(j.state, 0) + 1
        by_partition.setdefault(j.partition, {"running": 0, "pending": 0})
        if j.state == "RUNNING":
            by_partition[j.partition]["running"] += 1
        elif j.state == "PENDING":
            by_partition[j.partition]["pending"] += 1

    rows = []
    for part in sorted(by_partition):
        d = by_partition[part]
        rows.append(
            f"<tr><td>{html.escape(part)}</td>"
            f"<td>{d['running']}</td>"
            f"<td>{d['pending']}</td></tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Partition</th><th>Running</th><th>Pending</th>"
        "</tr></thead><tbody>"
        + "".join(rows) +
        "</tbody></table>"
    )


def render_top_jobs(jobs: list[Job]) -> str:
    running = sorted(
        (j for j in jobs if j.state == "RUNNING"),
        key=lambda j: j.cores,
        reverse=True,
    )[:10]
    rows = []
    for j in running:
        rows.append(
            "<tr>"
            f"<td class='mono'>{j.job_id}</td>"
            f"<td>{html.escape(j.user)}</td>"
            f"<td>{html.escape(j.name)}</td>"
            f"<td>{j.nodes}</td>"
            f"<td>{j.cores}</td>"
            f"<td>{j.runtime_hours:.1f}h</td>"
            f"<td>{j.time_limit_hours:.0f}h</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Job ID</th><th>User</th><th>Name</th>"
        "<th>Nodes</th><th>Cores</th><th>Runtime</th><th>Limit</th>"
        "</tr></thead><tbody>"
        + "".join(rows) +
        "</tbody></table>"
    )


def render_infiniband_summary(links: list[InfiniBandLink]) -> str:
    by_switch: dict[str, dict[str, int]] = {}
    for l in links:
        by_switch.setdefault(l.switch, {"active": 0, "down": 0, "polling": 0, "errors": 0})
        if l.state == "Active":
            by_switch[l.switch]["active"] += 1
        elif l.state == "Down":
            by_switch[l.switch]["down"] += 1
        else:
            by_switch[l.switch]["polling"] += 1
        if l.error_count > 100:
            by_switch[l.switch]["errors"] += 1

    rows = []
    for sw in sorted(by_switch):
        d = by_switch[sw]
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(sw)}</td>"
            f"<td>{d['active']}</td>"
            f"<td>{d['down']}</td>"
            f"<td>{d['polling']}</td>"
            f"<td>{d['errors']}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Switch</th><th>Active</th><th>Down</th>"
        "<th>Polling</th><th>High Errors</th>"
        "</tr></thead><tbody>"
        + "".join(rows) +
        "</tbody></table>"
    )


def render_html(report: ClusterReport) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{html.escape(report.cluster_name)} — Cluster Health</title>
<style>{CSS}</style>
</head>
<body>
<header>
    <h1>{html.escape(report.cluster_name)} <span style="color:var(--text-dim);font-weight:400">cluster health</span></h1>
    <div class="meta">Generated {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}</div>
</header>
<main>
    <section>
        <h2>Overview</h2>
        {render_kpis(report)}
    </section>

    <section>
        <h2>Active alerts ({len(report.alerts)})</h2>
        {render_alerts(report.alerts)}
    </section>

    <section>
        <h2>Parallel filesystems</h2>
        {render_filesystem_table(report.filesystems)}
    </section>

    <div class="two-col">
        <section>
            <h2>Top running jobs by core count</h2>
            {render_top_jobs(report.jobs)}
        </section>
        <section>
            <h2>Queue by partition</h2>
            {render_queue_summary(report.jobs)}
        </section>
    </div>

    <section>
        <h2>InfiniBand fabric (HDR 200 Gb/s)</h2>
        {render_infiniband_summary(report.ib_links)}
    </section>

    <section>
        <h2>Compute nodes — problems and sample</h2>
        {render_node_table(report.nodes)}
    </section>
</main>
<footer>
    Generated by hpc_cluster_health.py — data simulated for demonstration.
    In production, this report would be regenerated every 60 seconds and
    served via a small HTTP daemon or written to a shared web root.
</footer>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate an HPC cluster health dashboard.",
    )
    parser.add_argument("--nodes", type=int, default=64,
                        help="Number of compute nodes to simulate (default: 64)")
    parser.add_argument("--cluster-name", default="aurora",
                        help="Cluster name to display in the report")
    parser.add_argument("--output", type=Path, default=Path("cluster_health.html"),
                        help="Output HTML file path")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible output")
    parser.add_argument("--open", action="store_true",
                        help="Open the report in the default browser")
    args = parser.parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)

    print(f"Collecting metrics for cluster '{args.cluster_name}' "
          f"({args.nodes} nodes)...")

    report = ClusterReport(
        generated_at=dt.datetime.now(),
        cluster_name=args.cluster_name,
        nodes=collect_compute_nodes(args.nodes),
        jobs=collect_jobs(args.nodes),
        filesystems=collect_filesystems(),
        ib_links=collect_infiniband(),
    )
    report.alerts = evaluate_alerts(report, Thresholds())

    print(f"  - {len(report.nodes)} nodes")
    print(f"  - {len(report.jobs)} jobs "
          f"({sum(1 for j in report.jobs if j.state == 'RUNNING')} running)")
    print(f"  - {len(report.filesystems)} filesystems")
    print(f"  - {len(report.ib_links)} IB links")
    print(f"  - {len(report.alerts)} alerts")

    args.output.write_text(render_html(report), encoding="utf-8")
    print(f"\nReport written to {args.output.resolve()}")

    if args.open:
        webbrowser.open(args.output.resolve().as_uri())

    return 0


if __name__ == "__main__":
    sys.exit(main())
