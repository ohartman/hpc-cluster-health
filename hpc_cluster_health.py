#!/usr/bin/env python3
"""
hpc_cluster_health.py — HPC cluster health monitoring and reporting tool.

Collects operational metrics from a high performance computing environment
and produces a dark-themed HTML dashboard with current state and historical
trends.

Data sources:
    sim     Synthetic data for development and demos.
    slurm   Real data from `sinfo` and `squeue` (parallel filesystem and
            InfiniBand collectors are still simulated — see TODO markers).
    auto    Try slurm; fall back to sim if the commands are not on PATH.

Configuration is loaded from hpc_monitor.toml (or the path passed to
--config). CLI flags override values from the config file. Each run
appends a snapshot of aggregate metrics to a SQLite history database,
and the report includes trend sparklines from that history.

Usage:
    python3 hpc_cluster_health.py
    python3 hpc_cluster_health.py --source slurm
    python3 hpc_cluster_health.py --config /etc/hpc_monitor.toml --open
    python3 hpc_cluster_health.py --seed 42 --nodes 128

Author: Owen Hartman
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import random
import shutil
import sqlite3
import subprocess
import sys
import tomllib
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Thresholds:
    load_warning: float = 0.85
    load_critical: float = 1.10
    memory_warning: float = 0.85
    memory_critical: float = 0.95
    storage_warning: float = 0.80
    storage_critical: float = 0.92
    queue_wait_warning_hours: float = 4.0
    queue_wait_critical_hours: float = 12.0


@dataclass
class Config:
    cluster_name: str = "aurora"
    source: str = "sim"               # sim | slurm | auto
    sim_nodes: int = 64
    partitions_include: list[str] = field(default_factory=list)
    history_db: Path = Path("history.db")
    history_display_days: int = 7
    history_retention_days: int = 90
    thresholds: Thresholds = field(default_factory=Thresholds)


DEFAULT_CONFIG_NAMES = ("hpc_monitor.toml", "hpc-monitor.toml")


def load_config(path: Path | None) -> Config:
    """Load config from a TOML file. If path is None, search the cwd for
    one of the DEFAULT_CONFIG_NAMES. Missing file is fine — defaults apply."""
    cfg = Config()

    if path is None:
        for name in DEFAULT_CONFIG_NAMES:
            candidate = Path.cwd() / name
            if candidate.exists():
                path = candidate
                break

    if path is None or not path.exists():
        return cfg

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        print(f"warning: could not load config {path}: {e}", file=sys.stderr)
        return cfg

    cluster = data.get("cluster", {})
    cfg.cluster_name = cluster.get("name", cfg.cluster_name)
    cfg.source = cluster.get("source", cfg.source)
    cfg.sim_nodes = cluster.get("sim_nodes", cfg.sim_nodes)

    partitions = data.get("partitions", {})
    cfg.partitions_include = partitions.get("include", [])

    history = data.get("history", {})
    cfg.history_db = Path(history.get("database", str(cfg.history_db)))
    cfg.history_display_days = history.get("display_days", cfg.history_display_days)
    cfg.history_retention_days = history.get("retention_days", cfg.history_retention_days)

    th = data.get("thresholds", {})
    load = th.get("load", {})
    mem = th.get("memory", {})
    storage = th.get("storage", {})
    queue = th.get("queue", {})
    cfg.thresholds = Thresholds(
        load_warning=load.get("warning", Thresholds.load_warning),
        load_critical=load.get("critical", Thresholds.load_critical),
        memory_warning=mem.get("warning", Thresholds.memory_warning),
        memory_critical=mem.get("critical", Thresholds.memory_critical),
        storage_warning=storage.get("warning", Thresholds.storage_warning),
        storage_critical=storage.get("critical", Thresholds.storage_critical),
        queue_wait_warning_hours=queue.get(
            "wait_warning_hours", Thresholds.queue_wait_warning_hours),
        queue_wait_critical_hours=queue.get(
            "wait_critical_hours", Thresholds.queue_wait_critical_hours),
    )

    return cfg


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
    reason: str = ""

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
    osts_total: int = 0
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
class HistorySnapshot:
    """Aggregate metrics persisted on each run for trend analysis."""
    timestamp: dt.datetime
    cores_total: int
    cores_alloc: int
    nodes_healthy: int
    nodes_down: int
    nodes_drain: int
    jobs_running: int
    jobs_pending: int
    storage_used_tb: float
    storage_total_tb: float
    alerts_critical: int
    alerts_warning: int


@dataclass
class ClusterReport:
    generated_at: dt.datetime
    cluster_name: str
    source: str
    nodes: list[ComputeNode]
    jobs: list[Job]
    filesystems: list[Filesystem]
    ib_links: list[InfiniBandLink]
    alerts: list[Alert] = field(default_factory=list)
    history: list[HistorySnapshot] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Data collection — Slurm (real)
# ---------------------------------------------------------------------------

def slurm_available() -> bool:
    return shutil.which("sinfo") is not None and shutil.which("squeue") is not None


SLURM_STATE_MAP = {
    "idle": "idle",
    "alloc": "allocated",
    "allocated": "allocated",
    "mix": "mixed",
    "mixed": "mixed",
    "comp": "allocated",
    "completing": "allocated",
    "down": "down",
    "drain": "drain",
    "drng": "drain",
    "draining": "drain",
    "drained": "drain",
    "maint": "maint",
    "resv": "maint",
    "reserved": "maint",
    "fail": "down",
    "failing": "down",
    "unk": "down",
    "unknown": "down",
}


def normalize_slurm_state(raw: str) -> str:
    """Strip trailing modifiers and map to our canonical state names."""
    base = raw.lower().rstrip("*~$#@+")
    if "drain" in base:
        return "drain"
    if "down" in base:
        return "down"
    if "maint" in base or "resv" in base:
        return "maint"
    return SLURM_STATE_MAP.get(base, base)


def parse_sinfo_cores(field_str: str) -> tuple[int, int]:
    """Parse sinfo's %C field: 'A/I/O/T' = allocated/idle/other/total."""
    parts = field_str.split("/")
    if len(parts) != 4:
        return (0, 0)
    try:
        return (int(parts[0]), int(parts[3]))
    except ValueError:
        return (0, 0)


def collect_compute_nodes_slurm() -> list[ComputeNode]:
    """Run sinfo and parse one node per line.

    Format string fields:
        %N node name      %P partition    %T state
        %C cores A/I/O/T  %m memory MB    %O CPU load
        %e free memory MB %G generic res  %u reason
    """
    fmt = "%N|%P|%T|%C|%m|%O|%e|%G|%u"
    result = subprocess.run(
        ["sinfo", "-N", "-h", "-o", fmt],
        capture_output=True, text=True, check=True,
    )
    nodes: list[ComputeNode] = []
    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) < 9:
            continue
        name, partition, state_raw, cores_raw, mem_raw, load_raw, _free_raw, gres, reason = parts[:9]

        cores_alloc, cores_total = parse_sinfo_cores(cores_raw)
        try:
            mem_total_mb = int(mem_raw)
        except ValueError:
            mem_total_mb = 0
        mem_total_gb = mem_total_mb // 1024

        try:
            load = float(load_raw) if load_raw not in ("N/A", "") else 0.0
        except ValueError:
            load = 0.0

        # Parse GRES like "gpu:4" or "gpu:tesla:4(S:0-1)"
        gpu_count = 0
        if "gpu" in gres.lower():
            for token in gres.split(","):
                if token.lower().startswith("gpu"):
                    bits = token.split(":")
                    try:
                        gpu_count = int(bits[-1].split("(")[0])
                    except (ValueError, IndexError):
                        pass

        state = normalize_slurm_state(state_raw)
        # Memory used isn't directly in sinfo; estimate from allocation ratio
        # as a placeholder. A production version would query
        # `scontrol show node <n>` for AllocMem.
        ratio = (cores_alloc / cores_total) if cores_total else 0
        mem_used_gb = round(mem_total_gb * ratio, 1)

        nodes.append(ComputeNode(
            name=name.strip(),
            partition=partition.strip(),
            state=state,
            cores_total=cores_total,
            cores_alloc=cores_alloc,
            mem_total_gb=mem_total_gb,
            mem_used_gb=mem_used_gb,
            load_1min=load,
            load_5min=load,
            load_15min=load,
            gpu_count=gpu_count,
            gpu_alloc=int(gpu_count * ratio) if gpu_count else 0,
            uptime_days=0,  # Would come from `scontrol show node` BootTime
            reason=reason.strip() if reason.strip() != "none" else "",
        ))
    return nodes


def parse_slurm_time(s: str) -> dt.datetime | None:
    """Slurm timestamps are 'YYYY-MM-DDTHH:MM:SS'. 'N/A' or 'Unknown' → None."""
    if not s or s in ("N/A", "Unknown", "None"):
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def parse_slurm_duration(s: str) -> float:
    """Parse Slurm time-limit strings to hours.

    Slurm formats: 'minutes', 'minutes:seconds', 'hours:minutes:seconds',
    'days-hours', 'days-hours:minutes', 'days-hours:minutes:seconds',
    or 'UNLIMITED'.
    """
    if not s or s in ("UNLIMITED", "INVALID", "NOT_SET"):
        return 0.0
    days = 0
    rest = s
    if "-" in s:
        d_str, rest = s.split("-", 1)
        try:
            days = int(d_str)
        except ValueError:
            days = 0
    parts = rest.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0.0
    if len(nums) == 1:
        h, m, sec = 0, nums[0], 0
    elif len(nums) == 2:
        if days:
            h, m, sec = nums[0], nums[1], 0
        else:
            h, m, sec = 0, nums[0], nums[1]
    elif len(nums) == 3:
        h, m, sec = nums
    else:
        return 0.0
    return days * 24 + h + m / 60 + sec / 3600


def collect_jobs_slurm() -> list[Job]:
    """Run squeue and parse one job per line.

    Format string fields:
        %i job id     %u user        %a account   %P partition
        %j name       %T state       %D nodes     %C cores
        %V submit     %S start       %l time lim  %r reason
    """
    fmt = "%i|%u|%a|%P|%j|%T|%D|%C|%V|%S|%l|%r"
    result = subprocess.run(
        ["squeue", "-h", "-o", fmt],
        capture_output=True, text=True, check=True,
    )
    jobs: list[Job] = []
    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) < 12:
            continue
        try:
            # Strip array suffix (123_4) and het-job suffix (123+0)
            job_id = int(parts[0].split("_")[0].split("+")[0])
        except ValueError:
            continue
        try:
            nodes = int(parts[6])
            cores = int(parts[7])
        except ValueError:
            nodes, cores = 0, 0

        submit = parse_slurm_time(parts[8]) or dt.datetime.now()
        start = parse_slurm_time(parts[9])

        jobs.append(Job(
            job_id=job_id,
            user=parts[1],
            account=parts[2],
            partition=parts[3],
            name=parts[4],
            state=parts[5],
            nodes=nodes,
            cores=cores,
            submit_time=submit,
            start_time=start,
            time_limit_hours=parse_slurm_duration(parts[10]),
            reason=parts[11],
        ))
    return jobs


# ---------------------------------------------------------------------------
# Data collection — simulation
# ---------------------------------------------------------------------------

PARTITIONS = ["compute", "gpu", "bigmem", "debug"]
USERS = ["jchen", "mpatel", "kowalski", "nasser", "rodriguez", "okonkwo",
         "tanaka", "fitzgerald", "ahmadi", "vasquez", "lindberg", "park"]
ACCOUNTS = ["physics", "biochem", "cs-ml", "astro", "climate", "engineering"]
JOB_NAMES = ["lammps_run", "vasp_relax", "gromacs_md", "tf_train",
             "openfoam_sim", "mpi_solve", "namd_protein", "ansys_cfd"]


def collect_compute_nodes_sim(count: int) -> list[ComputeNode]:
    nodes: list[ComputeNode] = []
    for i in range(count):
        partition = random.choices(
            PARTITIONS, weights=[60, 25, 10, 5], k=1
        )[0]
        if partition == "gpu":
            cores_total, mem_total, gpu_count = 64, 512, 4
        elif partition == "bigmem":
            cores_total, mem_total, gpu_count = 96, 1536, 0
        elif partition == "debug":
            cores_total, mem_total, gpu_count = 32, 192, 0
        else:
            cores_total, mem_total, gpu_count = 48, 384, 0

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
                "Not responding", "kernel panic 04:23 UTC",
                "Scheduled maintenance window", "IB port flapping",
                "GPU ECC errors", "DIMM failure slot 3",
            ])
        else:
            if state == "allocated":
                util = random.uniform(0.85, 1.05)
            elif state == "mixed":
                util = random.uniform(0.40, 0.85)
            else:
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


def collect_jobs_sim(node_count: int) -> list[Job]:
    now = dt.datetime.now()
    jobs: list[Job] = []
    job_count = max(20, node_count * 2)
    for i in range(job_count):
        state = random.choices(
            ["RUNNING", "PENDING", "COMPLETING", "FAILED"],
            weights=[55, 38, 5, 2], k=1,
        )[0]
        nodes = random.choices(
            [1, 2, 4, 8, 16, 32], weights=[35, 25, 20, 12, 6, 2], k=1
        )[0]
        cores_per_node = random.choice([16, 24, 32, 48])
        submit_offset_hours = random.uniform(0.1, 36.0)
        submit_time = now - dt.timedelta(hours=submit_offset_hours)

        if state == "RUNNING":
            start_offset = random.uniform(0.0, submit_offset_hours - 0.05)
            start_time = now - dt.timedelta(hours=submit_offset_hours - start_offset)
            reason = "None"
        elif state == "PENDING":
            start_time = None
            reason = random.choices(
                ["Resources", "Priority", "QOSMaxCpuPerUserLimit",
                 "ReqNodeNotAvail", "Dependency"],
                weights=[50, 30, 8, 8, 4], k=1,
            )[0]
        else:
            start_time = now - dt.timedelta(hours=random.uniform(0.5, 12))
            reason = "None"

        jobs.append(Job(
            job_id=1_000_000 + i,
            user=random.choice(USERS),
            account=random.choice(ACCOUNTS),
            partition=random.choices(PARTITIONS, weights=[60, 25, 10, 5], k=1)[0],
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


# TODO: collect_filesystems_real() — parse `lfs df -h` for Lustre,
# `beegfs-df` for BeeGFS, `mmlsfs`/`mmhealth` for GPFS, `df -h` for NFS.
# Each filesystem type needs its own parser. Lustre also requires
# `lctl get_param osc.*.state` to detect failed OSTs.

def collect_filesystems_sim() -> list[Filesystem]:
    return [
        Filesystem(
            name="scratch", mount="/scratch", fs_type="lustre",
            total_tb=2048.0,
            used_tb=round(random.uniform(1400, 1850), 1),
            inodes_used_pct=round(random.uniform(40, 75), 1),
            read_gbps=round(random.uniform(45, 88), 1),
            write_gbps=round(random.uniform(30, 70), 1),
            osts_total=64,
            osts_down=random.choices([0, 0, 0, 1, 2], weights=[60, 20, 10, 8, 2], k=1)[0],
        ),
        Filesystem(
            name="home", mount="/home", fs_type="nfs",
            total_tb=128.0,
            used_tb=round(random.uniform(70, 115), 1),
            inodes_used_pct=round(random.uniform(55, 85), 1),
            read_gbps=round(random.uniform(2, 8), 2),
            write_gbps=round(random.uniform(1, 5), 2),
        ),
        Filesystem(
            name="projects", mount="/projects", fs_type="beegfs",
            total_tb=1024.0,
            used_tb=round(random.uniform(600, 920), 1),
            inodes_used_pct=round(random.uniform(30, 60), 1),
            read_gbps=round(random.uniform(20, 55), 1),
            write_gbps=round(random.uniform(15, 40), 1),
            osts_total=32, osts_down=0,
        ),
        Filesystem(
            name="archive", mount="/archive", fs_type="gpfs",
            total_tb=4096.0,
            used_tb=round(random.uniform(2800, 3500), 1),
            inodes_used_pct=round(random.uniform(20, 40), 1),
            read_gbps=round(random.uniform(8, 20), 1),
            write_gbps=round(random.uniform(4, 12), 1),
        ),
    ]


# TODO: collect_infiniband_real() — parse `ibstat` for per-port state and
# speed, `ibdiagnet --pc` for symbol error counts, then aggregate per switch.

def collect_infiniband_sim() -> list[InfiniBandLink]:
    links: list[InfiniBandLink] = []
    for switch_idx in range(4):
        for port_idx in range(8):
            state = random.choices(
                ["Active", "Active", "Active", "Active", "Polling", "Down"],
                weights=[80, 8, 5, 3, 3, 1], k=1,
            )[0]
            links.append(InfiniBandLink(
                switch=f"ib-sw{switch_idx+1:02d}",
                port=f"{port_idx+1}/1",
                speed_gbps=200,
                state=state,
                error_count=random.choices(
                    [0, 0, 0, random.randint(1, 50), random.randint(100, 800)],
                    weights=[70, 15, 8, 5, 2], k=1,
                )[0],
            ))
    return links


# ---------------------------------------------------------------------------
# Source dispatch
# ---------------------------------------------------------------------------

def collect_report(cfg: Config, source: str) -> ClusterReport:
    """Build a ClusterReport using the requested source."""
    resolved = source
    if source == "auto":
        resolved = "slurm" if slurm_available() else "sim"

    if resolved == "slurm":
        if not slurm_available():
            print("error: --source slurm requested but sinfo/squeue not on PATH",
                  file=sys.stderr)
            sys.exit(2)
        nodes = collect_compute_nodes_slurm()
        jobs = collect_jobs_slurm()
    elif resolved == "sim":
        nodes = collect_compute_nodes_sim(cfg.sim_nodes)
        jobs = collect_jobs_sim(cfg.sim_nodes)
    else:
        print(f"error: unknown source '{source}'", file=sys.stderr)
        sys.exit(2)

    if cfg.partitions_include:
        keep = set(cfg.partitions_include)
        nodes = [n for n in nodes if n.partition in keep]
        jobs = [j for j in jobs if j.partition in keep]

    return ClusterReport(
        generated_at=dt.datetime.now(),
        cluster_name=cfg.cluster_name,
        source=resolved,
        nodes=nodes,
        jobs=jobs,
        filesystems=collect_filesystems_sim(),
        ib_links=collect_infiniband_sim(),
    )


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def evaluate_alerts(report: ClusterReport, t: Thresholds) -> list[Alert]:
    alerts: list[Alert] = []

    for node in report.nodes:
        if node.state == "down":
            alerts.append(Alert(
                "critical", f"node/{node.name}",
                f"Node DOWN — {node.reason or 'no reason reported'}"))
        elif node.state == "drain":
            alerts.append(Alert(
                "warning", f"node/{node.name}",
                f"Node draining — {node.reason or 'no reason reported'}"))
        elif node.state in ("allocated", "mixed", "idle"):
            if node.load_ratio >= t.load_critical:
                alerts.append(Alert(
                    "critical", f"node/{node.name}",
                    f"Load {node.load_5min:.1f} on {node.cores_total} cores "
                    f"({node.load_ratio:.0%} of capacity)"))
            elif node.load_ratio >= t.load_warning:
                alerts.append(Alert(
                    "warning", f"node/{node.name}",
                    f"High load: {node.load_5min:.1f} "
                    f"({node.load_ratio:.0%} of capacity)"))
            if node.mem_ratio >= t.memory_critical:
                alerts.append(Alert(
                    "critical", f"node/{node.name}",
                    f"Memory pressure {node.mem_ratio:.0%} "
                    f"({node.mem_used_gb:.0f}/{node.mem_total_gb} GB)"))
            elif node.mem_ratio >= t.memory_warning:
                alerts.append(Alert(
                    "warning", f"node/{node.name}",
                    f"Memory at {node.mem_ratio:.0%}"))

    for fs in report.filesystems:
        if fs.used_ratio >= t.storage_critical:
            alerts.append(Alert(
                "critical", f"fs/{fs.name}",
                f"{fs.fs_type.upper()} {fs.mount} at {fs.used_ratio:.0%} "
                f"({fs.used_tb:.0f}/{fs.total_tb:.0f} TB)"))
        elif fs.used_ratio >= t.storage_warning:
            alerts.append(Alert(
                "warning", f"fs/{fs.name}",
                f"{fs.fs_type.upper()} {fs.mount} at {fs.used_ratio:.0%}"))
        if fs.osts_down > 0:
            alerts.append(Alert(
                "critical", f"fs/{fs.name}",
                f"{fs.osts_down} of {fs.osts_total} OSTs offline"))

    pending = [j for j in report.jobs if j.state == "PENDING"]
    long_waits = [j for j in pending if j.wait_hours >= t.queue_wait_critical_hours]
    medium_waits = [j for j in pending
                    if t.queue_wait_warning_hours <= j.wait_hours < t.queue_wait_critical_hours]
    if long_waits:
        alerts.append(Alert(
            "warning", "scheduler",
            f"{len(long_waits)} job(s) pending over {t.queue_wait_critical_hours:.0f}h"))
    if medium_waits:
        alerts.append(Alert(
            "info", "scheduler",
            f"{len(medium_waits)} job(s) pending over {t.queue_wait_warning_hours:.0f}h"))

    failed = [j for j in report.jobs if j.state == "FAILED"]
    if failed:
        alerts.append(Alert(
            "warning", "scheduler", f"{len(failed)} job(s) in FAILED state"))

    down_links = [l for l in report.ib_links if l.state == "Down"]
    error_links = [l for l in report.ib_links if l.error_count > 100]
    if down_links:
        alerts.append(Alert(
            "critical", "infiniband", f"{len(down_links)} IB link(s) DOWN"))
    if error_links:
        alerts.append(Alert(
            "warning", "infiniband",
            f"{len(error_links)} IB link(s) with elevated error counts"))

    severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: severity_order.get(a.severity, 99))
    return alerts


def build_snapshot(report: ClusterReport) -> HistorySnapshot:
    return HistorySnapshot(
        timestamp=report.generated_at,
        cores_total=sum(n.cores_total for n in report.nodes),
        cores_alloc=sum(n.cores_alloc for n in report.nodes),
        nodes_healthy=sum(1 for n in report.nodes
                          if n.state in ("idle", "allocated", "mixed")),
        nodes_down=sum(1 for n in report.nodes if n.state == "down"),
        nodes_drain=sum(1 for n in report.nodes if n.state == "drain"),
        jobs_running=sum(1 for j in report.jobs if j.state == "RUNNING"),
        jobs_pending=sum(1 for j in report.jobs if j.state == "PENDING"),
        storage_used_tb=sum(f.used_tb for f in report.filesystems),
        storage_total_tb=sum(f.total_tb for f in report.filesystems),
        alerts_critical=sum(1 for a in report.alerts if a.severity == "critical"),
        alerts_warning=sum(1 for a in report.alerts if a.severity == "warning"),
    )


# ---------------------------------------------------------------------------
# History (SQLite)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster         TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    cores_total     INTEGER NOT NULL,
    cores_alloc     INTEGER NOT NULL,
    nodes_healthy   INTEGER NOT NULL,
    nodes_down      INTEGER NOT NULL,
    nodes_drain     INTEGER NOT NULL,
    jobs_running    INTEGER NOT NULL,
    jobs_pending    INTEGER NOT NULL,
    storage_used_tb REAL NOT NULL,
    storage_total_tb REAL NOT NULL,
    alerts_critical INTEGER NOT NULL,
    alerts_warning  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_cluster_ts
    ON snapshots(cluster, timestamp);
"""


def open_history(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def write_snapshot(conn: sqlite3.Connection, cluster: str, snap: HistorySnapshot) -> None:
    conn.execute(
        """INSERT INTO snapshots
           (cluster, timestamp, cores_total, cores_alloc, nodes_healthy,
            nodes_down, nodes_drain, jobs_running, jobs_pending,
            storage_used_tb, storage_total_tb, alerts_critical, alerts_warning)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cluster, snap.timestamp.isoformat(),
         snap.cores_total, snap.cores_alloc, snap.nodes_healthy,
         snap.nodes_down, snap.nodes_drain, snap.jobs_running, snap.jobs_pending,
         snap.storage_used_tb, snap.storage_total_tb,
         snap.alerts_critical, snap.alerts_warning),
    )
    conn.commit()


def prune_history(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = (dt.datetime.now() - dt.timedelta(days=retention_days)).isoformat()
    cur = conn.execute("DELETE FROM snapshots WHERE timestamp < ?", (cutoff,))
    conn.commit()
    return cur.rowcount


def read_history(conn: sqlite3.Connection, cluster: str, days: int) -> list[HistorySnapshot]:
    cutoff = (dt.datetime.now() - dt.timedelta(days=days)).isoformat()
    cur = conn.execute(
        """SELECT timestamp, cores_total, cores_alloc, nodes_healthy,
                  nodes_down, nodes_drain, jobs_running, jobs_pending,
                  storage_used_tb, storage_total_tb, alerts_critical, alerts_warning
           FROM snapshots
           WHERE cluster = ? AND timestamp >= ?
           ORDER BY timestamp ASC""",
        (cluster, cutoff),
    )
    out: list[HistorySnapshot] = []
    for row in cur.fetchall():
        out.append(HistorySnapshot(
            timestamp=dt.datetime.fromisoformat(row[0]),
            cores_total=row[1], cores_alloc=row[2], nodes_healthy=row[3],
            nodes_down=row[4], nodes_drain=row[5],
            jobs_running=row[6], jobs_pending=row[7],
            storage_used_tb=row[8], storage_total_tb=row[9],
            alerts_critical=row[10], alerts_warning=row[11],
        ))
    return out


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

CSS = """
:root {
    --bg: #0d1117; --bg-elev: #161b22; --bg-row: #1c2128;
    --border: #30363d; --text: #c9d1d9; --text-dim: #8b949e;
    --accent: #58a6ff; --ok: #3fb950; --warn: #d29922;
    --crit: #f85149; --info: #58a6ff;
}
* { box-sizing: border-box; }
body {
    margin: 0;
    font-family: -apple-system, "SF Pro Text", "Segoe UI", system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    font-size: 14px; line-height: 1.5;
}
header {
    background: var(--bg-elev); border-bottom: 1px solid var(--border);
    padding: 20px 32px; display: flex;
    justify-content: space-between; align-items: baseline;
}
header h1 { margin: 0; font-size: 22px; font-weight: 600; }
header .meta { color: var(--text-dim); font-size: 13px; }
header .source-tag {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em; margin-left: 8px;
    background: rgba(88, 166, 255, 0.15); color: var(--accent);
}
main { padding: 24px 32px; max-width: 1600px; margin: 0 auto; }
section { margin-bottom: 32px; }
section h2 {
    font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em;
    color: var(--text-dim); border-bottom: 1px solid var(--border);
    padding-bottom: 8px; margin: 0 0 16px; font-weight: 600;
}
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
}
.kpi {
    background: var(--bg-elev); border: 1px solid var(--border);
    border-radius: 6px; padding: 16px;
}
.kpi .label {
    color: var(--text-dim); font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.05em;
}
.kpi .value {
    font-size: 28px; font-weight: 600; margin-top: 6px;
    font-variant-numeric: tabular-nums;
}
.kpi .sub { color: var(--text-dim); font-size: 12px; margin-top: 4px; }
table {
    width: 100%; border-collapse: collapse;
    background: var(--bg-elev); border: 1px solid var(--border);
    border-radius: 6px; overflow: hidden;
    font-variant-numeric: tabular-nums;
}
th, td {
    text-align: left; padding: 8px 12px;
    border-bottom: 1px solid var(--border); font-size: 13px;
}
th {
    background: var(--bg-row); color: var(--text-dim);
    font-weight: 600; text-transform: uppercase;
    font-size: 11px; letter-spacing: 0.05em;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg-row); }
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em;
}
.badge-ok { background: rgba(63, 185, 80, 0.15); color: var(--ok); }
.badge-warn { background: rgba(210, 153, 34, 0.15); color: var(--warn); }
.badge-crit { background: rgba(248, 81, 73, 0.15); color: var(--crit); }
.badge-info { background: rgba(88, 166, 255, 0.15); color: var(--info); }
.badge-dim { background: rgba(139, 148, 158, 0.15); color: var(--text-dim); }
.bar {
    background: var(--bg-row); border-radius: 3px;
    height: 6px; overflow: hidden; margin-top: 4px;
}
.bar-fill { height: 100%; transition: width 0.3s; }
.bar-ok { background: var(--ok); }
.bar-warn { background: var(--warn); }
.bar-crit { background: var(--crit); }
.alert {
    background: var(--bg-elev); border: 1px solid var(--border);
    border-left: 3px solid var(--text-dim); border-radius: 4px;
    padding: 10px 14px; margin-bottom: 8px;
    display: flex; gap: 12px; align-items: baseline;
}
.alert-critical { border-left-color: var(--crit); }
.alert-warning { border-left-color: var(--warn); }
.alert-info { border-left-color: var(--info); }
.alert .component {
    color: var(--text-dim);
    font-family: "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px; min-width: 180px;
}
.two-col {
    display: grid; grid-template-columns: 2fr 1fr; gap: 24px;
}
@media (max-width: 1100px) { .two-col { grid-template-columns: 1fr; } }
.trend-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 12px;
}
.trend-card {
    background: var(--bg-elev); border: 1px solid var(--border);
    border-radius: 6px; padding: 16px;
}
.trend-card .label {
    color: var(--text-dim); font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.05em;
}
.trend-card .value {
    font-size: 22px; font-weight: 600; margin-top: 4px;
    font-variant-numeric: tabular-nums;
}
.trend-card .delta { font-size: 12px; margin-top: 2px; }
.delta-up { color: var(--crit); }
.delta-down { color: var(--ok); }
.delta-flat { color: var(--text-dim); }
.trend-card svg { display: block; margin-top: 10px; width: 100%; height: 50px; }
footer {
    text-align: center; color: var(--text-dim); font-size: 12px;
    padding: 24px; border-top: 1px solid var(--border); margin-top: 32px;
}
.mono { font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 12px; }
.empty {
    color: var(--text-dim); font-style: italic;
    padding: 16px; text-align: center;
    background: var(--bg-elev); border: 1px dashed var(--border);
    border-radius: 6px;
}
"""


def severity_class(sev: str) -> str:
    return {"critical": "crit", "warning": "warn", "info": "info"}.get(sev, "dim")


def state_badge(state: str) -> str:
    cls = {
        "idle": "ok", "allocated": "info", "mixed": "info",
        "drain": "warn", "down": "crit", "maint": "dim",
        "RUNNING": "ok", "PENDING": "warn",
        "COMPLETING": "info", "FAILED": "crit",
        "Active": "ok", "Polling": "warn", "Down": "crit",
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
    return (f'<div class="bar"><div class="bar-fill bar-{cls}" '
            f'style="width: {pct:.1f}%"></div></div>')


def sparkline(values: list[float], width: int = 240, height: int = 50,
              color: str = "var(--accent)") -> str:
    """Render an inline SVG sparkline. Empty/single-value lists return empty."""
    if len(values) < 2:
        return ""
    lo = min(values)
    hi = max(values)
    span = hi - lo if hi > lo else 1.0
    pad = 4
    inner_w = width - 2 * pad
    inner_h = height - 2 * pad
    points = []
    for i, v in enumerate(values):
        x = pad + (i / (len(values) - 1)) * inner_w
        y = pad + (1 - (v - lo) / span) * inner_h
        points.append(f"{x:.1f},{y:.1f}")
    poly = " ".join(points)
    last_x, last_y = points[-1].split(",")
    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">'
        f'<polyline fill="none" stroke="{color}" stroke-width="1.5" '
        f'points="{poly}"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="2.5" fill="{color}"/>'
        f'</svg>'
    )


def format_delta(current: float, previous: float, unit: str = "",
                 neutral: bool = False) -> str:
    """Format a delta with directional indicator.

    By default, increases render red (delta-up) and decreases render green
    (delta-down) — appropriate for metrics where 'up is bad' (utilization,
    pending queue, storage used, nodes down). Set neutral=True for metrics
    where direction has no inherent good/bad meaning (jobs running).
    """
    if previous == 0:
        if current == 0:
            return '<span class="delta delta-flat">flat</span>'
        return '<span class="delta delta-flat">no prior data</span>'
    diff = current - previous
    pct = (diff / previous) * 100 if previous else 0
    if abs(pct) < 0.5:
        return '<span class="delta delta-flat">flat</span>'
    arrow = "up" if diff > 0 else "down"
    if neutral:
        cls = "delta-flat"
    else:
        cls = "delta-up" if diff > 0 else "delta-down"
    return (f'<span class="delta {cls}">{arrow} {abs(pct):.1f}% '
            f'({diff:+.1f}{unit})</span>')


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
        ("Healthy nodes", f"{healthy}", f"{down} down, {drain} draining"),
        ("Jobs running", f"{running}", f"{pending} pending in queue"),
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


def render_trends(history: list[HistorySnapshot], display_days: int) -> str:
    if len(history) < 2:
        return (f'<div class="empty">Not enough history yet — run the script '
                f'a few more times to populate trends. '
                f'(Currently {len(history)} snapshot(s) recorded.)</div>')

    util_series = [
        (s.cores_alloc / s.cores_total * 100) if s.cores_total else 0
        for s in history
    ]
    pending_series = [float(s.jobs_pending) for s in history]
    running_series = [float(s.jobs_running) for s in history]
    storage_series = [s.storage_used_tb for s in history]
    down_series = [float(s.nodes_down) for s in history]

    first = history[0]
    last = history[-1]
    first_util = (first.cores_alloc / first.cores_total * 100) if first.cores_total else 0
    last_util = (last.cores_alloc / last.cores_total * 100) if last.cores_total else 0

    cards = [
        ("Utilization", f"{last_util:.1f}%",
         format_delta(last_util, first_util, "%"),
         sparkline(util_series, color="#58a6ff")),
        ("Jobs running", f"{last.jobs_running}",
         format_delta(last.jobs_running, first.jobs_running, neutral=True),
         sparkline(running_series, color="#3fb950")),
        ("Pending queue depth", f"{last.jobs_pending}",
         format_delta(last.jobs_pending, first.jobs_pending),
         sparkline(pending_series, color="#d29922")),
        ("Storage used", f"{last.storage_used_tb/1024:.2f} PB",
         format_delta(last.storage_used_tb, first.storage_used_tb, " TB", neutral=True),
         sparkline(storage_series, color="#58a6ff")),
        ("Nodes down", f"{last.nodes_down}",
         format_delta(last.nodes_down, first.nodes_down),
         sparkline(down_series, color="#f85149")),
    ]
    body = "".join(
        f'<div class="trend-card">'
        f'<div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div>'
        f'<div>{delta}</div>'
        f'{spark}'
        f'</div>'
        for label, value, delta, spark in cards
    )
    span_hours = (last.timestamp - first.timestamp).total_seconds() / 3600
    note = (f'<div style="color:var(--text-dim);font-size:12px;margin-bottom:12px">'
            f'{len(history)} snapshots over {span_hours:.1f} hours '
            f'(window: last {display_days} days)</div>')
    return note + f'<div class="trend-grid">{body}</div>'


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
            f'<div class="alert"><span>... and {len(alerts) - 25} more</span></div>')
    return "".join(rows)


def render_node_table(nodes: list[ComputeNode]) -> str:
    problem = [n for n in nodes if n.state in ("down", "drain", "maint")
               or n.load_ratio >= 0.85 or n.mem_ratio >= 0.85]
    healthy = [n for n in nodes if n not in problem][:15]
    display = problem + healthy

    rows = []
    for n in display:
        core_ratio = n.cores_alloc / n.cores_total if n.cores_total else 0
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(n.name)}</td>"
            f"<td>{html.escape(n.partition)}</td>"
            f"<td>{state_badge(n.state)}</td>"
            f"<td>{n.cores_alloc}/{n.cores_total}{utilization_bar(core_ratio)}</td>"
            f"<td>{n.mem_used_gb:.0f}/{n.mem_total_gb} GB"
            f"{utilization_bar(n.mem_ratio)}</td>"
            f"<td>{n.load_5min:.2f}</td>"
            f"<td>{n.gpu_alloc}/{n.gpu_count}</td>"
            f"<td>{n.uptime_days}d</td>"
            f"<td class='mono' style='color:var(--text-dim)'>{html.escape(n.reason)}</td>"
            "</tr>"
        )
    return ("<table><thead><tr>"
            "<th>Node</th><th>Partition</th><th>State</th>"
            "<th>Cores</th><th>Memory</th><th>Load</th>"
            "<th>GPU</th><th>Uptime</th><th>Reason</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


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
    return ("<table><thead><tr>"
            "<th>Mount</th><th>Type</th><th>Capacity</th>"
            "<th>Inodes</th><th>Read</th><th>Write</th><th>OSTs</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def render_queue_summary(jobs: list[Job]) -> str:
    by_partition: dict[str, dict[str, int]] = {}
    for j in jobs:
        by_partition.setdefault(j.partition, {"running": 0, "pending": 0})
        if j.state == "RUNNING":
            by_partition[j.partition]["running"] += 1
        elif j.state == "PENDING":
            by_partition[j.partition]["pending"] += 1
    rows = []
    for part in sorted(by_partition):
        d = by_partition[part]
        rows.append(f"<tr><td>{html.escape(part)}</td>"
                    f"<td>{d['running']}</td><td>{d['pending']}</td></tr>")
    return ("<table><thead><tr>"
            "<th>Partition</th><th>Running</th><th>Pending</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def render_top_jobs(jobs: list[Job]) -> str:
    running = sorted((j for j in jobs if j.state == "RUNNING"),
                     key=lambda j: j.cores, reverse=True)[:10]
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
    return ("<table><thead><tr>"
            "<th>Job ID</th><th>User</th><th>Name</th>"
            "<th>Nodes</th><th>Cores</th><th>Runtime</th><th>Limit</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def render_infiniband_summary(links: list[InfiniBandLink]) -> str:
    by_switch: dict[str, dict[str, int]] = {}
    for l in links:
        d = by_switch.setdefault(l.switch,
                                  {"active": 0, "down": 0, "polling": 0, "errors": 0})
        if l.state == "Active":
            d["active"] += 1
        elif l.state == "Down":
            d["down"] += 1
        else:
            d["polling"] += 1
        if l.error_count > 100:
            d["errors"] += 1
    rows = []
    for sw in sorted(by_switch):
        d = by_switch[sw]
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(sw)}</td>"
            f"<td>{d['active']}</td><td>{d['down']}</td>"
            f"<td>{d['polling']}</td><td>{d['errors']}</td>"
            "</tr>"
        )
    return ("<table><thead><tr>"
            "<th>Switch</th><th>Active</th><th>Down</th>"
            "<th>Polling</th><th>High Errors</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def render_html(report: ClusterReport, display_days: int) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{html.escape(report.cluster_name)} — Cluster Health</title>
<style>{CSS}</style>
</head>
<body>
<header>
    <h1>{html.escape(report.cluster_name)}
        <span style="color:var(--text-dim);font-weight:400">cluster health</span>
        <span class="source-tag">source: {html.escape(report.source)}</span>
    </h1>
    <div class="meta">Generated {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}</div>
</header>
<main>
    <section>
        <h2>Overview</h2>
        {render_kpis(report)}
    </section>

    <section>
        <h2>Trends (last {display_days} days)</h2>
        {render_trends(report.history, display_days)}
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
    Generated by hpc_cluster_health.py — source: {html.escape(report.source)}.
    History persisted to SQLite for trend analysis. In production, regenerate
    every 60 seconds via cron or systemd timer and serve from a shared web root.
</footer>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
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

    report = collect_report(cfg, cfg.source)
    report.alerts = evaluate_alerts(report, cfg.thresholds)

    print(f"  - {len(report.nodes)} nodes")
    print(f"  - {len(report.jobs)} jobs "
          f"({sum(1 for j in report.jobs if j.state == 'RUNNING')} running)")
    print(f"  - {len(report.filesystems)} filesystems")
    print(f"  - {len(report.ib_links)} IB links")
    print(f"  - {len(report.alerts)} alerts")

    if not args.no_history:
        try:
            conn = open_history(cfg.history_db)
            snap = build_snapshot(report)
            write_snapshot(conn, cfg.cluster_name, snap)
            pruned = prune_history(conn, cfg.history_retention_days)
            report.history = read_history(
                conn, cfg.cluster_name, cfg.history_display_days)
            conn.close()
            print(f"  - history: {len(report.history)} snapshot(s) in window"
                  + (f", pruned {pruned} old row(s)" if pruned else ""))
        except sqlite3.Error as e:
            print(f"warning: history database error: {e}", file=sys.stderr)

    args.output.write_text(render_html(report, cfg.history_display_days),
                           encoding="utf-8")
    print(f"\nReport written to {args.output.resolve()}")

    if args.open:
        webbrowser.open(args.output.resolve().as_uri())

    return 0


if __name__ == "__main__":
    sys.exit(main())
