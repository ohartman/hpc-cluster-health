"""Data models for cluster state, jobs, filesystems, fabric, and history."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field


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
