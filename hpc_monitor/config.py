"""Configuration loading from TOML."""

from __future__ import annotations

import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


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
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    refresh_seconds: int = 60
    # Max seconds since last successful collection before /healthz returns 503.
    healthz_staleness_limit: int = 300


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
    server: ServerConfig = field(default_factory=ServerConfig)


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

    server = data.get("server", {})
    cfg.server = ServerConfig(
        host=server.get("host", ServerConfig.host),
        port=server.get("port", ServerConfig.port),
        refresh_seconds=server.get("refresh_seconds", ServerConfig.refresh_seconds),
        healthz_staleness_limit=server.get(
            "healthz_staleness_limit", ServerConfig.healthz_staleness_limit),
    )

    return cfg
