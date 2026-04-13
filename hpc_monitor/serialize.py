"""JSON serialization for the Flask API endpoints.

The dataclasses in models.py use datetime fields which json.dumps can't
handle natively, so we have explicit converters here. Keeps render.py
focused on HTML and keeps the dataclasses themselves framework-agnostic.
"""

from __future__ import annotations

from typing import Any

from .models import (
    Alert,
    ClusterReport,
    ComputeNode,
    Filesystem,
    HistorySnapshot,
    InfiniBandLink,
    Job,
)


def node_to_dict(n: ComputeNode) -> dict[str, Any]:
    return {
        "name": n.name,
        "partition": n.partition,
        "state": n.state,
        "cores_total": n.cores_total,
        "cores_alloc": n.cores_alloc,
        "cores_ratio": round(
            n.cores_alloc / n.cores_total if n.cores_total else 0, 4),
        "mem_total_gb": n.mem_total_gb,
        "mem_used_gb": n.mem_used_gb,
        "mem_ratio": round(n.mem_ratio, 4),
        "load_1min": n.load_1min,
        "load_5min": n.load_5min,
        "load_15min": n.load_15min,
        "load_ratio": round(n.load_ratio, 4),
        "gpu_count": n.gpu_count,
        "gpu_alloc": n.gpu_alloc,
        "uptime_days": n.uptime_days,
        "reason": n.reason,
    }


def job_to_dict(j: Job) -> dict[str, Any]:
    return {
        "job_id": j.job_id,
        "user": j.user,
        "account": j.account,
        "partition": j.partition,
        "name": j.name,
        "state": j.state,
        "nodes": j.nodes,
        "cores": j.cores,
        "submit_time": j.submit_time.isoformat(),
        "start_time": j.start_time.isoformat() if j.start_time else None,
        "time_limit_hours": j.time_limit_hours,
        "wait_hours": round(j.wait_hours, 2),
        "runtime_hours": round(j.runtime_hours, 2),
        "reason": j.reason,
    }


def filesystem_to_dict(f: Filesystem) -> dict[str, Any]:
    return {
        "name": f.name,
        "mount": f.mount,
        "fs_type": f.fs_type,
        "total_tb": f.total_tb,
        "used_tb": f.used_tb,
        "used_ratio": round(f.used_ratio, 4),
        "inodes_used_pct": f.inodes_used_pct,
        "read_gbps": f.read_gbps,
        "write_gbps": f.write_gbps,
        "osts_total": f.osts_total,
        "osts_down": f.osts_down,
    }


def ib_link_to_dict(l: InfiniBandLink) -> dict[str, Any]:
    return {
        "switch": l.switch,
        "port": l.port,
        "speed_gbps": l.speed_gbps,
        "state": l.state,
        "error_count": l.error_count,
    }


def alert_to_dict(a: Alert) -> dict[str, Any]:
    return {
        "severity": a.severity,
        "component": a.component,
        "message": a.message,
    }


def history_snapshot_to_dict(s: HistorySnapshot) -> dict[str, Any]:
    return {
        "timestamp": s.timestamp.isoformat(),
        "cores_total": s.cores_total,
        "cores_alloc": s.cores_alloc,
        "cores_ratio": round(
            s.cores_alloc / s.cores_total if s.cores_total else 0, 4),
        "nodes_healthy": s.nodes_healthy,
        "nodes_down": s.nodes_down,
        "nodes_drain": s.nodes_drain,
        "jobs_running": s.jobs_running,
        "jobs_pending": s.jobs_pending,
        "storage_used_tb": s.storage_used_tb,
        "storage_total_tb": s.storage_total_tb,
        "alerts_critical": s.alerts_critical,
        "alerts_warning": s.alerts_warning,
    }


def report_to_dict(r: ClusterReport) -> dict[str, Any]:
    """Full report as a single dict — used by /api/health."""
    total_cores = sum(n.cores_total for n in r.nodes)
    alloc_cores = sum(n.cores_alloc for n in r.nodes)
    return {
        "generated_at": r.generated_at.isoformat(),
        "cluster_name": r.cluster_name,
        "source": r.source,
        "summary": {
            "cores_total": total_cores,
            "cores_alloc": alloc_cores,
            "utilization": round(
                alloc_cores / total_cores if total_cores else 0, 4),
            "nodes_total": len(r.nodes),
            "nodes_healthy": sum(
                1 for n in r.nodes
                if n.state in ("idle", "allocated", "mixed")),
            "nodes_down": sum(1 for n in r.nodes if n.state == "down"),
            "nodes_drain": sum(1 for n in r.nodes if n.state == "drain"),
            "jobs_running": sum(1 for j in r.jobs if j.state == "RUNNING"),
            "jobs_pending": sum(1 for j in r.jobs if j.state == "PENDING"),
            "storage_used_tb": sum(f.used_tb for f in r.filesystems),
            "storage_total_tb": sum(f.total_tb for f in r.filesystems),
            "alerts_total": len(r.alerts),
            "alerts_critical": sum(
                1 for a in r.alerts if a.severity == "critical"),
            "alerts_warning": sum(
                1 for a in r.alerts if a.severity == "warning"),
        },
        "alerts": [alert_to_dict(a) for a in r.alerts],
        "filesystems": [filesystem_to_dict(f) for f in r.filesystems],
        "ib_links": [ib_link_to_dict(l) for l in r.ib_links],
        # Nodes and jobs are big — omitted from the /health summary.
        # Use /api/nodes and /api/jobs for the full lists.
    }
