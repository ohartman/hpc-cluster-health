"""Threshold-based alert evaluation."""

from __future__ import annotations

from .config import Thresholds
from .models import Alert, ClusterReport, HistorySnapshot


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
