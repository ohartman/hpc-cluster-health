"""HTML report rendering — dark-themed dashboard."""

from __future__ import annotations

import html

from .models import (
    Alert,
    ClusterReport,
    ComputeNode,
    Filesystem,
    HistorySnapshot,
    InfiniBandLink,
    Job,
)


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
    Generated by hpc_monitor — source: {html.escape(report.source)}.
    History persisted to SQLite for trend analysis. In production, regenerate
    every 60 seconds via cron, systemd timer, or the bundled Flask service.
</footer>
</body>
</html>
"""
