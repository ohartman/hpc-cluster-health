"""Flask service — runs a background collector thread on a refresh
interval and serves the cached report via HTTP endpoints.

Run with: python3 -m hpc_monitor.server

The collector and the request handlers are intentionally decoupled:
the background thread re-runs the full collect/evaluate/persist pipeline
every refresh_seconds, writes the result into a thread-safe cache, and
request handlers just read from the cache. This means HTTP requests
return instantly even when collection is slow, and it prevents parallel
curl requests from triggering multiple concurrent sinfo calls against
the controller.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

from .config import Config, load_config
from .models import ClusterReport
from .pipeline import build_report
from .render import render_html
from .serialize import (
    alert_to_dict,
    filesystem_to_dict,
    history_snapshot_to_dict,
    ib_link_to_dict,
    job_to_dict,
    node_to_dict,
    report_to_dict,
)


log = logging.getLogger("hpc_monitor.server")


# ---------------------------------------------------------------------------
# Thread-safe report cache
# ---------------------------------------------------------------------------

class ReportCache:
    """Holds the most recent ClusterReport along with collection metadata.

    Reads are lock-protected but very fast — they just return the current
    reference. Writes replace the reference atomically under the same lock.
    A `threading.Event` lets the background thread wake up early if the
    process is shutting down.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._report: ClusterReport | None = None
        self._last_success: dt.datetime | None = None
        self._last_error: str | None = None
        self._collection_count: int = 0
        self.shutdown = threading.Event()

    def set_report(self, report: ClusterReport) -> None:
        with self._lock:
            self._report = report
            self._last_success = dt.datetime.now()
            self._last_error = None
            self._collection_count += 1

    def set_error(self, error: str) -> None:
        with self._lock:
            self._last_error = error

    def snapshot(self) -> tuple[ClusterReport | None, dt.datetime | None, str | None, int]:
        """Return (report, last_success, last_error, collection_count)."""
        with self._lock:
            return (self._report, self._last_success,
                    self._last_error, self._collection_count)


# ---------------------------------------------------------------------------
# Background collector
# ---------------------------------------------------------------------------

def collector_loop(cfg: Config, cache: ReportCache) -> None:
    """Run build_report() every refresh_seconds until shutdown is signaled.

    Failures are logged but don't crash the thread — the cache retains
    whatever report it had before the failure, and /healthz will start
    returning 503 once staleness exceeds the configured limit.
    """
    log.info("collector thread started (refresh=%ds)", cfg.server.refresh_seconds)
    while not cache.shutdown.is_set():
        start = time.monotonic()
        try:
            report = build_report(cfg, write_history=True)
            cache.set_report(report)
            elapsed = time.monotonic() - start
            log.info("collection complete: %d nodes, %d jobs, %d alerts (%.2fs)",
                     len(report.nodes), len(report.jobs),
                     len(report.alerts), elapsed)
        except Exception as e:
            log.exception("collection failed: %s", e)
            cache.set_error(str(e))

        # Sleep until next refresh, but wake up immediately on shutdown
        cache.shutdown.wait(cfg.server.refresh_seconds)

    log.info("collector thread exiting")


# ---------------------------------------------------------------------------
# Flask app factory
# ---------------------------------------------------------------------------

def create_app(cfg: Config, cache: ReportCache) -> Flask:
    app = Flask(__name__)

    # Suppress Flask's default access log noise — we have our own logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    def _require_report() -> ClusterReport:
        """Return the cached report or abort with 503 if none yet."""
        report, _, _, _ = cache.snapshot()
        if report is None:
            from flask import abort
            abort(503, description="Report not yet collected — wait a moment and retry.")
        return report

    # -----------------------------------------------------------------------
    # HTML dashboard
    # -----------------------------------------------------------------------
    @app.get("/")
    def index() -> Response:
        report = _require_report()
        return Response(
            render_html(report, cfg.history_display_days),
            mimetype="text/html",
        )

    # -----------------------------------------------------------------------
    # Liveness probe
    # -----------------------------------------------------------------------
    @app.get("/healthz")
    def healthz() -> tuple[dict, int]:
        report, last_success, last_error, count = cache.snapshot()
        now = dt.datetime.now()
        staleness_seconds = (
            (now - last_success).total_seconds() if last_success else None
        )
        limit = cfg.server.healthz_staleness_limit

        status_ok = (
            last_success is not None
            and staleness_seconds is not None
            and staleness_seconds < limit
        )

        body = {
            "status": "ok" if status_ok else "stale",
            "last_collection": last_success.isoformat() if last_success else None,
            "last_error": last_error,
            "staleness_seconds": (
                round(staleness_seconds, 1) if staleness_seconds else None),
            "staleness_limit_seconds": limit,
            "collection_count": count,
            "cluster": cfg.cluster_name,
        }
        return body, (200 if status_ok else 503)

    # -----------------------------------------------------------------------
    # Aggregate report
    # -----------------------------------------------------------------------
    @app.get("/api/health")
    def api_health() -> Any:
        report = _require_report()
        return jsonify(report_to_dict(report))

    # -----------------------------------------------------------------------
    # Nodes
    # -----------------------------------------------------------------------
    @app.get("/api/nodes")
    def api_nodes() -> Any:
        report = _require_report()
        # Optional filtering by state or partition via query params
        state_filter = request.args.get("state")
        partition_filter = request.args.get("partition")
        nodes = report.nodes
        if state_filter:
            nodes = [n for n in nodes if n.state == state_filter]
        if partition_filter:
            nodes = [n for n in nodes if n.partition == partition_filter]
        return jsonify({
            "count": len(nodes),
            "nodes": [node_to_dict(n) for n in nodes],
        })

    @app.get("/api/nodes/<name>")
    def api_node_detail(name: str) -> Any:
        report = _require_report()
        node = next((n for n in report.nodes if n.name == name), None)
        if node is None:
            return jsonify({"error": f"node '{name}' not found"}), 404
        return jsonify(node_to_dict(node))

    # -----------------------------------------------------------------------
    # Jobs
    # -----------------------------------------------------------------------
    @app.get("/api/jobs")
    def api_jobs() -> Any:
        report = _require_report()
        state_filter = request.args.get("state")
        partition_filter = request.args.get("partition")
        user_filter = request.args.get("user")
        jobs = report.jobs
        if state_filter:
            jobs = [j for j in jobs if j.state == state_filter]
        if partition_filter:
            jobs = [j for j in jobs if j.partition == partition_filter]
        if user_filter:
            jobs = [j for j in jobs if j.user == user_filter]
        return jsonify({
            "count": len(jobs),
            "jobs": [job_to_dict(j) for j in jobs],
        })

    # -----------------------------------------------------------------------
    # Alerts, filesystems, infiniband, history
    # -----------------------------------------------------------------------
    @app.get("/api/alerts")
    def api_alerts() -> Any:
        report = _require_report()
        severity_filter = request.args.get("severity")
        alerts = report.alerts
        if severity_filter:
            alerts = [a for a in alerts if a.severity == severity_filter]
        return jsonify({
            "count": len(alerts),
            "alerts": [alert_to_dict(a) for a in alerts],
        })

    @app.get("/api/filesystems")
    def api_filesystems() -> Any:
        report = _require_report()
        return jsonify({
            "count": len(report.filesystems),
            "filesystems": [filesystem_to_dict(f) for f in report.filesystems],
        })

    @app.get("/api/infiniband")
    def api_infiniband() -> Any:
        report = _require_report()
        return jsonify({
            "count": len(report.ib_links),
            "links": [ib_link_to_dict(l) for l in report.ib_links],
        })

    @app.get("/api/history")
    def api_history() -> Any:
        report = _require_report()
        return jsonify({
            "count": len(report.history),
            "snapshots": [history_snapshot_to_dict(s) for s in report.history],
        })

    # -----------------------------------------------------------------------
    # Root-level API index (help for curl users)
    # -----------------------------------------------------------------------
    @app.get("/api")
    def api_index() -> Any:
        return jsonify({
            "endpoints": {
                "GET /":                 "HTML dashboard",
                "GET /healthz":          "Liveness probe (200 ok / 503 stale)",
                "GET /api":              "This index",
                "GET /api/health":       "Full report as JSON",
                "GET /api/nodes":        "All nodes (filter: ?state=, ?partition=)",
                "GET /api/nodes/<name>": "Single node detail",
                "GET /api/jobs":         "All jobs (filter: ?state=, ?partition=, ?user=)",
                "GET /api/alerts":       "Active alerts (filter: ?severity=)",
                "GET /api/filesystems":  "Parallel filesystem status",
                "GET /api/infiniband":   "InfiniBand fabric status",
                "GET /api/history":      "Recent trend snapshots",
            },
            "cluster": cfg.cluster_name,
        })

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hpc_monitor.server",
        description="Run the HPC monitor Flask service.")
    parser.add_argument("--config", type=Path, default=None,
                        help="Path to TOML config file (default: search cwd)")
    parser.add_argument("--host", default=None,
                        help="Bind host (overrides config)")
    parser.add_argument("--port", type=int, default=None,
                        help="Bind port (overrides config)")
    parser.add_argument("--refresh", type=int, default=None,
                        help="Collection interval in seconds (overrides config)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    )

    cfg = load_config(args.config)
    if args.host is not None:
        cfg.server.host = args.host
    if args.port is not None:
        cfg.server.port = args.port
    if args.refresh is not None:
        cfg.server.refresh_seconds = args.refresh

    cache = ReportCache()

    # Start the background collector
    collector = threading.Thread(
        target=collector_loop, args=(cfg, cache),
        name="collector", daemon=True,
    )
    collector.start()

    # Wait for the first collection to complete so we don't serve 503s
    # immediately after startup — gives up to 30 seconds.
    log.info("waiting for first collection...")
    for _ in range(60):
        report, _, _, _ = cache.snapshot()
        if report is not None:
            break
        time.sleep(0.5)
    else:
        log.warning("first collection didn't complete within 30s; starting anyway")

    app = create_app(cfg, cache)

    # Signal handling for graceful shutdown. Flask's dev server handles
    # SIGINT itself (Ctrl-C) but SIGTERM from `docker stop` needs explicit
    # handling so the collector thread has a chance to exit cleanly.
    def shutdown_handler(signum, frame):
        log.info("received signal %d, shutting down", signum)
        cache.shutdown.set()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    log.info("serving on http://%s:%d  (cluster=%s, source=%s)",
             cfg.server.host, cfg.server.port,
             cfg.cluster_name, cfg.source)

    try:
        app.run(
            host=cfg.server.host,
            port=cfg.server.port,
            debug=False,
            use_reloader=False,
            threaded=True,
        )
    finally:
        cache.shutdown.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
