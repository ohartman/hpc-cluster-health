"""SQLite history persistence — aggregate snapshot storage and retrieval."""

from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from .models import HistorySnapshot


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
