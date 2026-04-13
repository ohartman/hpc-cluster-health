# hpc-cluster-health

A small HPC cluster health monitoring tool that collects operational metrics
from a Slurm-managed cluster and renders a dark-themed HTML dashboard with
current state, threshold-based alerts, and historical trend sparklines.

A learning project exploring the data sources and operational concerns of
HPC system administration: Slurm scheduler state, parallel filesystem health
(Lustre, BeeGFS, GPFS, NFS), InfiniBand fabric status, and per-node CPU and
memory pressure.

Pure Python 3.11+, no third-party dependencies.

## Screenshot

Run the script to generate `cluster_health.html` and open it in a browser.
The report has an overview header, a trends section with sparklines, an
alerts feed, parallel filesystem capacity bars, the top running jobs by
core count, queue depth by partition, an InfiniBand fabric summary, and
a compute node table that surfaces problem nodes first.

## Features

- **Two data sources.** `--source sim` generates synthetic but realistic
  data for development and demos. `--source slurm` shells out to `sinfo`
  and `squeue` and parses their output. `--source auto` picks slurm if
  the commands are on PATH, otherwise falls back to sim.

- **Threshold-based alerting.** Configurable warning and critical
  thresholds for CPU load, memory pressure, storage capacity, and job
  queue wait times. Alerts are sorted by severity and grouped by
  component path (`node/cn042`, `fs/scratch`, `scheduler`, `infiniband`).

- **Historical trends.** Each run appends a snapshot of aggregate metrics
  to a SQLite database. The report includes a trends section with inline
  SVG sparklines showing how utilization, queue depth, storage, and node
  health have moved over the configured window. Old snapshots are pruned
  on each run.

- **External config.** Cluster name, thresholds, history settings, and
  partition filters live in a TOML file. CLI flags override the file.

- **Single file, stdlib only.** No virtualenv, no `pip install`. Drop
  `hpc_cluster_health.py` on a host with Python 3.11+ and run it.

## What's simulated vs. real

| Data source          | Simulation | Real implementation                    |
|----------------------|------------|----------------------------------------|
| Compute nodes        | yes        | yes — parses `sinfo -N -h -o ...`      |
| Slurm jobs           | yes        | yes — parses `squeue -h -o ...`        |
| Parallel filesystems | yes        | TODO — `lfs df`, `beegfs-df`, `mmlsfs` |
| InfiniBand fabric    | yes        | TODO — `ibstat`, `ibdiagnet --pc`      |

The Slurm parsers handle state abbreviations (`alloc*`, `drng`, `mixed+drain`),
GRES strings for GPU counts, and Slurm's awkward duration formats
(`30` = 30 min, `1-12:00:00` = 36h, `UNLIMITED`, etc.).

## Quick start

```bash
# Requires Python 3.11+ for tomllib
python3 --version

# Copy the example config (the script looks for hpc_monitor.toml in cwd)
cp hpc_monitor.example.toml hpc_monitor.toml

# Run it
python3 hpc_cluster_health.py --open

# Run a few more times to populate the trends section
python3 hpc_cluster_health.py
python3 hpc_cluster_health.py
python3 hpc_cluster_health.py --open
```

The script writes `cluster_health.html` to the current directory and
appends a snapshot to `history.db` on each run.

## Usage

```
python3 hpc_cluster_health.py [options]

  --config PATH         TOML config file (default: search cwd)
  --source {sim,slurm,auto}
                        Data source (overrides config)
  --nodes N             Simulated node count (overrides config)
  --cluster-name NAME   Cluster name (overrides config)
  --output PATH         Output HTML file path (default: cluster_health.html)
  --seed N              Random seed for reproducible simulation
  --no-history          Skip writing this run to the history database
  --open                Open the report in the default browser
```

### Common invocations

```bash
# Bigger simulated cluster
python3 hpc_cluster_health.py --nodes 256 --open

# Reproducible output for screenshots
python3 hpc_cluster_health.py --seed 42 --open

# On a real Slurm cluster
python3 hpc_cluster_health.py --source slurm --open

# Auto-detect (try slurm, fall back to sim)
python3 hpc_cluster_health.py --source auto

# Multi-cluster monitoring (history is scoped per cluster name)
python3 hpc_cluster_health.py --cluster-name aurora
python3 hpc_cluster_health.py --cluster-name borealis
```

## Configuration

The script looks for `hpc_monitor.toml` in the current working directory,
or use `--config` to point at a different path. A missing config is fine —
defaults apply.

```toml
[cluster]
name = "aurora"
source = "sim"          # sim | slurm | auto
sim_nodes = 64

[partitions]
include = []            # empty = include all partitions

[history]
database = "history.db"
display_days = 7
retention_days = 90

[thresholds.load]
warning = 0.85          # 85% of core count
critical = 1.10

[thresholds.memory]
warning = 0.85
critical = 0.95

[thresholds.storage]
warning = 0.80
critical = 0.92

[thresholds.queue]
wait_warning_hours = 4.0
wait_critical_hours = 12.0
```

CLI flags override config values. Precedence order: defaults, then config
file, then CLI flags.

## Periodic execution

The intended deployment pattern is to run the script every minute or so
from cron or a systemd timer, writing the HTML output to a shared web root:

```cron
* * * * * cd /opt/hpc-monitor && /usr/bin/python3 hpc_cluster_health.py --output /var/www/html/cluster.html
```

A systemd timer is cleaner for production:

```ini
# /etc/systemd/system/hpc-monitor.service
[Unit]
Description=Generate HPC cluster health report
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/hpc-monitor
ExecStart=/usr/bin/python3 hpc_cluster_health.py --output /var/www/html/cluster.html
User=hpcmon
```

```ini
# /etc/systemd/system/hpc-monitor.timer
[Unit]
Description=Run HPC monitor every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s

[Install]
WantedBy=timers.target
```

## How the history database works

Each run inserts one row into the `snapshots` table in `history.db`. The
schema stores aggregate metrics only — total/allocated cores, healthy/down
node counts, jobs running and pending, total/used storage, and alert
counts by severity. No per-node or per-job detail, so the database stays
small even after months of runs (a row is roughly 100 bytes; one snapshot
per minute for a year is about 50 MB).

The `cluster` column scopes snapshots, so one database can hold history
for multiple clusters and the trends in each report only show the
matching cluster's data.

Snapshots older than `retention_days` are pruned at the start of each run.

## Extending it

The collectors are deliberately structured so the simulated and real
implementations are interchangeable. To add real parsers for filesystems
or InfiniBand, write a `collect_filesystems_real()` and
`collect_infiniband_real()` alongside the existing `_sim()` versions, then
wire them into `collect_report()` based on the source.

Other reasonable next steps:

- Wrap the script in a small Flask or FastAPI service that regenerates
  the report on a schedule and serves it from `/`, with a JSON endpoint
  at `/api/health`.
- Add a `node_history` table to track per-node state changes over time
  and surface "node cn042 has been in drain state for 18 hours."
- Add a job efficiency analyzer that flags jobs requesting much more
  CPU or memory than they actually use.
- Containerize the service with Podman or Docker.

## Files

- `hpc_cluster_health.py` — the script (single file, stdlib only)
- `hpc_monitor.example.toml` — annotated example config
- `history.db` — created automatically on first run
- `cluster_health.html` — created on each run

## License

MIT
