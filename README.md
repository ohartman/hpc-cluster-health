# hpc-cluster-health

An HPC cluster health monitoring tool that collects operational metrics from
a Slurm-managed cluster and renders a dark-themed HTML dashboard with current
state, threshold-based alerts, and historical trend sparklines. Ships as both
a one-shot CLI for cron/systemd-timer use and a Flask service with a JSON
API and a containerized deployment.

A learning project exploring the data sources and operational concerns of
HPC system administration: Slurm scheduler state, parallel filesystem health
(Lustre, BeeGFS, GPFS, NFS), InfiniBand fabric status, and per-node CPU and
memory pressure.

Python 3.11+. The CLI uses only stdlib; the Flask service adds Flask as its
single dependency.

## Features

- **Two data sources.** `--source sim` generates synthetic but realistic
  data for development and demos. `--source slurm` shells out to real HPC
  tools and parses their output. `--source auto` picks slurm if the commands
  are on PATH, otherwise falls back to sim.

- **Real parsers for every data source.** No more simulation in slurm mode:
  `sinfo`/`squeue` for nodes and jobs, `lfs df`/`df -hT`/`beegfs-df` for
  filesystems, `ibstat`/`ibdiagnet` for InfiniBand. Each parser is a pure
  function tested against fixture files captured from the tools' documented
  output.

- **Threshold-based alerting.** Configurable warning and critical thresholds
  for CPU load, memory pressure, storage capacity, and job queue wait times.
  Alerts are sorted by severity and grouped by component path
  (`node/cn042`, `fs/scratch`, `scheduler`, `infiniband`).

- **Historical trends.** Each run appends a snapshot of aggregate metrics to
  a SQLite database. The report includes a trends section with inline SVG
  sparklines showing how utilization, queue depth, storage, and node health
  have moved over the configured window. Old snapshots are pruned on each
  run.

- **Flask service mode.** Run as a long-lived service with a background
  collector thread that refreshes every 60 seconds and a JSON API for
  scripting. Endpoints for nodes, jobs, alerts, filesystems, InfiniBand,
  history, and a `/healthz` liveness probe for container orchestration.

- **Containerized.** Dockerfile and compose.yaml included. Runs as a
  non-root user, with the SQLite history database persisted to a volume.
  Works with both Docker and Podman.

- **External config.** Cluster name, thresholds, history settings, partition
  filters, and server settings all live in a TOML file. CLI flags override
  the file.

- **Test suite.** 85 unit tests covering every parser, run with
  `python3 -m unittest discover`. No pytest dependency.

## Data sources

| Source               | CLI / sim mode | slurm mode command                                 |
|----------------------|----------------|----------------------------------------------------|
| Compute nodes        | simulated      | `sinfo -N -h -o '%N|%P|%T|%C|%m|%O|%e|%G|%u'`      |
| Slurm jobs           | simulated      | `squeue -h -o '%i|%u|%a|%P|%j|%T|%D|%C|%V|%S|%l|%r'` |
| Lustre               | simulated      | `lfs df -h`                                        |
| BeeGFS               | simulated      | `beegfs-df`                                        |
| NFS / ext4 / xfs     | simulated      | `df -hT`                                           |
| InfiniBand state     | simulated      | `ibstat`                                           |
| InfiniBand errors    | simulated      | `ibdiagnet --pc`                                   |

## Quick start — CLI

```bash
# Requires Python 3.11+ for tomllib
python3 --version

# Copy the example config
cp hpc_monitor.example.toml hpc_monitor.toml

# Run the one-shot CLI
python3 -m hpc_monitor --open

# Run a few more times to populate the trends section
python3 -m hpc_monitor
python3 -m hpc_monitor
python3 -m hpc_monitor --open
```

The CLI writes `cluster_health.html` to the current directory and appends
a snapshot to `history.db` on each run.

## Quick start — Flask service

```bash
# Create a venv and install Flask
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run the service
python3 -m hpc_monitor.server --verbose
```

The service binds to `0.0.0.0:8080` by default. Open
<http://127.0.0.1:8080/> in a browser for the dashboard, or hit the JSON
API:

```bash
curl -s http://127.0.0.1:8080/api | python3 -m json.tool
curl -s http://127.0.0.1:8080/api/health | python3 -m json.tool
curl -s "http://127.0.0.1:8080/api/nodes?state=down" | python3 -m json.tool
curl -s "http://127.0.0.1:8080/api/alerts?severity=critical" | python3 -m json.tool
```

## Quick start — container

```bash
# Build and run with compose (works with docker or podman)
docker compose up

# Or build and run directly
docker build -t hpc-cluster-health .
docker run -p 8080:8080 -v hpc_data:/data hpc-cluster-health
```

Open <http://127.0.0.1:8080/> once the container is healthy (usually
5–10 seconds after startup — the background collector has to complete
its first pass). The named volume `hpc_data` persists the SQLite history
database across restarts.

To customize configuration or inspect the database from the host, swap
the named volume for a bind mount in `compose.yaml`:

```yaml
volumes:
  - ./data:/data
```

Then `mkdir -p data` on the host before starting. See the comments in
`compose.yaml` for details.

## CLI usage

```
python3 -m hpc_monitor [options]

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

## Server usage

```
python3 -m hpc_monitor.server [options]

  --config PATH         TOML config file (default: search cwd)
  --host HOST           Bind host (overrides config, default 0.0.0.0)
  --port PORT           Bind port (overrides config, default 8080)
  --refresh SECONDS     Collection interval (overrides config, default 60)
  --verbose, -v         Verbose logging
```

### Endpoints

| Method | Path                  | Description                                    |
|--------|-----------------------|------------------------------------------------|
| GET    | `/`                   | HTML dashboard                                 |
| GET    | `/healthz`            | Liveness probe (200 ok / 503 stale)            |
| GET    | `/api`                | Endpoint index                                 |
| GET    | `/api/health`         | Full report as JSON                            |
| GET    | `/api/nodes`          | All nodes (filter: `?state=`, `?partition=`)   |
| GET    | `/api/nodes/<name>`   | Single node detail                             |
| GET    | `/api/jobs`           | All jobs (filter: `?state=`, `?partition=`, `?user=`) |
| GET    | `/api/alerts`         | Active alerts (filter: `?severity=`)           |
| GET    | `/api/filesystems`    | Parallel filesystem status                     |
| GET    | `/api/infiniband`     | InfiniBand fabric status                       |
| GET    | `/api/history`        | Recent trend snapshots                         |

The background collector runs every `refresh_seconds` (default 60) and
writes results to a thread-safe cache. HTTP handlers read from the cache,
so requests return instantly even while a collection is in progress.

`/healthz` returns 200 as long as the last successful collection was
within `healthz_staleness_limit` seconds (default 300). After that it
returns 503, which is what Docker's HEALTHCHECK and Kubernetes liveness
probes use to decide a container needs restarting.

## Configuration

The script and server both look for `hpc_monitor.toml` in the current
working directory, or use `--config` to point at a different path. A
missing config is fine — defaults apply.

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

[server]
host = "0.0.0.0"
port = 8080
refresh_seconds = 60
healthz_staleness_limit = 300
```

CLI flags override config values. Precedence order: defaults, then config
file, then CLI flags.

## Running the tests

```bash
python3 -m unittest discover -v
```

85 tests across the Slurm, filesystem, and InfiniBand parsers, plus their
supporting helpers (state normalization, size parsing, duration parsing,
error merging). Uses only stdlib `unittest` — no pytest dependency.

Fixture files in `fixtures/` contain sample output from real HPC commands
(`lfs df -h`, `ibstat`, `ibdiagnet --pc`, `df -hT`, `beegfs-df`) captured
from the tools' documented output formats. The parsers are tested against
these fixtures, which means they work against real command output without
needing access to an actual cluster.

## Deployment patterns

### Cron one-shot

The CLI is designed to be run periodically from cron or a systemd timer,
writing its HTML output to a shared web root served by nginx or Apache:

```cron
* * * * * cd /opt/hpc-monitor && /usr/bin/python3 -m hpc_monitor --output /var/www/html/cluster.html
```

### systemd timer

Cleaner than cron for anything that's part of a service:

```ini
# /etc/systemd/system/hpc-monitor.service
[Unit]
Description=Generate HPC cluster health report
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/hpc-monitor
ExecStart=/usr/bin/python3 -m hpc_monitor --output /var/www/html/cluster.html
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

### Flask service behind nginx

For dashboard-style deployments where you want the full JSON API and
real-time refresh, run the Flask service behind nginx as a reverse proxy.
The service handles its own refresh loop; nginx just passes requests
through. Gunicorn is overkill for a monitoring dashboard but can be
swapped in if request volume is high.

### Container

`docker compose up` is the fastest path for a demo or home lab. For
production, run the container under a real orchestrator (Kubernetes,
Nomad, Docker Swarm) and let the built-in HEALTHCHECK drive
restart-on-failure behavior.

## How the history database works

Each run inserts one row into the `snapshots` table in `history.db`. The
schema stores aggregate metrics only — total/allocated cores, healthy/down
node counts, jobs running and pending, total/used storage, and alert counts
by severity. No per-node or per-job detail, so the database stays small
even after months of runs (a row is roughly 100 bytes; one snapshot per
minute for a year is about 50 MB).

The `cluster` column scopes snapshots, so one database can hold history
for multiple clusters and the trends in each report only show the matching
cluster's data.

Snapshots older than `retention_days` are pruned at the start of each run.

## Package layout

```
hpc_monitor/
├── __init__.py
├── __main__.py               # python3 -m hpc_monitor
├── cli.py                    # CLI entry point
├── server.py                 # python3 -m hpc_monitor.server
├── config.py                 # TOML loader + dataclasses
├── models.py                 # ComputeNode, Job, Filesystem, etc.
├── pipeline.py               # collect → evaluate → persist
├── alerts.py                 # Threshold evaluation
├── history.py                # SQLite layer
├── render.py                 # HTML rendering
├── serialize.py              # JSON converters for the API
└── collectors/
    ├── __init__.py
    ├── slurm.py              # sinfo, squeue
    ├── filesystems.py        # lfs df, df -hT, beegfs-df
    ├── infiniband.py         # ibstat, ibdiagnet
    └── sim.py                # synthetic data

tests/
├── test_slurm_parsers.py
├── test_filesystem_parsers.py
└── test_infiniband_parsers.py

fixtures/                     # Sample output for parser tests
├── lfs_df.txt
├── lfs_df_healthy.txt
├── df_ht.txt
├── beegfs_df.txt
├── ibstat.txt
├── ibstat_healthy.txt
└── ibdiagnet.txt
```

## Extending

- Add a real parser: write `parse_X(text) -> list[Model]` as a pure
  function alongside a `collect_X()` subprocess wrapper, capture a fixture
  file from the real command's output, add tests, and wire it into
  `pipeline.py` behind the `source == "slurm"` branch.
- Add a new alert rule: extend `evaluate_alerts()` in `alerts.py`. Every
  alert has a severity, a component path, and a message.
- Add an API endpoint: add a route to `create_app()` in `server.py`
  that reads from the cache and serializes via `serialize.py`.
- Add trend metrics: extend `HistorySnapshot` in `models.py`, add the
  columns to the SQL schema in `history.py`, capture them in
  `build_snapshot()`, and render them in `render.py`.

Possible next steps: Prometheus metrics exporter, per-node history for
state-change timelines, job efficiency analyzer (flagging jobs that
request much more CPU/memory than they use), authentication for the
service mode.

## License

MIT
