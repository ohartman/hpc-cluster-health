# hpc-cluster-health — container image for the Flask service
#
# Build:  docker build -t hpc-cluster-health .
# Run:    docker run -p 8080:8080 -v hpc_data:/data hpc-cluster-health
# Or use the compose.yaml for the full setup with persistent history.

FROM python:3.12-slim

# We deliberately don't install anything via apt to avoid Debian repo
# hash-mismatch issues that can break the build for hours at a time.
# Instead we use Python alternatives:
#   - signal handling: handled directly in server.py, no tini needed
#   - healthcheck: Python one-liner instead of curl

# Non-root user. Matches the security pattern used at ICSD and means a
# container escape doesn't give attackers root on the host.
RUN useradd --create-home --shell /bin/bash --uid 10001 hpcmon

WORKDIR /app

# Install dependencies first so the layer can be cached across code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the package and config. fixtures/ and tests/ are not included in
# the image — they're for development, not production. We don't ship the
# example TOML either; the user mounts their real config at runtime.
COPY hpc_monitor/ ./hpc_monitor/
COPY hpc_monitor.example.toml /app/hpc_monitor.example.toml

# The history database and any user config live under /data so they
# survive container restarts via a volume mount.
RUN mkdir -p /data \
 && chown -R hpcmon:hpcmon /app /data

USER hpcmon

# hpc_monitor.toml is expected to live in /data so users can customize it
# by editing the file on the host. If it's missing, the script uses
# defaults, which is fine for a demo.
WORKDIR /data

# The package code lives at /app but we run from /data so config and
# history database paths resolve relative to the mounted volume. PYTHONPATH
# makes the package importable from anywhere.
ENV PYTHONPATH=/app

EXPOSE 8080

# HEALTHCHECK using Python instead of curl — avoids the apt dependency.
# Hits /healthz and exits 0 if the response status is 200.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz').status == 200 else 1)"

# STOPSIGNAL tells Docker which signal to send on `docker stop`. Default
# is SIGTERM which our server.py already handles, but being explicit
# documents the intent. Without an init like tini, Python is PID 1 and
# receives the signal directly — that's fine because we have a signal
# handler installed in server.py.
STOPSIGNAL SIGTERM

CMD ["python3", "-m", "hpc_monitor.server"]
