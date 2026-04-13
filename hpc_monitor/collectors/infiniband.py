"""Real InfiniBand collectors — parses `ibstat` for per-port state and
`ibdiagnet --pc` for fabric-wide error counter summaries.

Design note on the data model:
    ibstat runs on a host and reports HCA (Host Channel Adapter) ports on
    that host. ibdiagnet runs on the subnet manager and reports links
    (HCA-port ↔ switch-port pairs). Our InfiniBandLink dataclass was
    originally modeled around switches for the simulation, but for real
    data we populate it with HCA device names in the `switch` field
    (e.g. 'mlx5_0') since that's what the host-side `ibstat` gives us.
    The dashboard aggregation by 'switch' then groups by HCA device,
    which is still a useful view on a compute node.

    Error counts are matched precisely by (HCA, port) rather than
    aggregated to the HCA level — if mlx5_0/U1 has symbol errors but
    mlx5_0/U2 is clean, only port 1 shows the errors.

    A future refactor could introduce a proper topology model with
    distinct node-port and switch-port types, but that's a bigger change.
"""

from __future__ import annotations

import re
import shutil
import subprocess

from ..models import InfiniBandLink


# ---------------------------------------------------------------------------
# ibstat parser
# ---------------------------------------------------------------------------
#
# ibstat output is block-structured — one block per HCA, then nested blocks
# per port within each HCA. Example:
#
#     CA 'mlx5_0'
#         CA type: MT4123
#         Number of ports: 1
#         Firmware version: 20.35.1012
#         ...
#         Port 1:
#             State: Active
#             Physical state: LinkUp
#             Rate: 200
#             Base lid: 42
#             ...
#
# We care about Port <N> blocks and their State, Physical state, and Rate
# fields. Everything else can be ignored.

# Normalize ibstat state strings to our canonical Active/Down/Polling.
# ibstat reports: Active, Down, Init/Initializing, Armed, Polling, Sleeping.
# We collapse Init/Initializing/Armed to Polling (transitional states) and
# anything else to Down.
IB_STATE_MAP = {
    "Active":       "Active",
    "Down":         "Down",
    "Initializing": "Polling",
    "Init":         "Polling",
    "Armed":        "Polling",
    "Polling":      "Polling",
    "Sleeping":     "Polling",
}


def normalize_ib_state(raw: str) -> str:
    return IB_STATE_MAP.get(raw.strip(), "Down")


_CA_LINE_RE = re.compile(r"^CA\s+'([^']+)'")
_PORT_HEADER_RE = re.compile(r"^\s+Port\s+(\d+):")
# Field lines are indented further than port headers; we match key: value
_FIELD_RE = re.compile(r"^\s+([A-Za-z ]+?):\s*(.+?)\s*$")


def parse_ibstat(text: str) -> list[InfiniBandLink]:
    """Parse `ibstat` output into InfiniBandLink records, one per port.

    Error counts are not available from ibstat — those come from ibdiagnet.
    Callers can merge the two with merge_error_counts() below.
    """
    links: list[InfiniBandLink] = []
    current_ca: str | None = None
    current_port: dict | None = None

    def flush_port():
        nonlocal current_port
        if current_ca and current_port:
            links.append(InfiniBandLink(
                switch=current_ca,
                port=str(current_port.get("num", "?")),
                speed_gbps=current_port.get("rate", 0),
                state=current_port.get("state", "Down"),
                error_count=0,
            ))
        current_port = None

    for line in text.splitlines():
        # New HCA starts a new block
        ca_match = _CA_LINE_RE.match(line)
        if ca_match:
            flush_port()
            current_ca = ca_match.group(1)
            continue

        # New port starts a new port record within the current HCA
        port_match = _PORT_HEADER_RE.match(line)
        if port_match:
            flush_port()
            current_port = {"num": int(port_match.group(1))}
            continue

        # Field within the current port
        if current_port is not None:
            field_match = _FIELD_RE.match(line)
            if field_match:
                key = field_match.group(1).strip()
                val = field_match.group(2).strip()
                if key == "State":
                    current_port["state"] = normalize_ib_state(val)
                elif key == "Rate":
                    try:
                        current_port["rate"] = int(val.split()[0])
                    except (ValueError, IndexError):
                        current_port["rate"] = 0

    flush_port()
    return links


# ---------------------------------------------------------------------------
# ibdiagnet --pc parser
# ---------------------------------------------------------------------------
#
# ibdiagnet produces a lot of output. We care about the per-link warning
# lines emitted during the Port Counters stage:
#
#   -W- link: "H-b8cef603005e1a40"/P1<-->"S-ec0d9a0300abc111"/P5 - Either
#       mlx5_0/U1 or spine01/P5 have "SymbolErrors" increased by 142 (threshold=10)
#
# Each line tells us which HCA or switch endpoint has an elevated counter
# and how much. The "Either X or Y" phrasing is because ibdiagnet doesn't
# know which side of the link is at fault without deeper probing.
#
# For our aggregate view, we attribute the error count to the HCA side
# (the "H-" GUID) since that's what matches the ibstat records we already
# have. The friendly name after "Either" (e.g. "mlx5_0/U1") also gives us
# the HCA device.

_IBDIAG_WARNING_RE = re.compile(
    r'^-W-\s+link:.*?Either\s+(\S+)\s+or\s+\S+\s+have\s+"([^"]+)"\s+'
    r'increased by\s+(\d+)'
)


def parse_ibdiagnet(text: str) -> dict[str, int]:
    """Parse ibdiagnet output and return {hca_port: total_error_count}.

    The key format is 'mlx5_0/U1' etc, matching ibdiagnet's friendly naming.
    Different counter types (SymbolErrors, LinkDownedCounter, PortRcvErrors)
    are summed into a single error count per port.
    """
    errors: dict[str, int] = {}
    for line in text.splitlines():
        m = _IBDIAG_WARNING_RE.match(line)
        if not m:
            continue
        hca_port = m.group(1)  # e.g. "mlx5_0/U1"
        try:
            count = int(m.group(3))
        except ValueError:
            continue
        errors[hca_port] = errors.get(hca_port, 0) + count
    return errors


def merge_error_counts(
    links: list[InfiniBandLink],
    errors: dict[str, int],
) -> list[InfiniBandLink]:
    """Merge ibdiagnet error counts into InfiniBandLink records from ibstat.

    ibdiagnet keys look like 'mlx5_0/U1' — 'U1' means unit (port) 1, so we
    match on both HCA name and port number when possible. Errors on one
    port of an HCA do NOT bleed over to other ports on the same HCA.
    Returns a new list; does not mutate the input.
    """
    # Build a lookup keyed by (hca_lower, port_str) for precise matching
    port_errors: dict[tuple[str, str], int] = {}
    for key, count in errors.items():
        # Split 'mlx5_0/U1' → ('mlx5_0', 'U1')
        parts = key.split("/")
        if len(parts) != 2:
            continue
        hca = parts[0].lower()
        # Strip leading 'U' or 'P' from the port designator ('U1' → '1')
        port_str = parts[1].lstrip("UuPp")
        k = (hca, port_str)
        port_errors[k] = port_errors.get(k, 0) + count

    out: list[InfiniBandLink] = []
    for link in links:
        ec = port_errors.get((link.switch.lower(), link.port), 0)
        out.append(InfiniBandLink(
            switch=link.switch,
            port=link.port,
            speed_gbps=link.speed_gbps,
            state=link.state,
            error_count=ec,
        ))
    return out


# ---------------------------------------------------------------------------
# Subprocess wrappers
# ---------------------------------------------------------------------------

def ibstat_available() -> bool:
    return shutil.which("ibstat") is not None


def ibdiagnet_available() -> bool:
    return shutil.which("ibdiagnet") is not None


def collect_ibstat() -> list[InfiniBandLink]:
    """Run `ibstat` and parse the output. Returns [] if ibstat isn't
    available or the command fails."""
    if not ibstat_available():
        return []
    try:
        result = subprocess.run(
            ["ibstat"],
            capture_output=True, text=True, check=True, timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []
    return parse_ibstat(result.stdout)


def collect_ibdiagnet_errors() -> dict[str, int]:
    """Run `ibdiagnet --pc` and return {hca_port: error_count}.

    Note: ibdiagnet needs subnet-manager-level access and typically runs as
    root. On most hosts this returns {} because the command isn't available
    or the user doesn't have permission. We treat any failure as 'no error
    data available' rather than crashing — the ibstat data is still useful
    without it.
    """
    if not ibdiagnet_available():
        return {}
    try:
        result = subprocess.run(
            ["ibdiagnet", "--pc"],
            capture_output=True, text=True, check=False, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return {}
    # ibdiagnet returns nonzero even on success sometimes; don't check
    return parse_ibdiagnet(result.stdout)


def collect_all() -> list[InfiniBandLink]:
    """Collect InfiniBand state using ibstat, enriched with ibdiagnet error
    counts if available. Returns [] if ibstat isn't installed."""
    links = collect_ibstat()
    if not links:
        return []
    errors = collect_ibdiagnet_errors()
    if errors:
        links = merge_error_counts(links, errors)
    return links
