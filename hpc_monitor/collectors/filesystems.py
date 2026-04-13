"""Real parallel filesystem collectors — Lustre (lfs df), generic df, BeeGFS.

Each parser is a pure function `parse_X(text) -> list[Filesystem]` that can
be unit-tested against fixture files without running real commands. The
collect_X() wrappers shell out and feed the parsers.

Throughput values (read_gbps, write_gbps) are not available from capacity
commands — these are set to 0.0 here. A real production tool would pull
those from /proc/fs/lustre/llite/*/stats or similar, which is a separate
data source. Marked as TODO.
"""

from __future__ import annotations

import re
import shutil
import subprocess

from ..models import Filesystem


# ---------------------------------------------------------------------------
# Size string parsing
# ---------------------------------------------------------------------------

# Map of unit suffixes to bytes. Handles both:
#   - Decimal-ish shorthand from df/lfs df: K, M, G, T, P
#   - IEC binary from beegfs-df: KiB, MiB, GiB, TiB, PiB
#
# Both `lfs df -h` and `df -h` use *binary* values despite the suffix looking
# decimal (e.g. "1.8T" really means 1.8 * 1024^4 bytes). BeeGFS spells it out
# as "1.8TiB". We treat them all as binary for consistency.
UNIT_BYTES = {
    "B":   1,
    "K":   1024,
    "KB":  1024, "KIB": 1024,
    "M":   1024 ** 2,
    "MB":  1024 ** 2, "MIB": 1024 ** 2,
    "G":   1024 ** 3,
    "GB":  1024 ** 3, "GIB": 1024 ** 3,
    "T":   1024 ** 4,
    "TB":  1024 ** 4, "TIB": 1024 ** 4,
    "P":   1024 ** 5,
    "PB":  1024 ** 5, "PIB": 1024 ** 5,
}

_SIZE_RE = re.compile(r"^([\d.]+)\s*([A-Za-z]+)?$")


def parse_size_to_tb(s: str) -> float:
    """Parse a human-readable size string ('32.0T', '1.8TiB', '500G', '64.0TiB')
    and return the value in TB (where TB means 1024^4 bytes, i.e. TiB).

    Returns 0.0 on unparseable input rather than raising — filesystems are a
    best-effort collector and we don't want one weird line to blow up the run.
    """
    if not s:
        return 0.0
    m = _SIZE_RE.match(s.strip())
    if not m:
        return 0.0
    try:
        value = float(m.group(1))
    except ValueError:
        return 0.0
    unit = (m.group(2) or "B").upper()
    bytes_per_unit = UNIT_BYTES.get(unit)
    if bytes_per_unit is None:
        return 0.0
    total_bytes = value * bytes_per_unit
    return total_bytes / (1024 ** 4)


# ---------------------------------------------------------------------------
# Lustre (lfs df -h)
# ---------------------------------------------------------------------------
#
# Output format (from the Lustre manual and real systems):
#
#   UUID                    bytes    Used   Available Use% Mounted on
#   scratch-MDT0000_UUID    1.8T     45.2G  1.7T       3% /scratch[MDT:0]
#   scratch-OST0000_UUID   32.0T     24.5T  7.5T      77% /scratch[OST:0]
#   scratch-OST0007_UUID   32.0T : inactive device
#   ...
#   filesystem_summary:    320.0T  224.6T  63.4T      78% /scratch
#
# Key parsing concerns:
#   - The last non-blank line starting with 'filesystem_summary:' gives total
#     capacity and usage.
#   - OST lines with " : inactive device" are failed targets — count them.
#   - MDT lines track metadata capacity separately from OST lines; we count
#     them for OST-count statistics but use the OST-only filesystem_summary
#     for capacity.
#   - Multiple filesystems can be mounted; we return one Filesystem per
#     unique mount point seen in the summary lines.

_LFS_SUMMARY_RE = re.compile(
    r"^filesystem_summary:\s+"
    r"(\S+)\s+"      # total
    r"(\S+)\s+"      # used
    r"(\S+)\s+"      # available
    r"(\d+)%\s+"     # use pct
    r"(\S+)"         # mount
)

_LFS_OST_LINE_RE = re.compile(
    r"^(\S+)-OST[0-9a-fA-F]+_UUID\s+(.+)"
)

_LFS_INACTIVE_RE = re.compile(r":\s*inactive\s+device", re.IGNORECASE)


def parse_lfs_df(text: str) -> list[Filesystem]:
    """Parse `lfs df -h` output into Filesystem objects, one per mounted fs."""
    filesystems_by_mount: dict[str, dict] = {}

    for line in text.splitlines():
        line = line.rstrip()
        if not line:
            continue

        # Track OST counts and inactive OSTs per filesystem name
        ost_match = _LFS_OST_LINE_RE.match(line)
        if ost_match:
            fs_name = ost_match.group(1)
            rest = ost_match.group(2)
            entry = filesystems_by_mount.setdefault(fs_name, {
                "osts_total": 0, "osts_down": 0,
                "total_tb": 0.0, "used_tb": 0.0, "mount": f"/{fs_name}",
            })
            entry["osts_total"] += 1
            if _LFS_INACTIVE_RE.search(rest):
                entry["osts_down"] += 1
            continue

        # The summary line has canonical capacity numbers
        summary_match = _LFS_SUMMARY_RE.match(line)
        if summary_match:
            total, used, _avail, _pct, mount = summary_match.groups()
            # The fs name is the last path component of the mount
            fs_name = mount.strip("/").split("/")[-1] or mount
            entry = filesystems_by_mount.setdefault(fs_name, {
                "osts_total": 0, "osts_down": 0,
                "total_tb": 0.0, "used_tb": 0.0, "mount": mount,
            })
            entry["mount"] = mount
            entry["total_tb"] = parse_size_to_tb(total)
            entry["used_tb"] = parse_size_to_tb(used)

    out: list[Filesystem] = []
    for name, entry in filesystems_by_mount.items():
        # Skip filesystems we only saw OSTs for but never got a summary line —
        # likely truncated output, better to drop than emit bogus zeros.
        if entry["total_tb"] == 0.0:
            continue
        out.append(Filesystem(
            name=name,
            mount=entry["mount"],
            fs_type="lustre",
            total_tb=entry["total_tb"],
            used_tb=entry["used_tb"],
            inodes_used_pct=0.0,  # not available from lfs df -h; needs lfs df -i
            read_gbps=0.0,        # TODO: /proc/fs/lustre/llite/*/stats
            write_gbps=0.0,
            osts_total=entry["osts_total"],
            osts_down=entry["osts_down"],
        ))
    return out


# ---------------------------------------------------------------------------
# Generic df -hT (NFS, ext4, xfs, etc.)
# ---------------------------------------------------------------------------
#
# We only care about real storage filesystems — skip tmpfs, devtmpfs,
# cgroup, overlay, and Lustre mounts (those go through parse_lfs_df).
# The interesting types are: nfs, nfs4, ext4, xfs, btrfs, zfs.

_DF_INTERESTING_TYPES = {"nfs", "nfs4", "ext4", "xfs", "btrfs", "zfs"}

# Minimum capacity (in TB) to surface a filesystem. The root disk on a login
# node (typically 50-100 GB) is noise in an HPC monitoring context — we only
# care about bulk research storage. Set to 1 TB as a reasonable floor.
_DF_MIN_CAPACITY_TB = 1.0

# df -hT output:
#   Filesystem    Type  Size  Used Avail Use% Mounted on
#   /dev/sda1     xfs   50G   12G   39G  24%  /
# Fields are whitespace-delimited but the first field (Filesystem) can
# contain ':' for NFS mounts, which is still whitespace-safe.


def parse_df_ht(text: str) -> list[Filesystem]:
    """Parse `df -hT` output, keeping only interesting filesystem types."""
    out: list[Filesystem] = []
    lines = text.splitlines()
    for line in lines[1:]:  # skip header
        parts = line.split()
        if len(parts) < 7:
            continue
        source, fs_type = parts[0], parts[1].lower()
        size, used = parts[2], parts[3]
        # Everything after 'Use%' can be a mount path with spaces — join it
        mount = " ".join(parts[6:])

        if fs_type not in _DF_INTERESTING_TYPES:
            continue

        total_tb = parse_size_to_tb(size)
        used_tb = parse_size_to_tb(used)
        if total_tb < _DF_MIN_CAPACITY_TB:
            continue

        # Derive a short name from the mount point
        name = mount.strip("/").split("/")[-1] or "root"

        out.append(Filesystem(
            name=name,
            mount=mount,
            fs_type=fs_type,
            total_tb=total_tb,
            used_tb=used_tb,
            inodes_used_pct=0.0,  # would need `df -i` for this
            read_gbps=0.0,
            write_gbps=0.0,
        ))
    return out


# ---------------------------------------------------------------------------
# BeeGFS (beegfs-df)
# ---------------------------------------------------------------------------
#
# Output has two sections: METADATA SERVERS and STORAGE TARGETS. We want
# the aggregate of the STORAGE TARGETS section for capacity, and count the
# metadata targets separately. Format:
#
#     METADATA SERVERS:
#     TargetID   Cap. Pool        Total         Free    %      ITotal       IFree    %
#     ========   =========        =====         ====    =      ======       =====    =
#            1       normal      1.8TiB       1.7TiB  94%     1932.7M     1845.2M  95%
#
#     STORAGE TARGETS:
#     (same format, different rows)
#
# Note that Free is given, not Used — we compute Used = Total - Free.

def parse_beegfs_df(text: str) -> list[Filesystem]:
    """Parse beegfs-df output. Returns one Filesystem representing the
    aggregate storage pool."""
    section: str | None = None
    storage_total = 0.0
    storage_used = 0.0
    storage_count = 0
    inode_used_sum = 0.0
    inode_count = 0

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        up = stripped.upper()
        if up.startswith("METADATA SERVERS"):
            section = "metadata"
            continue
        if up.startswith("STORAGE TARGETS"):
            section = "storage"
            continue
        # Skip header/separator rows
        if stripped.startswith("TargetID") or stripped.startswith("====="):
            continue

        parts = stripped.split()
        if len(parts) < 8:
            continue
        # parts: [TargetID, Pool, Total, Free, Use%, ITotal, IFree, IUse%]
        # Index:  0         1     2      3     4     5       6      7
        try:
            int(parts[0])  # confirm first col is a target id
        except ValueError:
            continue

        if section != "storage":
            continue

        total_tb = parse_size_to_tb(parts[2])
        free_tb = parse_size_to_tb(parts[3])
        if total_tb == 0.0:
            continue

        used_tb = max(total_tb - free_tb, 0.0)
        storage_total += total_tb
        storage_used += used_tb
        storage_count += 1

        # Inode use% is the 8th column, e.g. "95%"
        try:
            inode_pct_str = parts[7].rstrip("%")
            inode_free_pct = float(inode_pct_str)
            inode_used_sum += (100.0 - inode_free_pct)
            inode_count += 1
        except (ValueError, IndexError):
            pass

    if storage_count == 0:
        return []

    avg_inode_used = inode_used_sum / inode_count if inode_count else 0.0

    return [Filesystem(
        name="beegfs",
        mount="/beegfs",
        fs_type="beegfs",
        total_tb=storage_total,
        used_tb=storage_used,
        inodes_used_pct=avg_inode_used,
        read_gbps=0.0,
        write_gbps=0.0,
        osts_total=storage_count,
        osts_down=0,  # beegfs-df doesn't directly report target health;
                      # use `beegfs-ctl --listtargets --state` for that.
    )]


# ---------------------------------------------------------------------------
# Subprocess wrappers
# ---------------------------------------------------------------------------

def lfs_available() -> bool:
    return shutil.which("lfs") is not None


def df_available() -> bool:
    return shutil.which("df") is not None


def beegfs_df_available() -> bool:
    return shutil.which("beegfs-df") is not None


def collect_lustre() -> list[Filesystem]:
    """Run `lfs df -h` and parse the output. Returns [] if lfs isn't on PATH
    or the command fails."""
    if not lfs_available():
        return []
    try:
        result = subprocess.run(
            ["lfs", "df", "-h"],
            capture_output=True, text=True, check=True, timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []
    return parse_lfs_df(result.stdout)


def collect_df() -> list[Filesystem]:
    """Run `df -hT` and parse the output."""
    if not df_available():
        return []
    try:
        result = subprocess.run(
            ["df", "-hT"],
            capture_output=True, text=True, check=True, timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []
    return parse_df_ht(result.stdout)


def collect_beegfs() -> list[Filesystem]:
    """Run `beegfs-df` and parse the output."""
    if not beegfs_df_available():
        return []
    try:
        result = subprocess.run(
            ["beegfs-df"],
            capture_output=True, text=True, check=True, timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []
    return parse_beegfs_df(result.stdout)


def collect_all() -> list[Filesystem]:
    """Run all available real collectors and return the merged list.

    In production this is what gets called when source=slurm — try every
    real collector, return whatever we find. If nothing's available (e.g.
    running on a workstation with no HPC tools), returns an empty list and
    the caller should fall back to simulation.
    """
    filesystems: list[Filesystem] = []
    filesystems.extend(collect_lustre())
    filesystems.extend(collect_beegfs())
    filesystems.extend(collect_df())
    return filesystems
