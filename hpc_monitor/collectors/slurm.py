"""Slurm data collection — parses sinfo and squeue output."""

from __future__ import annotations

import datetime as dt
import shutil
import subprocess

from ..models import ComputeNode, Job


def slurm_available() -> bool:
    return shutil.which("sinfo") is not None and shutil.which("squeue") is not None


SLURM_STATE_MAP = {
    "idle": "idle",
    "alloc": "allocated",
    "allocated": "allocated",
    "mix": "mixed",
    "mixed": "mixed",
    "comp": "allocated",
    "completing": "allocated",
    "down": "down",
    "drain": "drain",
    "drng": "drain",
    "draining": "drain",
    "drained": "drain",
    "maint": "maint",
    "resv": "maint",
    "reserved": "maint",
    "fail": "down",
    "failing": "down",
    "unk": "down",
    "unknown": "down",
}


def normalize_slurm_state(raw: str) -> str:
    """Strip trailing modifiers and map to our canonical state names."""
    base = raw.lower().rstrip("*~$#@+")
    if "drain" in base:
        return "drain"
    if "down" in base:
        return "down"
    if "maint" in base or "resv" in base:
        return "maint"
    return SLURM_STATE_MAP.get(base, base)


def parse_sinfo_cores(field_str: str) -> tuple[int, int]:
    """Parse sinfo's %C field: 'A/I/O/T' = allocated/idle/other/total."""
    parts = field_str.split("/")
    if len(parts) != 4:
        return (0, 0)
    try:
        return (int(parts[0]), int(parts[3]))
    except ValueError:
        return (0, 0)


def parse_slurm_time(s: str) -> dt.datetime | None:
    """Slurm timestamps are 'YYYY-MM-DDTHH:MM:SS'. 'N/A' or 'Unknown' → None."""
    if not s or s in ("N/A", "Unknown", "None"):
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def parse_slurm_duration(s: str) -> float:
    """Parse Slurm time-limit strings to hours.

    Slurm formats: 'minutes', 'minutes:seconds', 'hours:minutes:seconds',
    'days-hours', 'days-hours:minutes', 'days-hours:minutes:seconds',
    or 'UNLIMITED'.
    """
    if not s or s in ("UNLIMITED", "INVALID", "NOT_SET"):
        return 0.0
    days = 0
    rest = s
    if "-" in s:
        d_str, rest = s.split("-", 1)
        try:
            days = int(d_str)
        except ValueError:
            days = 0
    parts = rest.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0.0
    if len(nums) == 1:
        h, m, sec = 0, nums[0], 0
    elif len(nums) == 2:
        if days:
            h, m, sec = nums[0], nums[1], 0
        else:
            h, m, sec = 0, nums[0], nums[1]
    elif len(nums) == 3:
        h, m, sec = nums
    else:
        return 0.0
    return days * 24 + h + m / 60 + sec / 3600


def parse_sinfo_output(text: str) -> list[ComputeNode]:
    """Parse sinfo output into ComputeNode objects. Pure function — testable."""
    nodes: list[ComputeNode] = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 9:
            continue
        name, partition, state_raw, cores_raw, mem_raw, load_raw, _free_raw, gres, reason = parts[:9]

        cores_alloc, cores_total = parse_sinfo_cores(cores_raw)
        try:
            mem_total_mb = int(mem_raw)
        except ValueError:
            mem_total_mb = 0
        mem_total_gb = mem_total_mb // 1024

        try:
            load = float(load_raw) if load_raw not in ("N/A", "") else 0.0
        except ValueError:
            load = 0.0

        # Parse GRES like "gpu:4" or "gpu:tesla:4(S:0-1)"
        gpu_count = 0
        if "gpu" in gres.lower():
            for token in gres.split(","):
                if token.lower().startswith("gpu"):
                    bits = token.split(":")
                    try:
                        gpu_count = int(bits[-1].split("(")[0])
                    except (ValueError, IndexError):
                        pass

        state = normalize_slurm_state(state_raw)
        # Memory used isn't directly in sinfo; estimate from allocation ratio
        # as a placeholder. A production version would query
        # `scontrol show node <n>` for AllocMem.
        ratio = (cores_alloc / cores_total) if cores_total else 0
        mem_used_gb = round(mem_total_gb * ratio, 1)

        nodes.append(ComputeNode(
            name=name.strip(),
            partition=partition.strip(),
            state=state,
            cores_total=cores_total,
            cores_alloc=cores_alloc,
            mem_total_gb=mem_total_gb,
            mem_used_gb=mem_used_gb,
            load_1min=load,
            load_5min=load,
            load_15min=load,
            gpu_count=gpu_count,
            gpu_alloc=int(gpu_count * ratio) if gpu_count else 0,
            uptime_days=0,
            reason=reason.strip() if reason.strip() != "none" else "",
        ))
    return nodes


def parse_squeue_output(text: str) -> list[Job]:
    """Parse squeue output into Job objects. Pure function — testable."""
    jobs: list[Job] = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 12:
            continue
        try:
            job_id = int(parts[0].split("_")[0].split("+")[0])
        except ValueError:
            continue
        try:
            nodes = int(parts[6])
            cores = int(parts[7])
        except ValueError:
            nodes, cores = 0, 0

        submit = parse_slurm_time(parts[8]) or dt.datetime.now()
        start = parse_slurm_time(parts[9])

        jobs.append(Job(
            job_id=job_id,
            user=parts[1],
            account=parts[2],
            partition=parts[3],
            name=parts[4],
            state=parts[5],
            nodes=nodes,
            cores=cores,
            submit_time=submit,
            start_time=start,
            time_limit_hours=parse_slurm_duration(parts[10]),
            reason=parts[11],
        ))
    return jobs


def collect_compute_nodes() -> list[ComputeNode]:
    """Run sinfo and parse one node per line.

    Format string fields:
        %N node name      %P partition    %T state
        %C cores A/I/O/T  %m memory MB    %O CPU load
        %e free memory MB %G generic res  %u reason
    """
    fmt = "%N|%P|%T|%C|%m|%O|%e|%G|%u"
    result = subprocess.run(
        ["sinfo", "-N", "-h", "-o", fmt],
        capture_output=True, text=True, check=True,
    )
    return parse_sinfo_output(result.stdout)


def collect_jobs() -> list[Job]:
    """Run squeue and parse one job per line.

    Format string fields:
        %i job id     %u user        %a account   %P partition
        %j name       %T state       %D nodes     %C cores
        %V submit     %S start       %l time lim  %r reason
    """
    fmt = "%i|%u|%a|%P|%j|%T|%D|%C|%V|%S|%l|%r"
    result = subprocess.run(
        ["squeue", "-h", "-o", fmt],
        capture_output=True, text=True, check=True,
    )
    return parse_squeue_output(result.stdout)
