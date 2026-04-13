"""Synthetic data generation for development and demos."""

from __future__ import annotations

import datetime as dt
import random

from ..models import ComputeNode, Filesystem, InfiniBandLink, Job


PARTITIONS = ["compute", "gpu", "bigmem", "debug"]
USERS = ["jchen", "mpatel", "kowalski", "nasser", "rodriguez", "okonkwo",
         "tanaka", "fitzgerald", "ahmadi", "vasquez", "lindberg", "park"]
ACCOUNTS = ["physics", "biochem", "cs-ml", "astro", "climate", "engineering"]
JOB_NAMES = ["lammps_run", "vasp_relax", "gromacs_md", "tf_train",
             "openfoam_sim", "mpi_solve", "namd_protein", "ansys_cfd"]


def collect_compute_nodes(count: int) -> list[ComputeNode]:
    nodes: list[ComputeNode] = []
    for i in range(count):
        partition = random.choices(
            PARTITIONS, weights=[60, 25, 10, 5], k=1
        )[0]
        if partition == "gpu":
            cores_total, mem_total, gpu_count = 64, 512, 4
        elif partition == "bigmem":
            cores_total, mem_total, gpu_count = 96, 1536, 0
        elif partition == "debug":
            cores_total, mem_total, gpu_count = 32, 192, 0
        else:
            cores_total, mem_total, gpu_count = 48, 384, 0

        state = random.choices(
            ["allocated", "mixed", "idle", "drain", "down", "maint"],
            weights=[55, 20, 18, 4, 2, 1],
            k=1,
        )[0]

        if state in ("down", "drain", "maint"):
            cores_alloc = 0
            mem_used = 0.0
            load = 0.0
            gpu_alloc = 0
            reason = random.choice([
                "Not responding", "kernel panic 04:23 UTC",
                "Scheduled maintenance window", "IB port flapping",
                "GPU ECC errors", "DIMM failure slot 3",
            ])
        else:
            if state == "allocated":
                util = random.uniform(0.85, 1.05)
            elif state == "mixed":
                util = random.uniform(0.40, 0.85)
            else:
                util = random.uniform(0.0, 0.10)
            cores_alloc = min(cores_total, int(cores_total * util))
            mem_used = round(mem_total * util * random.uniform(0.7, 1.05), 1)
            mem_used = min(mem_used, mem_total * 0.99)
            load = round(cores_total * util * random.uniform(0.9, 1.1), 2)
            gpu_alloc = min(gpu_count, int(gpu_count * util)) if gpu_count else 0
            reason = ""

        nodes.append(ComputeNode(
            name=f"cn{i+1:03d}",
            partition=partition,
            state=state,
            cores_total=cores_total,
            cores_alloc=cores_alloc,
            mem_total_gb=mem_total,
            mem_used_gb=mem_used,
            load_1min=round(load * random.uniform(0.95, 1.05), 2),
            load_5min=load,
            load_15min=round(load * random.uniform(0.92, 1.03), 2),
            gpu_count=gpu_count,
            gpu_alloc=gpu_alloc,
            uptime_days=random.randint(1, 180),
            reason=reason,
        ))
    return nodes


def collect_jobs(node_count: int) -> list[Job]:
    now = dt.datetime.now()
    jobs: list[Job] = []
    job_count = max(20, node_count * 2)
    for i in range(job_count):
        state = random.choices(
            ["RUNNING", "PENDING", "COMPLETING", "FAILED"],
            weights=[55, 38, 5, 2], k=1,
        )[0]
        nodes = random.choices(
            [1, 2, 4, 8, 16, 32], weights=[35, 25, 20, 12, 6, 2], k=1
        )[0]
        cores_per_node = random.choice([16, 24, 32, 48])
        submit_offset_hours = random.uniform(0.1, 36.0)
        submit_time = now - dt.timedelta(hours=submit_offset_hours)

        if state == "RUNNING":
            start_offset = random.uniform(0.0, submit_offset_hours - 0.05)
            start_time = now - dt.timedelta(hours=submit_offset_hours - start_offset)
            reason = "None"
        elif state == "PENDING":
            start_time = None
            reason = random.choices(
                ["Resources", "Priority", "QOSMaxCpuPerUserLimit",
                 "ReqNodeNotAvail", "Dependency"],
                weights=[50, 30, 8, 8, 4], k=1,
            )[0]
        else:
            start_time = now - dt.timedelta(hours=random.uniform(0.5, 12))
            reason = "None"

        jobs.append(Job(
            job_id=1_000_000 + i,
            user=random.choice(USERS),
            account=random.choice(ACCOUNTS),
            partition=random.choices(PARTITIONS, weights=[60, 25, 10, 5], k=1)[0],
            name=random.choice(JOB_NAMES),
            state=state,
            nodes=nodes,
            cores=nodes * cores_per_node,
            submit_time=submit_time,
            start_time=start_time,
            time_limit_hours=random.choice([1, 4, 12, 24, 48, 72]),
            reason=reason,
        ))
    return jobs


def collect_filesystems() -> list[Filesystem]:
    return [
        Filesystem(
            name="scratch", mount="/scratch", fs_type="lustre",
            total_tb=2048.0,
            used_tb=round(random.uniform(1400, 1850), 1),
            inodes_used_pct=round(random.uniform(40, 75), 1),
            read_gbps=round(random.uniform(45, 88), 1),
            write_gbps=round(random.uniform(30, 70), 1),
            osts_total=64,
            osts_down=random.choices([0, 0, 0, 1, 2], weights=[60, 20, 10, 8, 2], k=1)[0],
        ),
        Filesystem(
            name="home", mount="/home", fs_type="nfs",
            total_tb=128.0,
            used_tb=round(random.uniform(70, 115), 1),
            inodes_used_pct=round(random.uniform(55, 85), 1),
            read_gbps=round(random.uniform(2, 8), 2),
            write_gbps=round(random.uniform(1, 5), 2),
        ),
        Filesystem(
            name="projects", mount="/projects", fs_type="beegfs",
            total_tb=1024.0,
            used_tb=round(random.uniform(600, 920), 1),
            inodes_used_pct=round(random.uniform(30, 60), 1),
            read_gbps=round(random.uniform(20, 55), 1),
            write_gbps=round(random.uniform(15, 40), 1),
            osts_total=32, osts_down=0,
        ),
        Filesystem(
            name="archive", mount="/archive", fs_type="gpfs",
            total_tb=4096.0,
            used_tb=round(random.uniform(2800, 3500), 1),
            inodes_used_pct=round(random.uniform(20, 40), 1),
            read_gbps=round(random.uniform(8, 20), 1),
            write_gbps=round(random.uniform(4, 12), 1),
        ),
    ]


def collect_infiniband() -> list[InfiniBandLink]:
    links: list[InfiniBandLink] = []
    for switch_idx in range(4):
        for port_idx in range(8):
            state = random.choices(
                ["Active", "Active", "Active", "Active", "Polling", "Down"],
                weights=[80, 8, 5, 3, 3, 1], k=1,
            )[0]
            links.append(InfiniBandLink(
                switch=f"ib-sw{switch_idx+1:02d}",
                port=f"{port_idx+1}/1",
                speed_gbps=200,
                state=state,
                error_count=random.choices(
                    [0, 0, 0, random.randint(1, 50), random.randint(100, 800)],
                    weights=[70, 15, 8, 5, 2], k=1,
                )[0],
            ))
    return links
