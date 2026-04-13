"""Tests for the slurm parser helpers.

Run with: python3 -m unittest tests.test_slurm_parsers
"""

import unittest

from hpc_monitor.collectors.slurm import (
    normalize_slurm_state,
    parse_sinfo_cores,
    parse_sinfo_output,
    parse_slurm_duration,
    parse_slurm_time,
    parse_squeue_output,
)


class StateNormalizationTests(unittest.TestCase):

    def test_basic_states(self):
        self.assertEqual(normalize_slurm_state("idle"), "idle")
        self.assertEqual(normalize_slurm_state("mixed"), "mixed")
        self.assertEqual(normalize_slurm_state("allocated"), "allocated")

    def test_abbreviations(self):
        self.assertEqual(normalize_slurm_state("mix"), "mixed")
        self.assertEqual(normalize_slurm_state("alloc"), "allocated")
        self.assertEqual(normalize_slurm_state("comp"), "allocated")
        self.assertEqual(normalize_slurm_state("drng"), "drain")

    def test_trailing_modifiers_stripped(self):
        self.assertEqual(normalize_slurm_state("alloc*"), "allocated")
        self.assertEqual(normalize_slurm_state("down*"), "down")
        self.assertEqual(normalize_slurm_state("idle~"), "idle")

    def test_compound_states_drain_wins(self):
        self.assertEqual(normalize_slurm_state("mixed+drain"), "drain")
        self.assertEqual(normalize_slurm_state("alloc+drain"), "drain")

    def test_failure_states(self):
        self.assertEqual(normalize_slurm_state("fail"), "down")
        self.assertEqual(normalize_slurm_state("fail*"), "down")
        self.assertEqual(normalize_slurm_state("unk"), "down")

    def test_maintenance_and_reservation(self):
        self.assertEqual(normalize_slurm_state("maint"), "maint")
        self.assertEqual(normalize_slurm_state("resv"), "maint")


class CoresParserTests(unittest.TestCase):

    def test_standard_format(self):
        self.assertEqual(parse_sinfo_cores("12/4/0/16"), (12, 16))
        self.assertEqual(parse_sinfo_cores("64/0/0/64"), (64, 64))

    def test_all_zero(self):
        self.assertEqual(parse_sinfo_cores("0/0/0/0"), (0, 0))

    def test_malformed_returns_zeros(self):
        self.assertEqual(parse_sinfo_cores("invalid"), (0, 0))
        self.assertEqual(parse_sinfo_cores("1/2/3"), (0, 0))
        self.assertEqual(parse_sinfo_cores(""), (0, 0))


class DurationParserTests(unittest.TestCase):

    def test_minutes_only(self):
        self.assertAlmostEqual(parse_slurm_duration("30"), 0.5, places=3)

    def test_minutes_seconds(self):
        # Slurm quirk: without days, "1:30" means 1 min 30 sec
        self.assertAlmostEqual(parse_slurm_duration("1:30"), 1.5 / 60, places=3)

    def test_hours_minutes_seconds(self):
        self.assertAlmostEqual(parse_slurm_duration("2:15:00"), 2.25, places=3)

    def test_days_hours_minutes_seconds(self):
        self.assertAlmostEqual(parse_slurm_duration("1-12:00:00"), 36.0, places=3)

    def test_days_hours_only(self):
        self.assertAlmostEqual(parse_slurm_duration("3-00"), 72.0, places=3)

    def test_unlimited(self):
        self.assertEqual(parse_slurm_duration("UNLIMITED"), 0.0)

    def test_empty(self):
        self.assertEqual(parse_slurm_duration(""), 0.0)


class TimeParserTests(unittest.TestCase):

    def test_valid_timestamp(self):
        t = parse_slurm_time("2026-04-12T08:30:45")
        self.assertIsNotNone(t)
        self.assertEqual(t.year, 2026)
        self.assertEqual(t.hour, 8)

    def test_na_returns_none(self):
        self.assertIsNone(parse_slurm_time("N/A"))
        self.assertIsNone(parse_slurm_time("Unknown"))
        self.assertIsNone(parse_slurm_time(""))


class SinfoOutputParserTests(unittest.TestCase):

    def test_single_node_line(self):
        line = "cn001|compute|allocated|48/0/0/48|384000|45.2|1024|gpu:4|none"
        nodes = parse_sinfo_output(line)
        self.assertEqual(len(nodes), 1)
        n = nodes[0]
        self.assertEqual(n.name, "cn001")
        self.assertEqual(n.partition, "compute")
        self.assertEqual(n.state, "allocated")
        self.assertEqual(n.cores_total, 48)
        self.assertEqual(n.cores_alloc, 48)
        self.assertEqual(n.mem_total_gb, 375)  # 384000 MB / 1024
        self.assertEqual(n.gpu_count, 4)

    def test_gres_with_type_suffix(self):
        line = "cn002|gpu|mixed|32/32/0/64|512000|20.0|4096|gpu:tesla:2|none"
        nodes = parse_sinfo_output(line)
        self.assertEqual(nodes[0].gpu_count, 2)

    def test_empty_reason_normalized(self):
        line = "cn003|compute|idle|0/48/0/48|384000|0.0|384000|(null)|none"
        nodes = parse_sinfo_output(line)
        self.assertEqual(nodes[0].reason, "")

    def test_short_line_skipped(self):
        self.assertEqual(parse_sinfo_output("not|enough|fields"), [])


class SqueueOutputParserTests(unittest.TestCase):

    def test_running_job(self):
        line = ("12345|alice|physics|compute|lammps_run|RUNNING|4|192|"
                "2026-04-12T08:00:00|2026-04-12T08:05:00|24:00:00|None")
        jobs = parse_squeue_output(line)
        self.assertEqual(len(jobs), 1)
        j = jobs[0]
        self.assertEqual(j.job_id, 12345)
        self.assertEqual(j.user, "alice")
        self.assertEqual(j.state, "RUNNING")
        self.assertEqual(j.nodes, 4)
        self.assertEqual(j.cores, 192)
        self.assertAlmostEqual(j.time_limit_hours, 24.0, places=2)

    def test_array_job_suffix_stripped(self):
        line = ("12345_4|alice|physics|compute|array_task|RUNNING|1|16|"
                "2026-04-12T08:00:00|2026-04-12T08:05:00|1:00:00|None")
        jobs = parse_squeue_output(line)
        self.assertEqual(jobs[0].job_id, 12345)

    def test_pending_job_has_no_start_time(self):
        line = ("99999|bob|astro|gpu|tf_train|PENDING|2|128|"
                "2026-04-12T08:00:00|N/A|12:00:00|Priority")
        jobs = parse_squeue_output(line)
        self.assertIsNone(jobs[0].start_time)
        self.assertEqual(jobs[0].reason, "Priority")

    def test_malformed_job_id_skipped(self):
        line = ("notanumber|alice|physics|compute|x|RUNNING|1|16|"
                "2026-04-12T08:00:00|2026-04-12T08:05:00|1:00:00|None")
        self.assertEqual(parse_squeue_output(line), [])


if __name__ == "__main__":
    unittest.main()
