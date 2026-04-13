"""Tests for the filesystem parsers.

Run with: python3 -m unittest tests.test_filesystem_parsers
Or all tests: python3 -m unittest discover
"""

import unittest
from pathlib import Path

from hpc_monitor.collectors.filesystems import (
    parse_beegfs_df,
    parse_df_ht,
    parse_lfs_df,
    parse_size_to_tb,
)


FIXTURES = Path(__file__).parent.parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


class SizeParserTests(unittest.TestCase):
    """parse_size_to_tb handles every unit suffix the real tools emit."""

    def test_terabyte_shorthand(self):
        self.assertAlmostEqual(parse_size_to_tb("32.0T"), 32.0, places=4)

    def test_tebibyte_iec(self):
        self.assertAlmostEqual(parse_size_to_tb("32.0TiB"), 32.0, places=4)

    def test_gigabyte_converts_down(self):
        self.assertAlmostEqual(parse_size_to_tb("500G"), 500 / 1024, places=4)

    def test_megabyte_is_tiny(self):
        self.assertAlmostEqual(parse_size_to_tb("1024M"), 1 / 1024, places=4)

    def test_petabyte_scales_up(self):
        self.assertAlmostEqual(parse_size_to_tb("2P"), 2 * 1024, places=4)

    def test_decimal_values(self):
        self.assertAlmostEqual(parse_size_to_tb("1.8T"), 1.8, places=4)

    def test_whitespace_tolerated(self):
        self.assertAlmostEqual(parse_size_to_tb("  64.0T  "), 64.0, places=4)

    def test_empty_string(self):
        self.assertEqual(parse_size_to_tb(""), 0.0)

    def test_unparseable(self):
        self.assertEqual(parse_size_to_tb("not a size"), 0.0)

    def test_unknown_unit(self):
        self.assertEqual(parse_size_to_tb("32.0Q"), 0.0)


class LustreParserTests(unittest.TestCase):
    """parse_lfs_df extracts one Filesystem per mount, counts OSTs, flags
    inactive ones."""

    def test_scratch_summary_capacity(self):
        filesystems = parse_lfs_df(load_fixture("lfs_df.txt"))
        self.assertEqual(len(filesystems), 1)
        fs = filesystems[0]
        self.assertEqual(fs.name, "scratch")
        self.assertEqual(fs.mount, "/scratch")
        self.assertEqual(fs.fs_type, "lustre")
        self.assertAlmostEqual(fs.total_tb, 320.0, places=1)
        self.assertAlmostEqual(fs.used_tb, 224.6, places=1)

    def test_scratch_ost_counts(self):
        filesystems = parse_lfs_df(load_fixture("lfs_df.txt"))
        fs = filesystems[0]
        # Fixture has 10 OSTs (0-9), one inactive (OST0007)
        self.assertEqual(fs.osts_total, 10)
        self.assertEqual(fs.osts_down, 1)

    def test_healthy_fixture_no_inactive(self):
        filesystems = parse_lfs_df(load_fixture("lfs_df_healthy.txt"))
        self.assertEqual(len(filesystems), 1)
        fs = filesystems[0]
        self.assertEqual(fs.name, "work")
        self.assertEqual(fs.osts_total, 4)
        self.assertEqual(fs.osts_down, 0)
        self.assertAlmostEqual(fs.total_tb, 64.0, places=1)

    def test_empty_input(self):
        self.assertEqual(parse_lfs_df(""), [])

    def test_only_osts_no_summary_dropped(self):
        """If we see OST lines but no summary line, drop the filesystem
        rather than emit bogus zeros."""
        truncated = (
            "UUID                       bytes        Used   Available Use%\n"
            "scratch-OST0000_UUID       32.0T       24.5T        7.5T  77% /scratch[OST:0]\n"
        )
        self.assertEqual(parse_lfs_df(truncated), [])


class DfHtParserTests(unittest.TestCase):
    """parse_df_ht keeps NFS/ext4/xfs/btrfs/zfs, skips tmpfs/devtmpfs."""

    def test_keeps_only_interesting_types(self):
        filesystems = parse_df_ht(load_fixture("df_ht.txt"))
        # Fixture has: rhel-root(xfs, 50G), devtmpfs, 3x tmpfs, sda1(xfs, 1G),
        # nfs /home (128T), nfs /apps (50T). The two xfs volumes are below
        # the 1 TB floor and get filtered, leaving just the two NFS mounts.
        types = [f.fs_type for f in filesystems]
        self.assertEqual(sorted(types), ["nfs", "nfs"])

    def test_nfs_home_capacity(self):
        filesystems = parse_df_ht(load_fixture("df_ht.txt"))
        home = next(f for f in filesystems if f.mount == "/home")
        self.assertEqual(home.fs_type, "nfs")
        self.assertAlmostEqual(home.total_tb, 128.0, places=1)
        self.assertAlmostEqual(home.used_tb, 94.0, places=1)

    def test_nfs_apps_capacity(self):
        filesystems = parse_df_ht(load_fixture("df_ht.txt"))
        apps = next(f for f in filesystems if f.mount == "/apps")
        self.assertAlmostEqual(apps.total_tb, 50.0, places=1)
        self.assertAlmostEqual(apps.used_tb, 38.0, places=1)

    def test_tmpfs_filtered_out(self):
        filesystems = parse_df_ht(load_fixture("df_ht.txt"))
        self.assertFalse(any(f.fs_type == "tmpfs" for f in filesystems))
        self.assertFalse(any(f.fs_type == "devtmpfs" for f in filesystems))

    def test_root_xfs_filtered_as_too_small(self):
        """Tiny local filesystems like / (50 GB) are HPC monitoring noise
        and should be filtered out by the minimum size threshold."""
        filesystems = parse_df_ht(load_fixture("df_ht.txt"))
        self.assertIsNone(next((f for f in filesystems if f.mount == "/"), None))
        self.assertIsNone(next((f for f in filesystems if f.mount == "/boot"), None))

    def test_keeps_large_xfs(self):
        """A big xfs scratch filesystem should pass the size filter."""
        big_xfs = (
            "Filesystem   Type  Size  Used Avail Use% Mounted on\n"
            "/dev/sdb1    xfs    40T   28T   12T  70% /scratch_local\n"
        )
        filesystems = parse_df_ht(big_xfs)
        self.assertEqual(len(filesystems), 1)
        self.assertEqual(filesystems[0].fs_type, "xfs")
        self.assertAlmostEqual(filesystems[0].total_tb, 40.0, places=1)

    def test_empty_input(self):
        self.assertEqual(parse_df_ht(""), [])

    def test_header_only(self):
        header = "Filesystem  Type  Size  Used  Avail  Use%  Mounted on"
        self.assertEqual(parse_df_ht(header), [])


class BeegfsParserTests(unittest.TestCase):
    """parse_beegfs_df aggregates storage targets, ignores metadata."""

    def test_aggregate_capacity(self):
        filesystems = parse_beegfs_df(load_fixture("beegfs_df.txt"))
        self.assertEqual(len(filesystems), 1)
        fs = filesystems[0]
        # 8 storage targets × 64 TiB = 512 TiB
        self.assertAlmostEqual(fs.total_tb, 512.0, places=0)

    def test_metadata_section_not_included_in_storage(self):
        filesystems = parse_beegfs_df(load_fixture("beegfs_df.txt"))
        fs = filesystems[0]
        # Storage count should be 8, not 10 (which would include the 2 MDTs)
        self.assertEqual(fs.osts_total, 8)

    def test_used_computed_from_free(self):
        filesystems = parse_beegfs_df(load_fixture("beegfs_df.txt"))
        fs = filesystems[0]
        # Each storage target shows ~18-19 TiB free out of 64 TiB total,
        # so used is roughly 45 TiB per target × 8 ≈ 360 TiB
        self.assertGreater(fs.used_tb, 350)
        self.assertLess(fs.used_tb, 380)

    def test_fs_type_is_beegfs(self):
        filesystems = parse_beegfs_df(load_fixture("beegfs_df.txt"))
        self.assertEqual(filesystems[0].fs_type, "beegfs")

    def test_empty_input(self):
        self.assertEqual(parse_beegfs_df(""), [])

    def test_no_storage_section(self):
        """If only the metadata section is present, return nothing."""
        metadata_only = (
            "METADATA SERVERS:\n"
            "TargetID   Cap. Pool   Total   Free   %   ITotal   IFree   %\n"
            "========   =========   =====   ====   =   ======   =====   =\n"
            "       1   normal      1.8TiB  1.7TiB 94% 1932.7M  1845.2M 95%\n"
        )
        self.assertEqual(parse_beegfs_df(metadata_only), [])


if __name__ == "__main__":
    unittest.main()
