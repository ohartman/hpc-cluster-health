"""Tests for the InfiniBand parsers.

Run with: python3 -m unittest tests.test_infiniband_parsers
"""

import unittest
from pathlib import Path

from hpc_monitor.collectors.infiniband import (
    merge_error_counts,
    normalize_ib_state,
    parse_ibdiagnet,
    parse_ibstat,
)
from hpc_monitor.models import InfiniBandLink


FIXTURES = Path(__file__).parent.parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


class StateNormalizationTests(unittest.TestCase):

    def test_active(self):
        self.assertEqual(normalize_ib_state("Active"), "Active")

    def test_down(self):
        self.assertEqual(normalize_ib_state("Down"), "Down")

    def test_init_becomes_polling(self):
        self.assertEqual(normalize_ib_state("Initializing"), "Polling")
        self.assertEqual(normalize_ib_state("Init"), "Polling")

    def test_armed_becomes_polling(self):
        self.assertEqual(normalize_ib_state("Armed"), "Polling")

    def test_polling_stays_polling(self):
        self.assertEqual(normalize_ib_state("Polling"), "Polling")

    def test_unknown_defaults_to_down(self):
        self.assertEqual(normalize_ib_state("something weird"), "Down")

    def test_whitespace_stripped(self):
        self.assertEqual(normalize_ib_state("  Active  "), "Active")


class IbstatParserTests(unittest.TestCase):

    def test_counts_all_ports_across_hcas(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        # Fixture has: mlx5_0 (1 port), mlx5_1 (1 port), mlx5_2 (2 ports),
        # mlx4_0 (1 port) = 5 total.
        self.assertEqual(len(links), 5)

    def test_active_ports_parsed(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        active = [l for l in links if l.state == "Active"]
        self.assertEqual(len(active), 2)
        # mlx5_0 and mlx5_1 are the active ones
        active_names = sorted(l.switch for l in active)
        self.assertEqual(active_names, ["mlx5_0", "mlx5_1"])

    def test_down_ports_parsed(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        down = [l for l in links if l.state == "Down"]
        # mlx5_2/Port1 (Down) and mlx4_0/Port1 (Down)
        self.assertEqual(len(down), 2)

    def test_initializing_normalized_to_polling(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        polling = [l for l in links if l.state == "Polling"]
        # mlx5_2/Port2 is in Initializing state
        self.assertEqual(len(polling), 1)
        self.assertEqual(polling[0].switch, "mlx5_2")
        self.assertEqual(polling[0].port, "2")

    def test_rate_parsed(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        # mlx5_0 is HDR 200 Gb/s
        mlx5_0 = next(l for l in links if l.switch == "mlx5_0")
        self.assertEqual(mlx5_0.speed_gbps, 200)
        # mlx5_2 is EDR 100 Gb/s
        mlx5_2 = next(l for l in links if l.switch == "mlx5_2" and l.port == "1")
        self.assertEqual(mlx5_2.speed_gbps, 100)
        # mlx4_0 is FDR 40 Gb/s (old hardware)
        mlx4_0 = next(l for l in links if l.switch == "mlx4_0")
        self.assertEqual(mlx4_0.speed_gbps, 40)

    def test_multiple_ports_per_hca(self):
        links = parse_ibstat(load_fixture("ibstat.txt"))
        mlx5_2_ports = [l for l in links if l.switch == "mlx5_2"]
        self.assertEqual(len(mlx5_2_ports), 2)
        port_numbers = sorted(l.port for l in mlx5_2_ports)
        self.assertEqual(port_numbers, ["1", "2"])

    def test_error_count_starts_zero(self):
        """ibstat doesn't report errors — that comes from ibdiagnet."""
        links = parse_ibstat(load_fixture("ibstat.txt"))
        self.assertTrue(all(l.error_count == 0 for l in links))

    def test_healthy_fixture_all_active(self):
        links = parse_ibstat(load_fixture("ibstat_healthy.txt"))
        self.assertEqual(len(links), 2)
        self.assertTrue(all(l.state == "Active" for l in links))
        self.assertTrue(all(l.speed_gbps == 200 for l in links))

    def test_empty_input(self):
        self.assertEqual(parse_ibstat(""), [])

    def test_ca_with_no_ports(self):
        """A CA block with no Port: subsection should emit nothing."""
        text = (
            "CA 'mlx5_empty'\n"
            "\tCA type: MT4123\n"
            "\tNumber of ports: 0\n"
        )
        self.assertEqual(parse_ibstat(text), [])


class IbdiagnetParserTests(unittest.TestCase):

    def test_parses_warning_lines(self):
        errors = parse_ibdiagnet(load_fixture("ibdiagnet.txt"))
        # Fixture has 5 -W- warning lines; some share ports so aggregation
        # will produce fewer unique entries.
        self.assertGreater(len(errors), 0)

    def test_sums_multiple_counters_per_port(self):
        """mlx5_2/U2 has both PortRcvErrors (58) and SymbolErrors (312)
        reported in separate warning lines. They should sum to 370."""
        errors = parse_ibdiagnet(load_fixture("ibdiagnet.txt"))
        self.assertEqual(errors.get("mlx5_2/U2"), 58 + 312)

    def test_individual_port_counts(self):
        errors = parse_ibdiagnet(load_fixture("ibdiagnet.txt"))
        self.assertEqual(errors.get("mlx5_0/U1"), 142)    # SymbolErrors
        self.assertEqual(errors.get("mlx5_1/U1"), 3)      # LinkDownedCounter

    def test_switch_endpoints_also_captured(self):
        """The last warning line is a switch-to-switch link, which should
        still parse even though we care more about HCA endpoints."""
        errors = parse_ibdiagnet(load_fixture("ibdiagnet.txt"))
        self.assertIn("spine01/P18", errors)

    def test_empty_input(self):
        self.assertEqual(parse_ibdiagnet(""), {})

    def test_no_warnings(self):
        """Output with only info lines and no warnings should return {}."""
        text = (
            "-I- Discovering ... 48 nodes discovered.\n"
            "-I- Links Check finished successfully\n"
            "-I- Port Counters finished\n"
        )
        self.assertEqual(parse_ibdiagnet(text), {})


class MergeErrorCountsTests(unittest.TestCase):

    def test_attributes_errors_to_correct_hca(self):
        links = [
            InfiniBandLink(switch="mlx5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
            InfiniBandLink(switch="mlx5_1", port="1", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        errors = {"mlx5_0/U1": 142, "mlx5_1/U1": 3}
        merged = merge_error_counts(links, errors)
        mlx5_0 = next(l for l in merged if l.switch == "mlx5_0")
        self.assertEqual(mlx5_0.error_count, 142)
        mlx5_1 = next(l for l in merged if l.switch == "mlx5_1")
        self.assertEqual(mlx5_1.error_count, 3)

    def test_ports_with_no_errors_stay_zero(self):
        links = [
            InfiniBandLink(switch="mlx5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
            InfiniBandLink(switch="mlx5_7", port="1", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        errors = {"mlx5_0/U1": 142}
        merged = merge_error_counts(links, errors)
        mlx5_7 = next(l for l in merged if l.switch == "mlx5_7")
        self.assertEqual(mlx5_7.error_count, 0)

    def test_errors_attributed_to_correct_port(self):
        """If mlx5_0 has errors on /U1 but not /U2, only port 1 gets them."""
        links = [
            InfiniBandLink(switch="mlx5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
            InfiniBandLink(switch="mlx5_0", port="2", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        errors = {"mlx5_0/U1": 50}
        merged = merge_error_counts(links, errors)
        port1 = next(l for l in merged if l.port == "1")
        port2 = next(l for l in merged if l.port == "2")
        self.assertEqual(port1.error_count, 50)
        self.assertEqual(port2.error_count, 0)

    def test_multiple_counter_types_sum_on_same_port(self):
        """If a port has both SymbolErrors and PortRcvErrors reported,
        they should sum into a single error count for that port."""
        links = [
            InfiniBandLink(switch="mlx5_2", port="2", speed_gbps=100,
                           state="Active", error_count=0),
        ]
        # parse_ibdiagnet already aggregates counter types per port, so
        # the input here represents what comes out of that parser
        errors = {"mlx5_2/U2": 58 + 312}
        merged = merge_error_counts(links, errors)
        self.assertEqual(merged[0].error_count, 370)

    def test_case_insensitive_matching(self):
        links = [
            InfiniBandLink(switch="MLX5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        errors = {"mlx5_0/U1": 100}
        merged = merge_error_counts(links, errors)
        self.assertEqual(merged[0].error_count, 100)

    def test_empty_errors_passthrough(self):
        links = [
            InfiniBandLink(switch="mlx5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        merged = merge_error_counts(links, {})
        self.assertEqual(merged[0].error_count, 0)

    def test_does_not_mutate_input(self):
        links = [
            InfiniBandLink(switch="mlx5_0", port="1", speed_gbps=200,
                           state="Active", error_count=0),
        ]
        errors = {"mlx5_0/U1": 100}
        merge_error_counts(links, errors)
        # Original list unchanged
        self.assertEqual(links[0].error_count, 0)


if __name__ == "__main__":
    unittest.main()
