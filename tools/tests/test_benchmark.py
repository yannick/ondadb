from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import math
from pathlib import Path
import subprocess
import tempfile
import unittest

from tools import benchmark


PUT_LINE = "Put                          100 ops    2.00 ms    50000 ops/sec\n"


class ParseOutputTests(unittest.TestCase):
    def test_parses_existing_harness_labels(self):
        parsed = benchmark.parse_output(
            PUT_LINE
            + "Get (cold)                   100 ops    1.00 ms    100000 ops/sec\n"
        )

        self.assertEqual(parsed["put"].ops, 100)
        self.assertEqual(parsed["put"].ops_per_second, 50000.0)
        self.assertEqual(parsed["get"].milliseconds, 1.0)

    def test_rejects_duplicate_phase_lines(self):
        with self.assertRaisesRegex(ValueError, "duplicate phase"):
            benchmark.parse_output(PUT_LINE + PUT_LINE)

    def test_rejects_non_positive_measurements(self):
        with self.assertRaisesRegex(ValueError, "positive"):
            benchmark.parse_output(
                "Put                          0 ops    2.00 ms    0 ops/sec\n"
            )

    def test_rejects_recognized_phase_without_throughput(self):
        with self.assertRaisesRegex(ValueError, "missing or malformed"):
            benchmark.parse_output("Put                          100 ops    2.00 ms\n")

    def test_rejects_non_finite_throughput(self):
        with self.assertRaisesRegex(ValueError, "finite"):
            benchmark.parse_output(
                "Put                          100 ops    2.00 ms    "
                + "9" * 400
                + " ops/sec\n"
            )


class SummaryTests(unittest.TestCase):
    def test_reports_median_and_relative_spread(self):
        summary = benchmark.summarize([100.0, 120.0, 80.0])

        self.assertEqual(summary["median"], 100.0)
        self.assertEqual(summary["minimum"], 80.0)
        self.assertEqual(summary["maximum"], 120.0)
        self.assertEqual(summary["relative_spread_percent"], 40.0)

    def test_rejects_empty_and_non_finite_samples(self):
        with self.assertRaisesRegex(ValueError, "at least one"):
            benchmark.summarize([])
        with self.assertRaisesRegex(ValueError, "finite"):
            benchmark.summarize([math.inf])


class CommandTests(unittest.TestCase):
    def test_phase_list_is_validated_and_deduplicated(self):
        self.assertEqual(benchmark.parse_phases("get,forward"), ("get", "forward"))
        with self.assertRaisesRegex(ValueError, "unknown phase"):
            benchmark.parse_phases("get,bogus")
        with self.assertRaisesRegex(ValueError, "duplicate phase"):
            benchmark.parse_phases("get,get")

    def test_uses_cargo_metadata_target_directory(self):
        path = benchmark.cargo_binary_path(
            {"target_directory": "/tmp/shared-cargo-target"}
        )

        self.assertEqual(path, Path("/tmp/shared-cargo-target/release/onda_bench"))

    def test_build_command_includes_complete_workload(self):
        workload = benchmark.Workload(
            runs=2,
            ops=100,
            threads=3,
            key_size=16,
            value_size=512,
            pattern="sequential",
            compression="zstd",
            batch=25,
            features="unsafe-fastpath",
            phases=("get", "forward"),
        )

        command = benchmark.build_command(
            Path("/tmp/onda_bench"), workload, Path("/tmp/db")
        )

        self.assertEqual(command[0], "/tmp/onda_bench")
        self.assertIn("get,forward", command)
        self.assertIn("/tmp/db", command)
        self.assertIn("512", command)
        self.assertIn("zstd", command)

    def test_rejects_missing_requested_phase(self):
        with self.assertRaisesRegex(ValueError, "missing requested phases: get"):
            benchmark.validate_phases({"put": object()}, ("put", "get"))


class ReportTests(unittest.TestCase):
    def test_writes_versioned_json_and_flat_csv(self):
        document = {
            "schema": "ondadb.benchmark.v1",
            "collected_at": "2026-08-15T12:00:00Z",
            "git": {"revision": "abc123", "dirty": False},
            "host": {"os": "Darwin", "architecture": "arm64", "cpu_count": 24},
            "rustc": "rustc 1.97.1",
            "workload": {
                "runs": 1,
                "ops": 100,
                "threads": 2,
                "key_size": 16,
                "value_size": 100,
                "pattern": "random",
                "compression": "none",
                "batch": 25,
                "features": "unsafe-fastpath",
                "phases": ["get"],
            },
            "runs": [
                {
                    "run": 1,
                    "stdout": "Get (cold)                   100 ops    1.00 ms    100000 ops/sec\n",
                    "stderr": "cache warmed\n",
                    "phases": {
                        "get": {
                            "ops": 100,
                            "milliseconds": 1.0,
                            "ops_per_second": 100000.0,
                        }
                    },
                }
            ],
            "summary": {
                "get": {
                    "median": 100000.0,
                    "minimum": 100000.0,
                    "maximum": 100000.0,
                    "relative_spread_percent": 0.0,
                }
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            json_path, csv_path = benchmark.write_reports(document, Path(directory))

            self.assertEqual(json_path.name, "onda-latest.json")
            self.assertIn('"schema": "ondadb.benchmark.v1"', json_path.read_text())
            with csv_path.open(newline="") as report:
                rows = list(csv.reader(report))
            self.assertEqual(
                rows[0],
                [
                    "schema",
                    "collected_at",
                    "git_revision",
                    "git_dirty",
                    "host_os",
                    "host_architecture",
                    "host_cpu_count",
                    "rustc",
                    "workload_runs",
                    "workload_ops",
                    "workload_threads",
                    "workload_key_size",
                    "workload_value_size",
                    "workload_pattern",
                    "workload_compression",
                    "workload_batch",
                    "workload_features",
                    "workload_phases",
                    "phase",
                    "run",
                    "ops",
                    "milliseconds",
                    "ops_per_second",
                    "stdout",
                    "stderr",
                ],
            )
            self.assertEqual(
                rows[1],
                [
                    "ondadb.benchmark.v1",
                    "2026-08-15T12:00:00Z",
                    "abc123",
                    "False",
                    "Darwin",
                    "arm64",
                    "24",
                    "rustc 1.97.1",
                    "1",
                    "100",
                    "2",
                    "16",
                    "100",
                    "random",
                    "none",
                    "25",
                    "unsafe-fastpath",
                    "get",
                    "get",
                    "1",
                    "100",
                    "1.0",
                    "100000.0",
                    "Get (cold)                   100 ops    1.00 ms    100000 ops/sec\n",
                    "cache warmed\n",
                ],
            )


class ExecutionTests(unittest.TestCase):
    def workload(self):
        return benchmark.Workload(
            runs=2,
            ops=100,
            threads=2,
            key_size=16,
            value_size=100,
            pattern="random",
            compression="none",
            batch=25,
            features="safe",
            phases=("get",),
        )

    def test_executes_each_run_and_requires_requested_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "fake-bench"
            executable.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' 'Get (cold)                   100 ops    1.00 ms    100000 ops/sec'\n"
                "printf '%s\\n' 'cache warmed' >&2\n"
            )
            executable.chmod(0o755)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                runs = benchmark.execute_runs(executable, self.workload(), root / "db")

            self.assertEqual(len(runs), 2)
            self.assertEqual(runs[0]["phases"]["get"]["ops_per_second"], 100000.0)
            self.assertEqual(
                runs[0]["stdout"],
                "Get (cold)                   100 ops    1.00 ms    100000 ops/sec\n",
            )
            self.assertEqual(runs[0]["stderr"], "cache warmed\n")

    def test_propagates_nonzero_benchmark_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "failing-bench"
            executable.write_text("#!/bin/sh\nexit 2\n")
            executable.chmod(0o755)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                with self.assertRaises(subprocess.CalledProcessError) as raised:
                    benchmark.execute_runs(executable, self.workload(), root / "db")

            self.assertEqual(raised.exception.returncode, 2)
            self.assertFalse((root / "db" / "run-1").exists())

    def test_positive_integer_argument_rejects_zero(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            benchmark.positive_int("0")

    def test_build_command_enables_only_requested_features(self):
        self.assertEqual(
            benchmark.cargo_build_command("safe"),
            ["cargo", "build", "--release", "--bin", "onda_bench"],
        )
        self.assertEqual(
            benchmark.cargo_build_command("unsafe-fastpath"),
            [
                "cargo",
                "build",
                "--release",
                "--features",
                "unsafe-fastpath",
                "--bin",
                "onda_bench",
            ],
        )


class DocumentTests(unittest.TestCase):
    def test_builds_summary_from_raw_runs(self):
        workload = benchmark.Workload(
            runs=2,
            ops=100,
            threads=2,
            key_size=16,
            value_size=100,
            pattern="random",
            compression="none",
            batch=25,
            features="safe",
            phases=("get",),
        )
        runs = [
            {
                "run": 1,
                "phases": {
                    "get": {
                        "ops": 100,
                        "milliseconds": 1.0,
                        "ops_per_second": 100000.0,
                    }
                },
            },
            {
                "run": 2,
                "phases": {
                    "get": {
                        "ops": 100,
                        "milliseconds": 2.0,
                        "ops_per_second": 50000.0,
                    }
                },
            },
        ]

        document = benchmark.make_document(
            workload,
            runs,
            collected_at="2026-08-15T12:00:00Z",
            git={"revision": "abc123", "dirty": False},
            host={"os": "Darwin", "architecture": "arm64", "cpu_count": 24},
            rustc="rustc 1.97.1",
        )

        self.assertEqual(document["schema"], "ondadb.benchmark.v1")
        self.assertEqual(document["workload"]["phases"], ["get"])
        self.assertEqual(document["summary"]["get"]["median"], 75000.0)


if __name__ == "__main__":
    unittest.main()
