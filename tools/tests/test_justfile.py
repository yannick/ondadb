import os
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]

PUBLIC_RECIPES = (
    "fmt",
    "lint-safe",
    "lint-fast",
    "test-safe",
    "test-fast",
    "check",
    "metrics",
    "metrics-check",
    "metrics-record",
    "metrics-baseline",
    "coverage",
    "coverage-open",
    "hotspots",
    "bench-onda",
    "bench-phase",
    "bench-put",
    "bench-get",
    "bench-forward",
    "bench-backward",
    "bench-delete",
    "bench-vlog",
    "bench-cf",
    "bench-suite",
    "bench-graphs",
    "bench-matrix",
    "tools-check",
    "tools-install",
)


def run_just(
    *arguments: str, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[bytes]:
    env = os.environ.copy()
    if environment is not None:
        env.update(environment)
    return subprocess.run(
        ("just", *arguments),
        cwd=ROOT,
        check=True,
        capture_output=True,
        env=env,
    )


class JustfileHelpTests(unittest.TestCase):
    def test_default_help_is_colored_and_grouped(self):
        completed = run_just("--color", "always")

        self.assertIn(b"\x1b[", completed.stdout)
        for heading in (b"Quality", b"Metrics", b"Benchmarks", b"Setup"):
            self.assertIn(heading, completed.stdout)
        for description in (
            b"Format Rust source",
            b"Generate quality metrics",
            b"Run the standalone ondaDB benchmark",
            b"Report required tool versions",
        ):
            self.assertIn(description, completed.stdout)

    def test_list_shows_parameters_but_hides_private_helpers(self):
        completed = run_just("--list")

        self.assertIn(b"metrics-record label", completed.stdout)
        self.assertIn(b"bench-phase phase", completed.stdout)
        self.assertNotIn(b"_require-bench-harness", completed.stdout)

    def test_summary_includes_each_public_recipe(self):
        completed = run_just("--summary")
        recipes = set(completed.stdout.decode().split())

        self.assertTrue(set(PUBLIC_RECIPES).issubset(recipes))

    def test_bench_harness_location_can_be_overridden(self):
        run_just(
            "_require-bench-harness",
            "sh",
            environment={"ONDADB_BENCH_DIR": "/bin"},
        )
