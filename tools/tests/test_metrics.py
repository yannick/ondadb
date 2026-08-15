import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from tools import metrics


GEIGER = {
    "packages": [
        {
            "package": {
                "id": {"name": "ondadb", "version": "0.8.0"},
                "dependencies": [],
                "dev_dependencies": [],
                "build_dependencies": [],
            },
            "unsafety": {
                "used": {
                    "functions": {"safe": 10, "unsafe_": 1},
                    "exprs": {"safe": 20, "unsafe_": 2},
                    "item_impls": {"safe": 3, "unsafe_": 0},
                    "item_traits": {"safe": 1, "unsafe_": 0},
                    "methods": {"safe": 8, "unsafe_": 1},
                },
                "unused": {
                    "functions": {"safe": 0, "unsafe_": 1},
                    "exprs": {"safe": 0, "unsafe_": 3},
                    "item_impls": {"safe": 0, "unsafe_": 1},
                    "item_traits": {"safe": 0, "unsafe_": 0},
                    "methods": {"safe": 0, "unsafe_": 2},
                },
                "forbids_unsafe": False,
            },
        }
    ],
    "packages_without_metrics": [],
    "used_but_not_scanned_files": [],
}


def function(name, start, end, cyclomatic, cognitive):
    return {
        "name": name,
        "start_line": start,
        "end_line": end,
        "kind": "function",
        "spaces": [],
        "metrics": {
            "cyclomatic": {"value": cyclomatic},
            "cognitive": {"value": cognitive},
            "loc": {"lloc": end - start + 1},
        },
    }


BCA_PRODUCTION = [
    {
        "name": "src/db.rs",
        "start_line": 1,
        "end_line": 300,
        "kind": "unit",
        "spaces": [
            function("open", 10, 20, 2, 3),
            {
                "name": "DbInner",
                "start_line": 50,
                "end_line": 270,
                "kind": "impl",
                "spaces": [function("compact", 60, 270, 18, 21)],
                "metrics": {},
            },
        ],
        "metrics": {"loc": {"sloc": 300}},
    }
]

BCA_TESTS = [
    {
        "name": "tests/db.rs",
        "start_line": 1,
        "end_line": 70,
        "kind": "unit",
        "spaces": [function("round_trip", 5, 20, 1, 0)],
        "metrics": {"loc": {"sloc": 70}},
    }
]

METADATA = {
    "packages": [
        {"name": "ondadb", "id": "ondadb 0.8.0"},
        {"name": "alpha", "id": "alpha 1.0.0"},
        {"name": "beta", "id": "beta 1.0.0"},
        {"name": "gamma", "id": "gamma 1.0.0"},
    ],
    "resolve": {
        "root": "ondadb 0.8.0",
        "nodes": [
            {"id": "ondadb 0.8.0", "deps": [{"pkg": "alpha 1.0.0"}, {"pkg": "beta 1.0.0"}]},
            {"id": "alpha 1.0.0", "deps": [{"pkg": "gamma 1.0.0"}]},
            {"id": "beta 1.0.0", "deps": []},
            {"id": "gamma 1.0.0", "deps": []},
        ],
    },
}


SNAPSHOT = {
    "schema": "ondadb.metrics.v1",
    "collected_at": "2026-08-15T12:00:00Z",
    "git": {"revision": "0123456789abcdef0123456789abcdef01234567", "dirty": True},
    "host": {"os": "Darwin", "architecture": "arm64", "cpu_count": 24},
    "tools": {"bca": "2.1.0"},
    "complexity": {
        "cyclomatic": {"max": 18, "median": 10.0, "p90": 16.4},
        "cognitive": {"max": 21, "median": 12.0, "p90": 19.2},
        "function_lloc": {"max": 211, "median": 111.0, "p90": 191.0},
        "baseline_exceptions": 1,
    },
    "unsafe": {"functions": 2, "expressions": 5, "impls": 1, "traits": 0, "methods": 3},
    "long_functions": {"src/db.rs::DbInner::compact": 211},
    "source": {"production_lines": 300, "test_lines": 70},
    "dependencies": {"direct": 2, "transitive": 1, "duplicate_versions": 1},
    "binary": {
        "safe": {"file_bytes": 1000, "text_bytes": 600},
        "unsafe_fastpath": {"file_bytes": 1100, "text_bytes": 650},
    },
    "coverage": None,
}


class GeigerTests(unittest.TestCase):
    def test_uses_repository_local_target_instead_of_shared_cargo_artifacts(self):
        environment = metrics.geiger_environment(
            {"PATH": "/bin", "CARGO_TARGET_DIR": "/shared/target"},
            Path("target/metrics/cargo-geiger-target"),
        )

        self.assertEqual(environment["PATH"], "/bin")
        self.assertEqual(
            environment["CARGO_TARGET_DIR"],
            "target/metrics/cargo-geiger-target",
        )

    def test_normalizes_project_owned_unsafe_surface(self):
        self.assertEqual(
            metrics.normalize_geiger(GEIGER),
            {
                "functions": 2,
                "expressions": 5,
                "impls": 1,
                "traits": 0,
                "methods": 3,
            },
        )

    def test_rejects_missing_project_package(self):
        with self.assertRaisesRegex(metrics.MetricsError, "ondadb package"):
            metrics.normalize_geiger(
                {
                    "packages": [],
                    "packages_without_metrics": [],
                    "used_but_not_scanned_files": [],
                }
            )

    def test_rejects_missing_unsafety_category_instead_of_using_zero(self):
        malformed = json.loads(json.dumps(GEIGER))
        del malformed["packages"][0]["unsafety"]["used"]["methods"]

        with self.assertRaisesRegex(metrics.MetricsError, "used.methods"):
            metrics.normalize_geiger(malformed)

    def test_rejects_missing_scan_completeness_fields(self):
        for field in ("packages_without_metrics", "used_but_not_scanned_files"):
            with self.subTest(field=field):
                malformed = json.loads(json.dumps(GEIGER))
                del malformed[field]

                with self.assertRaisesRegex(metrics.MetricsError, field):
                    metrics.normalize_geiger(malformed)

    def test_rejects_project_package_without_metrics(self):
        incomplete = json.loads(json.dumps(GEIGER))
        incomplete["packages_without_metrics"] = [
            {"name": "ondadb", "version": "0.8.0", "source": {"Path": "file:///repo"}}
        ]

        with self.assertRaisesRegex(metrics.MetricsError, "ondadb.*without metrics"):
            metrics.normalize_geiger(incomplete)

    def test_rejects_repository_file_that_was_used_but_not_scanned(self):
        partial = json.loads(json.dumps(GEIGER))
        partial["used_but_not_scanned_files"] = [
            str(metrics.ROOT / "src" / "lib.rs"),
        ]

        with self.assertRaisesRegex(metrics.MetricsError, "src/lib.rs.*not scanned"):
            metrics.normalize_geiger(partial)

    def test_permits_dependency_owned_scan_gaps(self):
        dependency_gap = json.loads(json.dumps(GEIGER))
        dependency_gap["packages_without_metrics"] = [
            {"name": "dependency", "version": "1.0.0", "source": {"Registry": {}}}
        ]
        dependency_gap["used_but_not_scanned_files"] = [
            "/cargo/registry/src/dependency/build-helper.c",
        ]

        self.assertEqual(
            metrics.normalize_geiger(dependency_gap),
            {
                "functions": 2,
                "expressions": 5,
                "impls": 1,
                "traits": 0,
                "methods": 3,
            },
        )


class SnapshotSchemaTests(unittest.TestCase):
    def test_accepts_complete_schema(self):
        metrics.validate_snapshot(SNAPSHOT)

    def test_rejects_missing_nested_field(self):
        malformed = json.loads(json.dumps(SNAPSHOT))
        del malformed["git"]["revision"]

        with self.assertRaisesRegex(metrics.MetricsError, "git.revision"):
            metrics.validate_snapshot(malformed)

    def test_rejects_invalid_timestamp_empty_tools_and_impossible_coverage(self):
        malformed = json.loads(json.dumps(SNAPSHOT))
        malformed["collected_at"] = "sometime"
        with self.assertRaisesRegex(metrics.MetricsError, "collected_at"):
            metrics.validate_snapshot(malformed)

        malformed = json.loads(json.dumps(SNAPSHOT))
        malformed["tools"] = {}
        with self.assertRaisesRegex(metrics.MetricsError, "tools"):
            metrics.validate_snapshot(malformed)

        malformed = json.loads(json.dumps(SNAPSHOT))
        malformed["coverage"] = {
            category: {"count": 1, "covered": 1, "percent": 101.0}
            for category in ("lines", "functions", "regions")
        }
        with self.assertRaisesRegex(metrics.MetricsError, "percent"):
            metrics.validate_snapshot(malformed)


class BaselineSchemaTests(unittest.TestCase):
    def test_accepts_complete_baseline_and_rejects_unknown_schema(self):
        baseline = {
            "schema": "ondadb.metrics-baseline.v1",
            "unsafe": SNAPSHOT["unsafe"],
            "long_functions": SNAPSHOT["long_functions"],
        }
        metrics.validate_baseline(baseline)

        with self.assertRaisesRegex(metrics.MetricsError, "baseline schema"):
            metrics.validate_baseline({**baseline, "schema": "unknown"})


class UnsafeRatchetTests(unittest.TestCase):
    def test_added_unsafe_expression_fails(self):
        baseline = {
            "unsafe": {
                "functions": 1,
                "expressions": 2,
                "impls": 0,
                "traits": 0,
                "methods": 0,
            }
        }
        current = {"unsafe": {**baseline["unsafe"], "expressions": 3}}

        self.assertEqual(
            metrics.unsafe_regressions(current, baseline),
            ["expressions: 2 -> 3"],
        )

    def test_debt_reduction_passes(self):
        baseline = {"unsafe": {"functions": 2, "expressions": 5, "impls": 1,
                                "traits": 0, "methods": 3}}
        current = {"unsafe": {"functions": 1, "expressions": 4, "impls": 0,
                               "traits": 0, "methods": 2}}

        self.assertEqual(metrics.unsafe_regressions(current, baseline), [])

    def test_missing_baseline_category_is_an_error(self):
        current = {
            "unsafe": {
                "functions": 0,
                "expressions": 0,
                "impls": 0,
                "traits": 0,
                "methods": 1,
            }
        }
        with self.assertRaisesRegex(metrics.MetricsError, "unsafe.methods"):
            metrics.unsafe_regressions(
                current,
                {"unsafe": {key: 0 for key in current["unsafe"] if key != "methods"}},
            )


class LongFunctionRatchetTests(unittest.TestCase):
    def test_new_and_worsened_long_functions_fail(self):
        current = {
            "long_functions": {
                "src/db.rs::old": 205,
                "src/db.rs::new": 201,
            }
        }
        baseline = {"long_functions": {"src/db.rs::old": 203}}

        self.assertEqual(
            metrics.long_function_regressions(current, baseline),
            [
                "src/db.rs::new: new long function (201 lines)",
                "src/db.rs::old: 203 -> 205",
            ],
        )

    def test_reduced_or_removed_long_functions_pass(self):
        current = {"long_functions": {"src/db.rs::old": 202}}
        baseline = {
            "long_functions": {
                "src/db.rs::old": 203,
                "src/db.rs::removed": 250,
            }
        }

        self.assertEqual(metrics.long_function_regressions(current, baseline), [])


class DistributionTests(unittest.TestCase):
    def test_percentile_interpolates_and_handles_single_value(self):
        self.assertEqual(metrics.percentile([10.0], 0.9), 10.0)
        self.assertEqual(metrics.percentile([0.0, 10.0], 0.5), 5.0)
        self.assertEqual(metrics.percentile([0.0, 10.0], 0.9), 9.0)

    def test_percentile_rejects_empty_input(self):
        with self.assertRaisesRegex(metrics.MetricsError, "empty distribution"):
            metrics.percentile([], 0.9)


class BcaTests(unittest.TestCase):
    def test_check_command_disables_inline_suppressions_in_effective_config(self):
        command = metrics.bca_check_command(("--print-effective-config=json",))

        self.assertEqual(
            command,
            (
                "bca",
                "check",
                "--no-suppress",
                "--print-effective-config=json",
            ),
        )
        completed = subprocess.run(
            command,
            cwd=metrics.ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        effective = json.loads(completed.stdout)
        self.assertIs(effective["check"]["no_suppress"], True)

    def test_only_documented_bca_violation_status_is_a_gate_failure(self):
        for returncode, expected_error in (
            (1, metrics.ToolError),
            (2, metrics.GateError),
            (3, metrics.ToolError),
            (7, metrics.ToolError),
        ):
            with self.subTest(returncode=returncode), mock.patch(
                "tools.metrics.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=("bca", "check"),
                    returncode=returncode,
                ),
            ):
                with self.assertRaises(expected_error):
                    metrics._run_bca_check(())

    def test_bca_uses_aggregate_output_files_for_production_and_tests(self):
        output = Path("target/metrics/raw/bca.json")
        self.assertEqual(
            metrics.bca_metrics_command(output, tests=False),
            (
                "bca", "metrics", "--format", "json", "--pretty", "--metrics",
                "cyclomatic,cognitive,lloc", "--output", str(output),
            ),
        )
        self.assertEqual(
            metrics.bca_metrics_command(output, tests=True),
            (
                "bca", "metrics", "--no-config", "--paths", "tests",
                "--cyclomatic-count-try=false", "--format", "json", "--pretty",
                "--metrics", "cyclomatic,cognitive,lloc", "--output", str(output),
            ),
        )

    def test_counts_generated_bca_baseline_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".bca-baseline.toml"
            path.write_text(
                'version = 6\nentry = [{ path = "src/a.rs" }, { path = "src/b.rs" }]\n',
                encoding="utf-8",
            )

            self.assertEqual(metrics.count_bca_baseline_entries(path), 2)

    def test_missing_bca_baseline_is_not_reported_as_zero_exceptions(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / ".bca-baseline.toml"
            with self.assertRaisesRegex(metrics.MetricsError, "does not exist"):
                metrics.count_bca_baseline_entries(missing)

    def test_extracts_nested_functions_and_source_span_lengths(self):
        functions = metrics.extract_bca_functions(BCA_PRODUCTION)

        self.assertEqual(
            functions,
            [
                {
                    "path": "src/db.rs",
                    "symbol": "open",
                    "cyclomatic": 2,
                    "cognitive": 3,
                    "lloc": 11,
                },
                {
                    "path": "src/db.rs",
                    "symbol": "DbInner::compact",
                    "cyclomatic": 18,
                    "cognitive": 21,
                    "lloc": 211,
                },
            ],
        )

    def test_normalizes_complexity_long_functions_and_source_lines(self):
        normalized = metrics.normalize_bca(BCA_PRODUCTION, BCA_TESTS, baseline_exceptions=1)

        self.assertEqual(normalized, {
            "complexity": SNAPSHOT["complexity"],
            "long_functions": SNAPSHOT["long_functions"],
            "source": SNAPSHOT["source"],
        })

    def test_rejects_missing_function_metric(self):
        malformed = json.loads(json.dumps(BCA_PRODUCTION))
        del malformed[0]["spaces"][0]["metrics"]["cognitive"]

        with self.assertRaisesRegex(metrics.MetricsError, "cognitive"):
            metrics.extract_bca_functions(malformed)


class DependencyTests(unittest.TestCase):
    def test_counts_direct_and_indirect_transitive_dependencies(self):
        self.assertEqual(
            metrics.normalize_metadata(METADATA),
            {"direct": 2, "transitive": 1},
        )

    def test_rejects_metadata_without_resolve_graph(self):
        malformed = {**METADATA, "resolve": None}
        with self.assertRaisesRegex(metrics.MetricsError, "resolve"):
            metrics.normalize_metadata(malformed)

    def test_counts_unique_extra_duplicate_versions(self):
        tree = (
            "alpha v1.0.0\n"
            "└── onda v0.1.0 (/repo)\n\n"
            "alpha v2.0.0\n"
            "└── beta v1.0.0\n\n"
            "alpha v2.0.0\n"
            "└── gamma v1.0.0\n"
        )

        self.assertEqual(metrics.count_duplicate_versions(tree), 1)

    def test_rejects_nonempty_malformed_duplicate_tree(self):
        with self.assertRaisesRegex(metrics.MetricsError, "cargo tree"):
            metrics.count_duplicate_versions(
                "alpha v1.0.0\n"
                "malformed producer record\n"
                "alpha v2.0.0\n"
            )

    def test_empty_duplicate_tree_means_no_duplicates(self):
        self.assertEqual(metrics.count_duplicate_versions(""), 0)


class ProducerNormalizationTests(unittest.TestCase):
    def test_normalizes_cargo_bloat_totals(self):
        self.assertEqual(
            metrics.normalize_bloat({"file-size": 1000, "text-section-size": 600, "functions": []}),
            {"file_bytes": 1000, "text_bytes": 600},
        )

    def test_normalizes_llvm_coverage_totals(self):
        document = {
            "type": "llvm.coverage.json.export",
            "version": "2.0.1",
            "data": [{
                "totals": {
                    "lines": {"count": 10, "covered": 9, "percent": 90.0},
                    "functions": {"count": 4, "covered": 3, "percent": 75.0},
                    "regions": {"count": 20, "covered": 15, "percent": 75.0},
                }
            }],
        }

        self.assertEqual(
            metrics.normalize_coverage(document),
            {
                "lines": {"count": 10, "covered": 9, "percent": 90.0},
                "functions": {"count": 4, "covered": 3, "percent": 75.0},
                "regions": {"count": 20, "covered": 15, "percent": 75.0},
            },
        )

    def test_rejects_malformed_bloat_and_coverage_documents(self):
        with self.assertRaisesRegex(metrics.MetricsError, "text-section-size"):
            metrics.normalize_bloat({"file-size": 1000})
        with self.assertRaisesRegex(metrics.MetricsError, "coverage.*regions"):
            metrics.normalize_coverage({"data": [{"totals": {
                "lines": {"count": 1, "covered": 1, "percent": 100.0},
                "functions": {"count": 1, "covered": 1, "percent": 100.0},
            }}]})


class ToolDiagnosticTests(unittest.TestCase):
    def test_cargo_plugins_use_cargo_subcommands_for_versions(self):
        self.assertEqual(
            metrics.TOOL_VERSION_COMMANDS["cargo-llvm-cov"],
            ("cargo", "llvm-cov", "--version"),
        )
        self.assertEqual(
            metrics.TOOL_VERSION_COMMANDS["cargo-bloat"],
            ("cargo", "bloat", "--version"),
        )

    def test_missing_tool_has_pinned_install_hint(self):
        with mock.patch("tools.metrics.shutil.which", return_value=None), mock.patch(
            "tools.metrics.subprocess.run"
        ) as run:
            results = metrics.inspect_tools()

        self.assertEqual(
            results[0]["message"],
            "bca: missing (expected 2.1.0); install with: "
            "cargo install --locked big-code-analysis-cli@2.1.0",
        )
        run.assert_not_called()

    def test_wrong_version_is_reported_without_installing(self):
        completed = subprocess.CompletedProcess(
            args=("bca", "--version"), returncode=0, stdout="bca 1.9.0\n", stderr=""
        )
        with mock.patch("tools.metrics.shutil.which", return_value="/tmp/bca"), mock.patch(
            "tools.metrics.subprocess.run", return_value=completed
        ) as run:
            results = metrics.inspect_tools(names=("bca",))

        self.assertEqual(results[0]["state"], "wrong-version")
        self.assertEqual(results[0]["actual"], "1.9.0")
        self.assertEqual(
            results[0]["message"],
            "bca: wrong version 1.9.0 (expected 2.1.0); reinstall with: "
            "cargo install --locked big-code-analysis-cli@2.1.0",
        )
        self.assertNotIn("install", run.call_args.args[0])

    def test_expected_version_passes(self):
        completed = subprocess.CompletedProcess(
            args=("bca", "--version"), returncode=0, stdout="bca 2.1.0\n", stderr=""
        )
        with mock.patch("tools.metrics.shutil.which", return_value="/tmp/bca"), mock.patch(
            "tools.metrics.subprocess.run", return_value=completed
        ):
            results = metrics.inspect_tools(names=("bca",))

        self.assertEqual(results[0]["state"], "ok")


class FileOperationTests(unittest.TestCase):
    def test_json_round_trip_and_atomic_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "value.json"
            metrics.atomic_write_json(path, {"value": 1})
            metrics.atomic_write_json(path, {"value": 2})

            self.assertEqual(metrics.load_json(path), {"value": 2})
            self.assertEqual(list(path.parent.glob("*.tmp")), [])

    def test_run_json_rejects_tool_failure_and_malformed_output(self):
        with self.assertRaisesRegex(metrics.ToolError, "exited 7"):
            metrics.run_json(("sh", "-c", "echo broken >&2; exit 7"))
        with self.assertRaisesRegex(metrics.MetricsError, "invalid JSON"):
            metrics.run_json(("sh", "-c", "printf nope"))

    def test_baseline_pair_rolls_back_if_second_publish_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bca = root / ".bca-baseline.toml"
            json_path = root / "baseline.json"
            candidate = root / ".candidate.toml"
            bca.write_text("old bca\n", encoding="utf-8")
            json_path.write_text('{"old": true}\n', encoding="utf-8")
            candidate.write_text("new bca\n", encoding="utf-8")
            real_replace = metrics.os.replace
            calls = 0

            def fail_second(source, destination):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated JSON publish failure")
                real_replace(source, destination)

            with mock.patch("tools.metrics.os.replace", side_effect=fail_second):
                with self.assertRaisesRegex(metrics.MetricsError, "publish baselines"):
                    metrics.publish_baseline_pair(
                        candidate,
                        {"new": True},
                        bca_destination=bca,
                        metrics_destination=json_path,
                    )

            self.assertEqual(bca.read_text(encoding="utf-8"), "old bca\n")
            self.assertEqual(json.loads(json_path.read_text()), {"old": True})

    def test_baseline_collection_failure_does_not_publish_bca_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bca = root / ".bca-baseline.toml"
            json_path = root / "baseline.json"
            bca.write_text("version = 6\nentry = []\n", encoding="utf-8")
            json_path.write_text('{"old": true}\n', encoding="utf-8")

            def write_candidate(arguments):
                Path(arguments[1]).write_text(
                    'version = 6\nentry = [{ path = "src/new.rs" }]\n',
                    encoding="utf-8",
                )

            with mock.patch.object(metrics, "BCA_BASELINE_PATH", bca), mock.patch.object(
                metrics, "BASELINE_PATH", json_path
            ), mock.patch("tools.metrics._ensure_tools", return_value={}), mock.patch(
                "tools.metrics._run_bca_check", side_effect=write_candidate
            ), mock.patch(
                "tools.metrics._deterministic_values",
                side_effect=metrics.ToolError("geiger failed"),
            ):
                with self.assertRaisesRegex(metrics.ToolError, "geiger failed"):
                    metrics.write_baselines()

            self.assertEqual(bca.read_text(encoding="utf-8"), "version = 6\nentry = []\n")
            self.assertEqual(json.loads(json_path.read_text()), {"old": True})

    def test_baseline_captures_git_provenance_before_candidate_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bca = root / ".bca-baseline.toml"
            json_path = root / "baseline.json"
            events = []

            def git_info():
                self.assertEqual(list(root.glob("*.candidate")), [])
                events.append("git")
                return {
                    "revision": "0123456789abcdef0123456789abcdef01234567",
                    "dirty": False,
                }

            def write_candidate(arguments):
                events.append("bca")
                Path(arguments[1]).write_text(
                    "version = 6\nentry = []\n",
                    encoding="utf-8",
                )

            published = {}

            def publish_candidate(candidate, document):
                published.update(document)
                candidate.unlink()

            with mock.patch.object(metrics, "ROOT", root), mock.patch.object(
                metrics, "BCA_BASELINE_PATH", bca
            ), mock.patch.object(
                metrics, "BASELINE_PATH", json_path
            ), mock.patch(
                "tools.metrics._ensure_tools", return_value={"bca": "2.1.0"}
            ), mock.patch(
                "tools.metrics._git_info", side_effect=git_info
            ), mock.patch(
                "tools.metrics._run_bca_check", side_effect=write_candidate
            ), mock.patch(
                "tools.metrics._deterministic_values",
                return_value={
                    "unsafe": {
                        "functions": 0,
                        "expressions": 0,
                        "impls": 0,
                        "traits": 0,
                        "methods": 0,
                    },
                    "long_functions": {},
                },
            ), mock.patch(
                "tools.metrics.publish_baseline_pair", side_effect=publish_candidate
            ):
                metrics.write_baselines()

            self.assertEqual(events, ["git", "bca"])
            self.assertIs(published["git"]["dirty"], False)


class HistoryTests(unittest.TestCase):
    def test_record_sanitizes_label_and_creates_expected_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = metrics.record_snapshot(
                SNAPSHOT,
                Path(directory),
                " Release Candidate! ",
                timestamp="20260815T120000Z",
            )

            self.assertEqual(
                path.name,
                "20260815T120000Z-0123456789ab-release-candidate.json",
            )
            self.assertEqual(metrics.load_json(path), SNAPSHOT)

    def test_record_never_overwrites_an_existing_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            first = metrics.record_snapshot(
                SNAPSHOT, Path(directory), "same", timestamp="20260815T120000Z"
            )
            with self.assertRaisesRegex(metrics.MetricsError, "already exists"):
                metrics.record_snapshot(
                    SNAPSHOT, Path(directory), "same", timestamp="20260815T120000Z"
                )
            self.assertEqual(metrics.load_json(first), SNAPSHOT)


class CoverageCommandTests(unittest.TestCase):
    def test_coverage_uses_repository_local_target_for_clean_and_reports(self):
        environment = metrics.coverage_environment(
            {"PATH": "/bin", "CARGO_TARGET_DIR": "/shared/target"},
            Path("target/metrics/llvm-cov-target"),
        )

        self.assertEqual(environment["PATH"], "/bin")
        self.assertEqual(
            environment["CARGO_TARGET_DIR"],
            "target/metrics/llvm-cov-target",
        )

    def test_coverage_update_records_totals_and_producer_version(self):
        snapshot = json.loads(json.dumps(SNAPSHOT))
        document = {
            "data": [{"totals": {
                "lines": {"count": 10, "covered": 9, "percent": 90.0},
                "functions": {"count": 4, "covered": 3, "percent": 75.0},
                "regions": {"count": 20, "covered": 15, "percent": 75.0},
            }}]
        }

        updated = metrics.attach_coverage(snapshot, document, "0.8.7")

        self.assertEqual(updated["coverage"]["lines"]["covered"], 9)
        self.assertEqual(updated["tools"]["cargo-llvm-cov"], "0.8.7")

    def test_merged_coverage_command_sequence_is_exact(self):
        self.assertEqual(
            metrics.coverage_commands(Path("target/metrics")),
            [
                ("cargo", "llvm-cov", "clean", "--workspace"),
                ("cargo", "llvm-cov", "--no-report"),
                ("cargo", "llvm-cov", "--no-report", "--features", "unsafe-fastpath"),
                (
                    "cargo", "llvm-cov", "report", "--json", "--output-path",
                    "target/metrics/raw/coverage.json",
                ),
                (
                    "cargo", "llvm-cov", "report", "--html", "--output-dir",
                    "target/metrics/coverage",
                ),
            ],
        )

    def _collect_with_current(self, current_document):
        temporary = tempfile.TemporaryDirectory(dir=metrics.ROOT / "target")
        self.addCleanup(temporary.cleanup)
        metrics_dir = Path(temporary.name)
        current_path = metrics_dir / "current.json"
        raw_dir = metrics_dir / "raw"
        if current_document is not None:
            metrics.atomic_write_json(current_path, current_document)

        fresh = json.loads(json.dumps(SNAPSHOT))
        fresh["collected_at"] = "2026-08-15T13:00:00Z"
        fresh["git"]["revision"] = "fedcba9876543210fedcba9876543210fedcba98"
        coverage_document = {
            "data": [{"totals": {
                "lines": {"count": 10, "covered": 9, "percent": 90.0},
                "functions": {"count": 4, "covered": 3, "percent": 75.0},
                "regions": {"count": 20, "covered": 15, "percent": 75.0},
            }}]
        }

        with mock.patch.object(metrics, "METRICS_DIR", metrics_dir), mock.patch.object(
            metrics, "RAW_DIR", raw_dir
        ), mock.patch.object(
            metrics, "CURRENT_PATH", current_path
        ), mock.patch(
            "tools.metrics._ensure_tools",
            return_value={"cargo-llvm-cov": "0.8.7"},
        ), mock.patch(
            "tools.metrics.collect_snapshot", return_value=fresh
        ) as collect, mock.patch(
            "tools.metrics.subprocess.run",
            return_value=subprocess.CompletedProcess(args=(), returncode=0),
        ), mock.patch(
            "tools.metrics.load_json", return_value=coverage_document
        ):
            metrics.collect_coverage()

        collect.assert_called_once_with(publish=False)
        return metrics.load_json(current_path)

    def test_coverage_creates_current_snapshot_when_it_is_missing(self):
        published = self._collect_with_current(None)

        self.assertEqual(
            published["git"]["revision"],
            "fedcba9876543210fedcba9876543210fedcba98",
        )
        self.assertEqual(published["coverage"]["lines"]["covered"], 9)

    def test_coverage_replaces_stale_snapshot_with_fresh_provenance(self):
        stale = json.loads(json.dumps(SNAPSHOT))
        stale["collected_at"] = "2026-08-15T11:00:00Z"
        published = self._collect_with_current(stale)

        self.assertEqual(published["collected_at"], "2026-08-15T13:00:00Z")
        self.assertNotEqual(published["git"], stale["git"])


if __name__ == "__main__":
    unittest.main()
