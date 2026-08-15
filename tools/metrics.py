#!/usr/bin/env python3
"""Collect and ratchet ondaDB's repository-native quality metrics."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from typing import NoReturn


SCHEMA = "ondadb.metrics.v1"
BASELINE_SCHEMA = "ondadb.metrics-baseline.v1"
LONG_FUNCTION_LIMIT = 200
ROOT = Path(__file__).resolve().parents[1]
METRICS_DIR = ROOT / "target" / "metrics"
RAW_DIR = METRICS_DIR / "raw"
CURRENT_PATH = METRICS_DIR / "current.json"
BASELINE_PATH = ROOT / "metrics" / "baseline.json"
HISTORY_DIR = ROOT / "metrics" / "history"
BCA_BASELINE_PATH = ROOT / ".bca-baseline.toml"
GEIGER_TARGET_DIR = ROOT / "target/metrics/cargo-geiger-target"

GEIGER_MIRROR_EXCLUDED_DIRECTORIES = {
    ".claude",
    ".git",
    ".superpowers",
    ".worktrees",
    "__pycache__",
    "target",
}

UNSAFE_CATEGORIES = {
    "functions": "functions",
    "expressions": "exprs",
    "impls": "item_impls",
    "traits": "item_traits",
    "methods": "methods",
}

TOOLS = {
    "bca": ("2.1.0", "cargo install --locked big-code-analysis-cli@2.1.0"),
    "cargo-llvm-cov": (
        "0.8.7",
        "cargo install --locked cargo-llvm-cov@0.8.7",
    ),
    "cargo-geiger": (
        "0.13.0",
        "cargo install --locked cargo-geiger@0.13.0",
    ),
    "cargo-bloat": (
        "0.12.1",
        "cargo install --locked cargo-bloat@0.12.1",
    ),
}

TOOL_VERSION_COMMANDS = {
    "bca": ("bca", "--version"),
    "cargo-llvm-cov": ("cargo", "llvm-cov", "--version"),
    "cargo-geiger": ("cargo-geiger", "--version"),
    "cargo-bloat": ("cargo", "bloat", "--version"),
}


class MetricsError(Exception):
    """A malformed producer document or invalid metrics operation."""


class ToolError(MetricsError):
    """An external analysis tool failed."""


class GateError(MetricsError):
    """A deterministic metric ratchet failed."""


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise MetricsError(f"{name} must be an object")
    return value


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise MetricsError(f"{name} must be an array")
    return value


def _required(mapping: dict[str, object], key: str, parent: str = "") -> object:
    name = f"{parent}.{key}" if parent else key
    if key not in mapping:
        raise MetricsError(f"missing {name}")
    return mapping[key]


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise MetricsError(f"{name} must be a non-negative integer")
    return value


def _number(value: object, name: str) -> float | int:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        raise MetricsError(f"{name} must be a non-negative finite number")
    return value


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise MetricsError(f"{name} must be a non-empty string")
    return value


def load_json(path: Path | str) -> object:
    """Load JSON without treating missing or malformed content as empty data."""
    source = Path(path)
    try:
        with source.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as error:
        raise MetricsError(f"JSON file does not exist: {source}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise MetricsError(f"cannot load JSON from {source}: {error}") from error


def atomic_write_json(path: Path | str, document: object) -> None:
    """Write deterministic JSON through a same-directory atomic replacement."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink()
            except FileNotFoundError:
                pass


def _atomic_write_text(path: Path, text: str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink()
            except FileNotFoundError:
                pass


def _run(
    command: Sequence[str], *, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            tuple(command),
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
    except OSError as error:
        raise ToolError(f"could not run {' '.join(command)}: {error}") from error
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        suffix = f": {detail}" if detail else ""
        raise ToolError(
            f"{' '.join(command)} exited {completed.returncode}{suffix}"
        )
    return completed


def run_json(
    command: Sequence[str],
    output_path: Path | str | None = None,
    *,
    environment: dict[str, str] | None = None,
) -> object:
    """Run a JSON producer, validate its output, and optionally retain it."""
    completed = _run(command, environment=environment)
    try:
        document = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise MetricsError(
            f"invalid JSON from {' '.join(command)}: {error.msg}"
        ) from error
    if output_path is not None:
        atomic_write_json(output_path, document)
    return document


def _run_text(command: Sequence[str], output_path: Path | None = None) -> str:
    completed = _run(command)
    if output_path is not None:
        _atomic_write_text(output_path, completed.stdout)
    return completed.stdout


def normalize_geiger(
    document: dict[str, object],
    package: str = "ondadb",
    *,
    repository_root: Path | None = None,
    allowed_unscanned_roots: Sequence[Path] | None = None,
) -> dict[str, int]:
    """Extract total project-owned unsafe items from cargo-geiger JSON."""
    packages_without_metrics = _array(
        _required(document, "packages_without_metrics"),
        "cargo-geiger packages_without_metrics",
    )
    for raw_package_id in packages_without_metrics:
        package_id = _mapping(
            raw_package_id,
            "cargo-geiger packages_without_metrics entry",
        )
        name = _string(
            _required(
                package_id,
                "name",
                "cargo-geiger packages_without_metrics entry",
            ),
            "cargo-geiger packages_without_metrics entry.name",
        )
        if name == package:
            raise MetricsError(f"cargo-geiger reports {package} without metrics")

    unscanned_files = _array(
        _required(document, "used_but_not_scanned_files"),
        "cargo-geiger used_but_not_scanned_files",
    )
    resolved_repository_root = (repository_root or ROOT).resolve()
    resolved_allowed_roots = tuple(
        path.resolve()
        for path in (
            allowed_unscanned_roots
            if allowed_unscanned_roots is not None
            else (GEIGER_TARGET_DIR,)
        )
    )
    for raw_path in unscanned_files:
        path_text = _string(
            raw_path,
            "cargo-geiger used_but_not_scanned_files entry",
        )
        path = Path(path_text)
        if not path.is_absolute():
            path = resolved_repository_root / path
        resolved_path = path.resolve()
        if any(
            resolved_path == allowed_root
            or resolved_path.is_relative_to(allowed_root)
            for allowed_root in resolved_allowed_roots
        ):
            continue
        try:
            project_path = resolved_path.relative_to(resolved_repository_root)
        except ValueError:
            continue
        raise MetricsError(
            f"cargo-geiger repository file {project_path} was used but not scanned"
        )

    packages = _array(_required(document, "packages"), "cargo-geiger packages")
    for entry_value in packages:
        entry = _mapping(entry_value, "cargo-geiger package entry")
        package_data = _mapping(
            _required(entry, "package", "cargo-geiger entry"),
            "cargo-geiger package",
        )
        package_id = _mapping(
            _required(package_data, "id", "cargo-geiger package"),
            "cargo-geiger package.id",
        )
        if package_id.get("name") != package:
            continue
        unsafety = _mapping(
            _required(entry, "unsafety", "cargo-geiger entry"),
            "cargo-geiger unsafety",
        )
        used = _mapping(
            _required(unsafety, "used", "cargo-geiger unsafety"),
            "cargo-geiger unsafety.used",
        )
        unused = _mapping(
            _required(unsafety, "unused", "cargo-geiger unsafety"),
            "cargo-geiger unsafety.unused",
        )
        normalized: dict[str, int] = {}
        for output_name, geiger_name in UNSAFE_CATEGORIES.items():
            used_count = _mapping(
                _required(used, geiger_name, "used"), f"used.{geiger_name}"
            )
            unused_count = _mapping(
                _required(unused, geiger_name, "unused"), f"unused.{geiger_name}"
            )
            normalized[output_name] = _integer(
                _required(used_count, "unsafe_", f"used.{geiger_name}"),
                f"used.{geiger_name}.unsafe_",
            ) + _integer(
                _required(unused_count, "unsafe_", f"unused.{geiger_name}"),
                f"unused.{geiger_name}.unsafe_",
            )
        return normalized
    raise MetricsError(f"cargo-geiger output does not contain the {package} package")


def geiger_environment(
    base: dict[str, str], target_dir: Path
) -> dict[str, str]:
    """Isolate geiger from unrelated artifacts in a shared Cargo target."""
    environment = dict(base)
    environment["CARGO_TARGET_DIR"] = str(target_dir)
    return environment


def _geiger_mirror_ignore(source_root: Path):
    resolved_source_root = source_root.resolve()

    def ignored(directory: str, names: list[str]) -> set[str]:
        excluded = {name for name in names if name == "__pycache__"}
        if Path(directory).resolve() != resolved_source_root:
            return excluded
        excluded.update(
            name
            for name in names
            if name in GEIGER_MIRROR_EXCLUDED_DIRECTORIES
            or name.startswith("target")
        )
        return excluded

    return ignored


@contextmanager
def geiger_project_mirror(
    source_root: Path,
    *,
    temporary_parent: Path | None = None,
):
    """Yield a fresh project mirror without repository administration trees."""
    temporary_root: Path | None = None
    try:
        temporary_root = Path(
            tempfile.mkdtemp(
                prefix="ondadb-cargo-geiger-",
                dir=temporary_parent,
            )
        )
        mirror = temporary_root / "project"
        shutil.copytree(
            source_root,
            mirror,
            symlinks=True,
            ignore=_geiger_mirror_ignore(source_root),
        )
    except OSError as error:
        cleanup_error: OSError | None = None
        if temporary_root is not None:
            try:
                shutil.rmtree(temporary_root)
            except OSError as remove_error:
                cleanup_error = remove_error
        cleanup_detail = (
            f"; cleanup also failed for {temporary_root}: {cleanup_error}"
            if cleanup_error is not None
            else ""
        )
        raise ToolError(
            f"could not create isolated cargo-geiger project mirror: "
            f"{error}{cleanup_detail}"
        ) from error

    producer_error: BaseException | None = None
    try:
        yield mirror
    except BaseException as error:
        producer_error = error
        raise
    finally:
        try:
            shutil.rmtree(temporary_root)
        except OSError as error:
            if producer_error is not None:
                raise ToolError(
                    f"cargo-geiger producer failed: {producer_error}; cleanup "
                    f"also failed for {temporary_root}: {error}"
                ) from error
            raise ToolError(
                f"could not remove isolated cargo-geiger project mirror "
                f"{temporary_root}: {error}"
            ) from error


def geiger_command(project_mirror: Path) -> tuple[str, ...]:
    """Build the pinned Geiger invocation against an explicit mirror manifest."""
    return (
        "cargo-geiger",
        "--manifest-path",
        str(project_mirror / "Cargo.toml"),
        "--features",
        "unsafe-fastpath",
        "--output-format",
        "Json",
        "--quiet",
    )


def collect_geiger_document(
    *,
    source_root: Path,
    output_path: Path,
    target_dir: Path,
) -> dict[str, object]:
    """Run Geiger against a temporary mirror and validate it before cleanup."""
    with geiger_project_mirror(source_root) as project_mirror:
        geiger_raw = run_json(
            geiger_command(project_mirror),
            output_path,
            environment=geiger_environment(dict(os.environ), target_dir),
        )
        geiger = _mapping(geiger_raw, "cargo-geiger document")
        normalize_geiger(
            geiger,
            repository_root=project_mirror,
            allowed_unscanned_roots=(target_dir,),
        )
        return geiger


def unsafe_regressions(
    current: dict[str, object], baseline: dict[str, object]
) -> list[str]:
    """Return unsafe categories whose current count exceeds the baseline."""
    current_unsafe = _mapping(current.get("unsafe"), "unsafe")
    baseline_unsafe = _mapping(baseline.get("unsafe"), "unsafe baseline")
    regressions: list[str] = []
    for category in UNSAFE_CATEGORIES:
        current_value = _integer(current_unsafe.get(category), f"unsafe.{category}")
        baseline_value = _integer(
            baseline_unsafe.get(category), f"unsafe.{category} baseline"
        )
        if current_value > baseline_value:
            regressions.append(f"{category}: {baseline_value} -> {current_value}")
    return regressions


def long_function_regressions(
    current: dict[str, object], baseline: dict[str, object]
) -> list[str]:
    """Return new and worsened functions above the logical-length limit."""
    current_values = _mapping(current.get("long_functions"), "long_functions")
    baseline_values = _mapping(
        baseline.get("long_functions"), "long_functions baseline"
    )
    regressions: list[str] = []
    for name in sorted(current_values):
        value = _integer(current_values[name], f"long_functions.{name}")
        if name not in baseline_values:
            regressions.append(f"{name}: new long function ({value} lines)")
            continue
        old_value = _integer(
            baseline_values[name], f"long_functions.{name} baseline"
        )
        if value > old_value:
            regressions.append(f"{name}: {old_value} -> {value}")
    return regressions


def percentile(values: Iterable[float], fraction: float) -> float:
    """Calculate a linearly interpolated percentile."""
    samples = sorted(float(value) for value in values)
    if not samples:
        raise MetricsError("cannot calculate percentile of an empty distribution")
    if not 0.0 <= fraction <= 1.0 or any(
        not math.isfinite(value) for value in samples
    ):
        raise MetricsError("percentile inputs must be finite and within range")
    if len(samples) == 1:
        return samples[0]
    position = (len(samples) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return samples[lower]
    weight = position - lower
    return samples[lower] * (1.0 - weight) + samples[upper] * weight


def _walk_bca_spaces(
    spaces: list[object], path: str, parents: tuple[str, ...]
) -> list[dict[str, object]]:
    functions: list[dict[str, object]] = []
    for raw_space in spaces:
        space = _mapping(raw_space, f"BCA space in {path}")
        name = _string(_required(space, "name", f"BCA space in {path}"), "BCA name")
        kind = _string(_required(space, "kind", f"BCA space {name}"), "BCA kind")
        nested = _array(
            _required(space, "spaces", f"BCA space {name}"),
            f"BCA {name}.spaces",
        )
        qualified = (*parents, name)
        if kind == "function":
            start = _integer(
                _required(space, "start_line", f"BCA function {name}"),
                f"BCA function {name}.start_line",
            )
            end = _integer(
                _required(space, "end_line", f"BCA function {name}"),
                f"BCA function {name}.end_line",
            )
            if start == 0 or end < start:
                raise MetricsError(f"BCA function {name} has an invalid source span")
            function_metrics = _mapping(
                _required(space, "metrics", f"BCA function {name}"),
                f"BCA function {name}.metrics",
            )
            cyclomatic = _mapping(
                _required(function_metrics, "cyclomatic", f"BCA function {name}.metrics"),
                f"BCA function {name}.cyclomatic",
            )
            cognitive = _mapping(
                _required(function_metrics, "cognitive", f"BCA function {name}.metrics"),
                f"BCA function {name}.cognitive",
            )
            functions.append(
                {
                    "path": path,
                    "symbol": "::".join(qualified),
                    "cyclomatic": _integer(
                        _required(cyclomatic, "value", f"BCA function {name}.cyclomatic"),
                        f"BCA function {name}.cyclomatic.value",
                    ),
                    "cognitive": _integer(
                        _required(cognitive, "value", f"BCA function {name}.cognitive"),
                        f"BCA function {name}.cognitive.value",
                    ),
                    "lloc": end - start + 1,
                }
            )
        functions.extend(_walk_bca_spaces(nested, path, qualified))
    return functions


def extract_bca_functions(document: object) -> list[dict[str, object]]:
    """Flatten BCA's nested spaces into stable qualified function records."""
    files = _array(document, "BCA document")
    functions: list[dict[str, object]] = []
    for raw_file in files:
        file_data = _mapping(raw_file, "BCA file")
        path = _string(_required(file_data, "name", "BCA file"), "BCA file.name")
        spaces = _array(_required(file_data, "spaces", path), f"{path}.spaces")
        functions.extend(_walk_bca_spaces(spaces, path, ()))
    return functions


def _source_lines(document: object, label: str) -> int:
    total = 0
    for raw_file in _array(document, f"BCA {label} document"):
        file_data = _mapping(raw_file, f"BCA {label} file")
        path = _string(_required(file_data, "name", f"BCA {label} file"), "BCA file.name")
        file_metrics = _mapping(
            _required(file_data, "metrics", path), f"BCA {path}.metrics"
        )
        loc = _mapping(_required(file_metrics, "loc", f"BCA {path}.metrics"), f"BCA {path}.loc")
        total += _integer(_required(loc, "sloc", f"BCA {path}.loc"), f"BCA {path}.loc.sloc")
    return total


def _distribution(values: list[int]) -> dict[str, float | int]:
    if not values:
        return {"max": 0, "median": 0, "p90": 0}
    return {
        "max": max(values),
        "median": round(percentile(values, 0.5), 2),
        "p90": round(percentile(values, 0.9), 2),
    }


def normalize_bca(
    production: object, tests: object, *, baseline_exceptions: int
) -> dict[str, object]:
    """Normalize BCA functions, long-function debt, and source totals."""
    exceptions = _integer(baseline_exceptions, "baseline_exceptions")
    functions = extract_bca_functions(production)
    long_functions: dict[str, int] = {}
    for function in functions:
        length = _integer(function["lloc"], "BCA function lloc")
        if length <= LONG_FUNCTION_LIMIT:
            continue
        identifier = f"{function['path']}::{function['symbol']}"
        if identifier in long_functions:
            raise MetricsError(f"BCA produced duplicate function identity {identifier}")
        long_functions[identifier] = length
    return {
        "complexity": {
            "cyclomatic": _distribution(
                [_integer(item["cyclomatic"], "cyclomatic") for item in functions]
            ),
            "cognitive": _distribution(
                [_integer(item["cognitive"], "cognitive") for item in functions]
            ),
            "function_lloc": _distribution(
                [_integer(item["lloc"], "function_lloc") for item in functions]
            ),
            "baseline_exceptions": exceptions,
        },
        "long_functions": dict(sorted(long_functions.items())),
        "source": {
            "production_lines": _source_lines(production, "production"),
            "test_lines": _source_lines(tests, "test"),
        },
    }


def normalize_metadata(document: dict[str, object], package: str = "ondadb") -> dict[str, int]:
    """Count active direct and indirect packages from Cargo metadata format 1."""
    packages = _array(_required(document, "packages", "cargo metadata"), "cargo metadata packages")
    project_ids: list[str] = []
    for raw_package in packages:
        package_data = _mapping(raw_package, "cargo metadata package")
        if package_data.get("name") == package:
            project_ids.append(_string(_required(package_data, "id", "cargo metadata package"), "cargo metadata package.id"))
    if len(project_ids) != 1:
        raise MetricsError(f"cargo metadata must contain exactly one {package} package")
    resolve = _mapping(_required(document, "resolve", "cargo metadata"), "cargo metadata resolve")
    nodes = _array(_required(resolve, "nodes", "cargo metadata resolve"), "cargo metadata resolve.nodes")
    graph: dict[str, set[str]] = {}
    for raw_node in nodes:
        node = _mapping(raw_node, "cargo metadata resolve node")
        node_id = _string(_required(node, "id", "cargo metadata node"), "cargo metadata node.id")
        dependencies: set[str] = set()
        for raw_dep in _array(_required(node, "deps", f"cargo metadata node {node_id}"), f"cargo metadata node {node_id}.deps"):
            dep = _mapping(raw_dep, f"cargo metadata dependency of {node_id}")
            dependencies.add(_string(_required(dep, "pkg", "cargo metadata dependency"), "cargo metadata dependency.pkg"))
        graph[node_id] = dependencies
    root = project_ids[0]
    if root not in graph:
        raise MetricsError(f"cargo metadata resolve graph does not contain {package}")
    direct = graph[root]
    reachable: set[str] = set()
    pending = list(direct)
    while pending:
        dependency = pending.pop()
        if dependency in reachable:
            continue
        if dependency not in graph:
            raise MetricsError(f"cargo metadata resolve graph is missing {dependency}")
        reachable.add(dependency)
        pending.extend(graph[dependency])
    return {"direct": len(direct), "transitive": len(reachable - direct)}


_TREE_PACKAGE = re.compile(r"^(\S+) v([^\s(]+)(?:\s|$)")


def count_duplicate_versions(tree: str) -> int:
    """Count extra unique versions among cargo tree duplicate roots."""
    versions: dict[str, set[str]] = {}
    for line in tree.splitlines():
        if not line:
            continue
        if line[0].isspace() or line.startswith(("├", "└", "│")):
            payload = line.lstrip(" \t│├└─")
            if _TREE_PACKAGE.match(payload) or re.fullmatch(
                r"\[(?:build-|dev-)?dependencies\]", payload
            ):
                continue
            raise MetricsError(f"malformed cargo tree row: {line}")
        match = _TREE_PACKAGE.match(line)
        if match is None:
            raise MetricsError(f"malformed cargo tree root: {line}")
        versions.setdefault(match.group(1), set()).add(match.group(2))
    if tree.strip() and not versions:
        raise MetricsError("cargo tree duplicate output contains no package roots")
    return sum(max(0, len(found) - 1) for found in versions.values())


def normalize_bloat(document: dict[str, object]) -> dict[str, int]:
    """Extract cargo-bloat's whole-file and text-section byte totals."""
    return {
        "file_bytes": _integer(_required(document, "file-size", "cargo-bloat"), "cargo-bloat file-size"),
        "text_bytes": _integer(
            _required(document, "text-section-size", "cargo-bloat"),
            "cargo-bloat text-section-size",
        ),
    }


def normalize_coverage(document: dict[str, object]) -> dict[str, object]:
    """Extract merged LLVM line, function, and region totals."""
    data = _array(_required(document, "data", "coverage"), "coverage data")
    if len(data) != 1:
        raise MetricsError("coverage data must contain exactly one aggregate")
    aggregate = _mapping(data[0], "coverage aggregate")
    totals = _mapping(_required(aggregate, "totals", "coverage aggregate"), "coverage totals")
    normalized: dict[str, object] = {}
    for category in ("lines", "functions", "regions"):
        values = _mapping(
            _required(totals, category, "coverage totals"),
            f"coverage totals.{category}",
        )
        count = _integer(_required(values, "count", f"coverage totals.{category}"), f"coverage totals.{category}.count")
        covered = _integer(_required(values, "covered", f"coverage totals.{category}"), f"coverage totals.{category}.covered")
        percent = _number(_required(values, "percent", f"coverage totals.{category}"), f"coverage totals.{category}.percent")
        if covered > count:
            raise MetricsError(f"coverage totals.{category}.covered exceeds count")
        if percent > 100:
            raise MetricsError(f"coverage totals.{category}.percent exceeds 100")
        normalized[category] = {"count": count, "covered": covered, "percent": percent}
    return normalized


def validate_snapshot(document: object) -> None:
    """Validate every required field in the normalized v1 snapshot."""
    snapshot = _mapping(document, "snapshot")
    schema = _string(_required(snapshot, "schema"), "schema")
    if schema != SCHEMA:
        raise MetricsError(f"schema must be {SCHEMA}")
    collected_at = _string(_required(snapshot, "collected_at"), "collected_at")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", collected_at):
        raise MetricsError("collected_at must use YYYY-MM-DDTHH:MM:SSZ")
    git = _mapping(_required(snapshot, "git"), "git")
    revision = _string(_required(git, "revision", "git"), "git.revision")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise MetricsError("git.revision must be a 40-character lowercase hexadecimal SHA")
    if not isinstance(_required(git, "dirty", "git"), bool):
        raise MetricsError("git.dirty must be a boolean")
    host = _mapping(_required(snapshot, "host"), "host")
    _string(_required(host, "os", "host"), "host.os")
    _string(_required(host, "architecture", "host"), "host.architecture")
    _integer(_required(host, "cpu_count", "host"), "host.cpu_count")
    tools = _mapping(_required(snapshot, "tools"), "tools")
    if not tools:
        raise MetricsError("tools must contain at least one producer version")
    for name, version in tools.items():
        _string(name, "tool name")
        _string(version, f"tools.{name}")
    complexity = _mapping(_required(snapshot, "complexity"), "complexity")
    for category in ("cyclomatic", "cognitive", "function_lloc"):
        distribution = _mapping(_required(complexity, category, "complexity"), f"complexity.{category}")
        for statistic in ("max", "median", "p90"):
            _number(_required(distribution, statistic, f"complexity.{category}"), f"complexity.{category}.{statistic}")
    _integer(_required(complexity, "baseline_exceptions", "complexity"), "complexity.baseline_exceptions")
    unsafe = _mapping(_required(snapshot, "unsafe"), "unsafe")
    for category in UNSAFE_CATEGORIES:
        _integer(_required(unsafe, category, "unsafe"), f"unsafe.{category}")
    long_functions = _mapping(_required(snapshot, "long_functions"), "long_functions")
    for name, length in long_functions.items():
        _string(name, "long function name")
        _integer(length, f"long_functions.{name}")
    for section, fields in (
        ("source", ("production_lines", "test_lines")),
        ("dependencies", ("direct", "transitive", "duplicate_versions")),
    ):
        values = _mapping(_required(snapshot, section), section)
        for field in fields:
            _integer(_required(values, field, section), f"{section}.{field}")
    binary = _mapping(_required(snapshot, "binary"), "binary")
    for feature in ("safe", "unsafe_fastpath"):
        values = _mapping(_required(binary, feature, "binary"), f"binary.{feature}")
        for field in ("file_bytes", "text_bytes"):
            _integer(_required(values, field, f"binary.{feature}"), f"binary.{feature}.{field}")
    coverage = _required(snapshot, "coverage")
    if coverage is not None:
        normalize_coverage({"data": [{"totals": coverage}]})


def validate_baseline(document: object) -> None:
    """Validate the deterministic fields consumed by project ratchets."""
    baseline = _mapping(document, "metrics baseline")
    schema = _string(_required(baseline, "schema", "baseline"), "baseline schema")
    if schema != BASELINE_SCHEMA:
        raise MetricsError(f"baseline schema must be {BASELINE_SCHEMA}")
    unsafe = _mapping(_required(baseline, "unsafe", "baseline"), "baseline unsafe")
    for category in UNSAFE_CATEGORIES:
        _integer(_required(unsafe, category, "baseline unsafe"), f"baseline unsafe.{category}")
    long_functions = _mapping(
        _required(baseline, "long_functions", "baseline"),
        "baseline long_functions",
    )
    for name, length in long_functions.items():
        _string(name, "baseline long function name")
        _integer(length, f"baseline long_functions.{name}")


def inspect_tools(names: Sequence[str] | None = None) -> list[dict[str, str]]:
    """Inspect required tools without installing or changing anything."""
    selected = tuple(names) if names is not None else tuple(TOOLS)
    results: list[dict[str, str]] = []
    for name in selected:
        if name not in TOOLS:
            raise MetricsError(f"unknown required tool {name}")
        expected, install = TOOLS[name]
        if shutil.which(name) is None:
            results.append(
                {
                    "name": name,
                    "state": "missing",
                    "actual": "-",
                    "expected": expected,
                    "install": install,
                    "message": f"{name}: missing (expected {expected}); install with: {install}",
                }
            )
            continue
        command = TOOL_VERSION_COMMANDS[name]
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        except OSError as error:
            actual = "unavailable"
            message = f"{name}: version check failed ({error}); reinstall with: {install}"
            results.append({"name": name, "state": "error", "actual": actual, "expected": expected, "install": install, "message": message})
            continue
        output = f"{completed.stdout}\n{completed.stderr}"
        match = re.search(r"\b\d+\.\d+\.\d+\b", output)
        actual = match.group(0) if match else "unknown"
        if completed.returncode != 0 or actual != expected:
            message = f"{name}: wrong version {actual} (expected {expected}); reinstall with: {install}"
            state = "wrong-version"
        else:
            message = f"{name}: {actual} (ok)"
            state = "ok"
        results.append({"name": name, "state": state, "actual": actual, "expected": expected, "install": install, "message": message})
    return results


def _ensure_tools(names: Sequence[str]) -> dict[str, str]:
    results = inspect_tools(names)
    failures = [result["message"] for result in results if result["state"] != "ok"]
    if failures:
        raise ToolError("required tools are unavailable:\n" + "\n".join(failures))
    return {result["name"]: result["actual"] for result in results}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_info() -> dict[str, object]:
    revision = _run_text(("git", "rev-parse", "HEAD")).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise MetricsError("git rev-parse did not return a full lowercase SHA")
    dirty = bool(_run_text(("git", "status", "--porcelain")).strip())
    return {"revision": revision, "dirty": dirty}


def _host_info() -> dict[str, object]:
    return {
        "os": platform.system() or "unknown",
        "architecture": platform.machine() or "unknown",
        "cpu_count": os.cpu_count() or 1,
    }


def count_bca_baseline_entries(path: Path = BCA_BASELINE_PATH) -> int:
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except FileNotFoundError:
        raise MetricsError(f"BCA baseline does not exist: {path}")
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise MetricsError(f"cannot parse BCA baseline {path}: {error}") from error
    entries = document.get("entry", [])
    if not isinstance(entries, list):
        raise MetricsError("BCA baseline 'entry' field must be an array")
    return len(entries)


def bca_metrics_command(output_path: Path, *, tests: bool) -> tuple[str, ...]:
    """Build BCA's aggregate-file command; stdout is per-file JSON streams."""
    if tests:
        return (
            "bca",
            "metrics",
            "--no-config",
            "--paths",
            "tests",
            "--cyclomatic-count-try=false",
            "--format",
            "json",
            "--pretty",
            "--metrics",
            "cyclomatic,cognitive,lloc",
            "--output",
            str(output_path),
        )
    return (
        "bca",
        "metrics",
        "--format",
        "json",
        "--pretty",
        "--metrics",
        "cyclomatic,cognitive,lloc",
        "--output",
        str(output_path),
    )


def _run_bca_metrics(output_path: Path, *, tests: bool) -> object:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _run(bca_metrics_command(output_path, tests=tests))
    return load_json(output_path)


def _producer_documents() -> tuple[object, object, dict[str, object]]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    production = _run_bca_metrics(
        RAW_DIR / "bca-production.json",
        tests=False,
    )
    tests = _run_bca_metrics(
        RAW_DIR / "bca-tests.json",
        tests=True,
    )
    geiger = collect_geiger_document(
        source_root=ROOT,
        output_path=RAW_DIR / "geiger.json",
        target_dir=GEIGER_TARGET_DIR.resolve(),
    )
    return production, tests, geiger


def _deterministic_values(
    bca_baseline_path: Path = BCA_BASELINE_PATH,
) -> dict[str, object]:
    production, tests, geiger = _producer_documents()
    bca = normalize_bca(
        production,
        tests,
        baseline_exceptions=count_bca_baseline_entries(bca_baseline_path),
    )
    return {
        "unsafe": normalize_geiger(geiger),
        "long_functions": bca["long_functions"],
    }


def collect_snapshot(*, publish: bool = True) -> dict[str, object]:
    """Collect trend metrics and optionally atomically publish the snapshot."""
    versions = _ensure_tools(("bca", "cargo-geiger", "cargo-bloat"))
    production, tests, geiger = _producer_documents()
    metadata_raw = run_json(
        ("cargo", "metadata", "--format-version", "1"),
        RAW_DIR / "cargo-metadata.json",
    )
    metadata = _mapping(metadata_raw, "cargo metadata document")
    duplicate_tree = _run_text(
        ("cargo", "tree", "--duplicates"), RAW_DIR / "cargo-tree-duplicates.txt"
    )
    safe_bloat_raw = run_json(
        (
            "cargo", "bloat", "--release", "--bin", "onda_bench",
            "--message-format", "json",
        ),
        RAW_DIR / "cargo-bloat-safe.json",
    )
    fast_bloat_raw = run_json(
        (
            "cargo", "bloat", "--release", "--bin", "onda_bench",
            "--features", "unsafe-fastpath", "--message-format", "json",
        ),
        RAW_DIR / "cargo-bloat-unsafe-fastpath.json",
    )
    bca = normalize_bca(
        production,
        tests,
        baseline_exceptions=count_bca_baseline_entries(),
    )
    dependencies = normalize_metadata(metadata)
    dependencies["duplicate_versions"] = count_duplicate_versions(duplicate_tree)
    snapshot: dict[str, object] = {
        "schema": SCHEMA,
        "collected_at": _utc_timestamp(),
        "git": _git_info(),
        "host": _host_info(),
        "tools": versions,
        "complexity": bca["complexity"],
        "unsafe": normalize_geiger(geiger),
        "long_functions": bca["long_functions"],
        "source": bca["source"],
        "dependencies": dependencies,
        "binary": {
            "safe": normalize_bloat(_mapping(safe_bloat_raw, "safe cargo-bloat document")),
            "unsafe_fastpath": normalize_bloat(_mapping(fast_bloat_raw, "unsafe-fastpath cargo-bloat document")),
        },
        "coverage": None,
    }
    validate_snapshot(snapshot)
    if publish:
        atomic_write_json(CURRENT_PATH, snapshot)
    return snapshot


def _print_snapshot(snapshot: dict[str, object]) -> None:
    complexity = _mapping(snapshot["complexity"], "complexity")
    source = _mapping(snapshot["source"], "source")
    dependencies = _mapping(snapshot["dependencies"], "dependencies")
    unsafe = _mapping(snapshot["unsafe"], "unsafe")
    binary = _mapping(snapshot["binary"], "binary")
    print("metric                 value")
    print("---------------------  ------------------------------")
    for name in ("cyclomatic", "cognitive", "function_lloc"):
        values = _mapping(complexity[name], f"complexity.{name}")
        print(f"{name:21}  max {values['max']}, median {values['median']:.1f}, p90 {values['p90']:.1f}")
    print(f"{'unsafe':21}  " + ", ".join(f"{name}={unsafe[name]}" for name in UNSAFE_CATEGORIES))
    print(f"{'source lines':21}  production={source['production_lines']}, tests={source['test_lines']}")
    print(f"{'dependencies':21}  direct={dependencies['direct']}, transitive={dependencies['transitive']}, duplicates={dependencies['duplicate_versions']}")
    for name in ("safe", "unsafe_fastpath"):
        values = _mapping(binary[name], f"binary.{name}")
        print(f"{('binary ' + name):21}  file={values['file_bytes']}, text={values['text_bytes']}")
    print(f"snapshot: {CURRENT_PATH.relative_to(ROOT)}")


def bca_check_command(arguments: Sequence[str]) -> tuple[str, ...]:
    """Build an auditable BCA gate command that cannot honor inline suppressions."""
    return ("bca", "check", "--no-suppress", *arguments)


def _run_bca_check(arguments: Sequence[str]) -> None:
    command = bca_check_command(arguments)
    try:
        completed = subprocess.run(command, cwd=ROOT, check=False)
    except OSError as error:
        raise ToolError(f"could not run {' '.join(command)}: {error}") from error
    if completed.returncode == 2:
        raise GateError(f"bca complexity ratchet failed (exit {completed.returncode})")
    if completed.returncode != 0:
        raise ToolError(f"bca check failed with a tool error (exit {completed.returncode})")


def check_ratchets() -> None:
    """Run BCA's gate plus project-owned unsafe and long-function ratchets."""
    _ensure_tools(("bca", "cargo-geiger"))
    _run_bca_check(())
    baseline_raw = load_json(BASELINE_PATH)
    baseline = _mapping(baseline_raw, "metrics baseline")
    validate_baseline(baseline)
    current = _deterministic_values()
    regressions = unsafe_regressions(current, baseline)
    regressions.extend(long_function_regressions(current, baseline))
    if regressions:
        raise GateError("metric ratchet failed:\n" + "\n".join(f"- {item}" for item in regressions))
    print("metric ratchets: ok")


def _stage_json(destination: Path, document: object) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        return Path(handle.name)


def _stage_bytes(destination: Path, content: bytes) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb",
        dir=destination.parent,
        prefix=f".{destination.name}.rollback.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        return Path(handle.name)


def publish_baseline_pair(
    bca_candidate: Path,
    metrics_document: object,
    *,
    bca_destination: Path = BCA_BASELINE_PATH,
    metrics_destination: Path = BASELINE_PATH,
) -> None:
    """Publish two fully staged baselines, rolling BCA back on JSON failure."""
    metrics_candidate = _stage_json(metrics_destination, metrics_document)
    prior_bca = bca_destination.read_bytes() if bca_destination.exists() else None
    rollback = (
        _stage_bytes(bca_destination, prior_bca) if prior_bca is not None else None
    )
    bca_published = False
    try:
        os.replace(bca_candidate, bca_destination)
        bca_published = True
        os.replace(metrics_candidate, metrics_destination)
    except OSError as error:
        if bca_published:
            try:
                if rollback is None:
                    bca_destination.unlink(missing_ok=True)
                else:
                    os.replace(rollback, bca_destination)
                    rollback = None
            except OSError as rollback_error:
                raise MetricsError(
                    "could not publish baselines and BCA rollback also failed: "
                    f"{rollback_error}"
                ) from error
        raise MetricsError(f"could not publish baselines: {error}") from error
    finally:
        for temporary in (bca_candidate, metrics_candidate, rollback):
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def write_baselines() -> None:
    """Explicitly refresh both deterministic metric baselines."""
    versions = _ensure_tools(("bca", "cargo-geiger"))
    git = _git_info()
    with tempfile.NamedTemporaryFile(
        dir=BCA_BASELINE_PATH.parent,
        prefix=f".{BCA_BASELINE_PATH.name}.",
        suffix=".candidate",
        delete=False,
    ) as handle:
        candidate = Path(handle.name)
    candidate.unlink()
    try:
        _run_bca_check(("--write-baseline", str(candidate)))
        count_bca_baseline_entries(candidate)
        values = _deterministic_values(candidate)
        baseline = {
            "schema": BASELINE_SCHEMA,
            "updated_at": _utc_timestamp(),
            "git": git,
            "tools": versions,
            **values,
        }
        validate_baseline(baseline)
        publish_baseline_pair(candidate, baseline)
    finally:
        candidate.unlink(missing_ok=True)
    print(f"updated {BCA_BASELINE_PATH.relative_to(ROOT)}")
    print(f"updated {BASELINE_PATH.relative_to(ROOT)}")


def sanitize_label(label: str) -> str:
    sanitized = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    if not sanitized:
        raise MetricsError("record label must contain at least one ASCII letter or digit")
    return sanitized


def record_snapshot(
    snapshot: dict[str, object],
    history_dir: Path,
    label: str,
    *,
    timestamp: str | None = None,
) -> Path:
    """Exclusively create a sanitized, append-only history snapshot."""
    validate_snapshot(snapshot)
    stamp = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not re.fullmatch(r"\d{8}T\d{6}Z", stamp):
        raise MetricsError("history timestamp must use YYYYMMDDTHHMMSSZ")
    git = _mapping(snapshot["git"], "git")
    short_sha = _string(git["revision"], "git.revision")[:12]
    destination = history_dir / f"{stamp}-{short_sha}-{sanitize_label(label)}.json"
    history_dir.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("x", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except FileExistsError as error:
        raise MetricsError(f"history snapshot already exists: {destination}") from error
    except OSError as error:
        raise MetricsError(f"cannot record history snapshot {destination}: {error}") from error
    return destination


def coverage_commands(metrics_dir: Path) -> list[tuple[str, ...]]:
    raw_path = metrics_dir / "raw" / "coverage.json"
    html_path = metrics_dir / "coverage"
    return [
        ("cargo", "llvm-cov", "clean", "--workspace"),
        ("cargo", "llvm-cov", "--no-report"),
        ("cargo", "llvm-cov", "--no-report", "--features", "unsafe-fastpath"),
        ("cargo", "llvm-cov", "report", "--json", "--output-path", str(raw_path)),
        ("cargo", "llvm-cov", "report", "--html", "--output-dir", str(html_path)),
    ]


def coverage_environment(
    base: dict[str, str], target_dir: Path
) -> dict[str, str]:
    """Keep coverage cleanup and profiles inside this repository's target."""
    environment = dict(base)
    environment["CARGO_TARGET_DIR"] = str(target_dir)
    return environment


def attach_coverage(
    snapshot: dict[str, object],
    coverage_document: dict[str, object],
    tool_version: str,
) -> dict[str, object]:
    """Attach merged coverage and its producer version to a snapshot."""
    tools = _mapping(_required(snapshot, "tools"), "tools")
    tools["cargo-llvm-cov"] = _string(tool_version, "cargo-llvm-cov version")
    snapshot["coverage"] = normalize_coverage(coverage_document)
    validate_snapshot(snapshot)
    return snapshot


def collect_coverage(open_report: bool = False) -> None:
    """Run the exact two-configuration merged coverage workflow."""
    versions = _ensure_tools(("cargo-llvm-cov",))
    snapshot = collect_snapshot(publish=False)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    environment = coverage_environment(
        dict(os.environ), Path("target/metrics/llvm-cov-target")
    )
    for command in coverage_commands(Path("target/metrics")):
        print(f"$ {' '.join(command)}", flush=True)
        try:
            subprocess.run(command, cwd=ROOT, env=environment, check=True)
        except (OSError, subprocess.CalledProcessError) as error:
            raise ToolError(f"coverage command failed: {' '.join(command)}: {error}") from error
    coverage_raw = load_json(RAW_DIR / "coverage.json")
    coverage_document = _mapping(coverage_raw, "coverage document")
    attach_coverage(snapshot, coverage_document, versions["cargo-llvm-cov"])
    atomic_write_json(CURRENT_PATH, snapshot)
    index = METRICS_DIR / "coverage" / "html" / "index.html"
    print(f"coverage report: {index.relative_to(ROOT)}")
    if not open_report:
        return
    system = platform.system()
    opener = "open" if system == "Darwin" else "xdg-open" if system == "Linux" else None
    if opener is None:
        print(index)
        return
    try:
        subprocess.run((opener, str(index)), check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        raise ToolError(f"could not open coverage report with {opener}: {error}") from error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", help="run deterministic complexity and unsafe ratchets")
    subparsers.add_parser("collect", help="collect a normalized trend snapshot")
    record = subparsers.add_parser("record", help="append the current snapshot to history")
    record.add_argument("label")
    subparsers.add_parser("baseline", help="explicitly refresh deterministic baselines")
    coverage = subparsers.add_parser("coverage", help="collect merged safe/fast coverage")
    coverage.add_argument("--open", action="store_true", dest="open_report")
    tools_check = subparsers.add_parser(
        "tools-check", help="diagnose required pinned tools"
    )
    tools_check.add_argument("tools", nargs="*", choices=tuple(TOOLS))
    subparsers.add_parser("tools-install-command", help="print pinned installation commands")
    return parser


def _fail(message: str, code: int) -> NoReturn:
    print(f"metrics: {message}", file=sys.stderr)
    raise SystemExit(code)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "tools-install-command":
            for _version, install in TOOLS.values():
                print(install)
            return 0
        if args.command == "tools-check":
            results = inspect_tools(args.tools or None)
            for result in results:
                print(result["message"])
            return 0 if all(result["state"] == "ok" for result in results) else 1
        if args.command == "collect":
            _print_snapshot(collect_snapshot())
            return 0
        if args.command == "check":
            check_ratchets()
            return 0
        if args.command == "baseline":
            write_baselines()
            return 0
        if args.command == "record":
            snapshot_raw = load_json(CURRENT_PATH)
            snapshot = _mapping(snapshot_raw, "current snapshot")
            destination = record_snapshot(snapshot, HISTORY_DIR, args.label)
            print(f"recorded {destination.relative_to(ROOT)}")
            return 0
        if args.command == "coverage":
            collect_coverage(args.open_report)
            return 0
        raise AssertionError(f"unhandled command {args.command}")
    except GateError as error:
        _fail(str(error), 2)
    except MetricsError as error:
        _fail(str(error), 1)


if __name__ == "__main__":
    raise SystemExit(main())
