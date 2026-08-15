#!/usr/bin/env python3
"""Run and summarize ondaDB's standalone benchmark."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import tempfile
from typing import Iterable


PHASE_NAMES = {
    "Put": "put",
    "Get (cold)": "get",
    "Forward Scan": "forward",
    "Backward Scan": "backward",
    "Delete": "delete",
}
PHASE_ORDER = ("put", "get", "forward", "backward", "delete")

RESULT_RE = re.compile(
    r"^(Put|Get \(cold\)|Forward Scan|Backward Scan|Delete)\s+"
    r"(\d+) ops\s+([0-9]+(?:\.[0-9]+)?) ms\s+"
    r"([0-9]+(?:\.[0-9]+)?) ops/sec\s*$"
)
PHASE_PREFIX_RE = re.compile(
    r"^(Put|Get \(cold\)|Forward Scan|Backward Scan|Delete)(?:\s|$)"
)


@dataclass(frozen=True)
class PhaseResult:
    ops: int
    milliseconds: float
    ops_per_second: float


@dataclass(frozen=True)
class Workload:
    runs: int
    ops: int
    threads: int
    key_size: int
    value_size: int
    pattern: str
    compression: str
    batch: int
    features: str
    phases: tuple[str, ...]


def parse_output(text: str) -> dict[str, PhaseResult]:
    """Parse the stable phase lines emitted by ``onda_bench``."""
    parsed: dict[str, PhaseResult] = {}
    for line in text.splitlines():
        phase_match = PHASE_PREFIX_RE.match(line)
        if phase_match is None:
            continue
        match = RESULT_RE.match(line)
        if match is None:
            raise ValueError(f"missing or malformed benchmark measurement: {line}")
        label, ops_text, milliseconds_text, rate_text = match.groups()
        phase = PHASE_NAMES[label]
        if phase in parsed:
            raise ValueError(f"duplicate phase in benchmark output: {phase}")
        result = PhaseResult(
            ops=int(ops_text),
            milliseconds=float(milliseconds_text),
            ops_per_second=float(rate_text),
        )
        if not math.isfinite(result.milliseconds) or not math.isfinite(
            result.ops_per_second
        ):
            raise ValueError(f"benchmark measurements must be finite: {line}")
        if (
            result.ops <= 0
            or result.milliseconds <= 0
            or result.ops_per_second <= 0
        ):
            raise ValueError(f"benchmark measurements must be positive: {line}")
        parsed[phase] = result
    return parsed


def summarize(values: Iterable[float]) -> dict[str, float]:
    """Return robust summary statistics for one benchmark phase."""
    samples = list(values)
    if not samples:
        raise ValueError("summary requires at least one sample")
    if any(not math.isfinite(value) for value in samples):
        raise ValueError("summary samples must be finite")
    if any(value <= 0 for value in samples):
        raise ValueError("summary samples must be positive")
    median = float(statistics.median(samples))
    minimum = min(samples)
    maximum = max(samples)
    return {
        "median": median,
        "minimum": minimum,
        "maximum": maximum,
        "relative_spread_percent": (maximum - minimum) / median * 100.0,
    }


def cargo_binary_path(metadata: dict[str, object]) -> Path:
    """Locate the release binary from Cargo's versioned metadata output."""
    target_directory = metadata.get("target_directory")
    if not isinstance(target_directory, str) or not target_directory:
        raise ValueError("Cargo metadata is missing target_directory")
    binary = "onda_bench.exe" if os.name == "nt" else "onda_bench"
    return Path(target_directory) / "release" / binary


def build_command(binary: Path, workload: Workload, db_path: Path) -> list[str]:
    """Build one complete ``onda_bench`` invocation."""
    return [
        str(binary),
        "-ops",
        str(workload.ops),
        "-key_size",
        str(workload.key_size),
        "-value_size",
        str(workload.value_size),
        "-threads",
        str(workload.threads),
        "-pattern",
        workload.pattern,
        "-compression",
        workload.compression,
        "-batch",
        str(workload.batch),
        "-phases",
        ",".join(workload.phases),
        "-db",
        str(db_path),
    ]


def validate_phases(parsed: dict[str, object], requested: tuple[str, ...]) -> None:
    """Require one result for every requested phase."""
    missing = [phase for phase in requested if phase not in parsed]
    if missing:
        raise ValueError(f"missing requested phases: {', '.join(missing)}")


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        temporary.write(content)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def write_reports(document: dict[str, object], output_dir: Path) -> tuple[Path, Path]:
    """Atomically write the latest benchmark JSON and flat per-run CSV."""
    json_path = output_dir / "onda-latest.json"
    csv_path = output_dir / "onda-latest.csv"
    _atomic_text(json_path, json.dumps(document, indent=2, sort_keys=True) + "\n")

    rows: list[list[object]] = []
    runs = document.get("runs")
    if not isinstance(runs, list):
        raise ValueError("benchmark report is missing runs")
    for run in runs:
        if not isinstance(run, dict) or not isinstance(run.get("phases"), dict):
            raise ValueError("benchmark run has an invalid phase map")
        run_number = run.get("run")
        stdout = run.get("stdout")
        stderr = run.get("stderr")
        if not isinstance(stdout, str) or not isinstance(stderr, str):
            raise ValueError("benchmark run is missing captured output")
        for phase, measurement in run["phases"].items():
            if not isinstance(measurement, dict):
                raise ValueError("benchmark phase measurement must be an object")
            rows.append(
                [
                    phase,
                    run_number,
                    measurement.get("ops"),
                    measurement.get("milliseconds"),
                    measurement.get("ops_per_second"),
                    stdout,
                    stderr,
                ]
            )

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=output_dir, delete=False
    ) as temporary:
        writer = csv.writer(temporary, lineterminator="\n")
        writer.writerow(
            [
                "phase",
                "run",
                "ops",
                "milliseconds",
                "ops_per_second",
                "stdout",
                "stderr",
            ]
        )
        writer.writerows(rows)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, csv_path)
    return json_path, csv_path


def positive_int(value: str) -> int:
    """Parse an argparse-compatible positive integer."""
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"expected a positive integer: {value}") from error
    if parsed <= 0:
        raise ValueError(f"expected a positive integer: {value}")
    return parsed


def execute_runs(binary: Path, workload: Workload, db_root: Path) -> list[dict[str, object]]:
    """Execute all repetitions and return normalized per-run measurements."""
    runs: list[dict[str, object]] = []
    for run_number in range(1, workload.runs + 1):
        db_path = db_root / f"run-{run_number}"
        completed = subprocess.run(
            build_command(binary, workload, db_path),
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.stderr:
            print(completed.stderr, file=sys.stderr, end="")
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                completed.args,
                output=completed.stdout,
                stderr=completed.stderr,
            )
        parsed = parse_output(completed.stdout)
        validate_phases(parsed, workload.phases)
        runs.append(
            {
                "run": run_number,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
                "phases": {phase: asdict(parsed[phase]) for phase in workload.phases},
            }
        )
    return runs


def cargo_build_command(features: str) -> list[str]:
    """Return the release build command for one feature configuration."""
    command = ["cargo", "build", "--release"]
    if features != "safe":
        command.extend(["--features", features])
    command.extend(["--bin", "onda_bench"])
    return command


def make_document(
    workload: Workload,
    runs: list[dict[str, object]],
    *,
    collected_at: str,
    git: dict[str, object],
    host: dict[str, object],
    rustc: str,
) -> dict[str, object]:
    """Combine raw runs and provenance into the versioned report schema."""
    summary: dict[str, dict[str, float]] = {}
    for phase in workload.phases:
        samples: list[float] = []
        for run in runs:
            phases = run.get("phases")
            if not isinstance(phases, dict) or not isinstance(phases.get(phase), dict):
                raise ValueError(f"run is missing requested phase: {phase}")
            value = phases[phase].get("ops_per_second")
            if not isinstance(value, (int, float)):
                raise ValueError(f"phase throughput is not numeric: {phase}")
            samples.append(float(value))
        summary[phase] = summarize(samples)

    workload_document = asdict(workload)
    workload_document["phases"] = list(workload.phases)
    return {
        "schema": "ondadb.benchmark.v1",
        "collected_at": collected_at,
        "git": git,
        "host": host,
        "rustc": rustc,
        "workload": workload_document,
        "runs": runs,
        "summary": summary,
    }


def parse_phases(value: str) -> tuple[str, ...]:
    """Parse a non-empty, duplicate-free comma-separated phase list."""
    phases = tuple(value.split(','))
    if not phases or any(not phase for phase in phases):
        raise ValueError("phase list must not be empty")
    unknown = [phase for phase in phases if phase not in PHASE_ORDER]
    if unknown:
        raise ValueError(f"unknown phase: {unknown[0]}")
    if len(set(phases)) != len(phases):
        raise ValueError("duplicate phase in phase list")
    return phases


def _command_output(command: list[str]) -> str:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _git_metadata() -> dict[str, object]:
    return {
        "revision": _command_output(["git", "rev-parse", "HEAD"]),
        "dirty": bool(_command_output(["git", "status", "--short"])),
    }


def _host_metadata() -> dict[str, object]:
    return {
        "os": platform.system(),
        "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    onda = subparsers.add_parser("onda", help="run ondaDB's standalone benchmark")
    onda.add_argument("--runs", type=positive_int, default=5)
    onda.add_argument("--ops", type=positive_int, default=1_000_000)
    onda.add_argument("--threads", type=positive_int, default=8)
    onda.add_argument("--key-size", type=positive_int, default=16)
    onda.add_argument("--value-size", type=positive_int, default=100)
    onda.add_argument("--batch", type=positive_int, default=1000)
    onda.add_argument("--pattern", choices=("random", "sequential"), default="random")
    onda.add_argument("--compression", choices=("none", "snappy", "zstd"), default="none")
    onda.add_argument("--features", default="unsafe-fastpath")
    onda.add_argument("--phases", type=parse_phases, default=parse_phases(",".join(PHASE_ORDER)))
    onda.add_argument("--output-dir", type=Path, default=Path("target/benchmarks"))
    return parser


def run_onda(arguments: argparse.Namespace) -> tuple[Path, Path]:
    workload = Workload(
        runs=arguments.runs,
        ops=arguments.ops,
        threads=arguments.threads,
        key_size=arguments.key_size,
        value_size=arguments.value_size,
        pattern=arguments.pattern,
        compression=arguments.compression,
        batch=arguments.batch,
        features=arguments.features,
        phases=arguments.phases,
    )
    subprocess.run(cargo_build_command(workload.features), check=True)
    metadata = json.loads(_command_output(["cargo", "metadata", "--no-deps", "--format-version", "1"]))
    binary = cargo_binary_path(metadata)
    if not binary.is_file():
        raise ValueError(f"Cargo did not produce benchmark binary: {binary}")
    runs = execute_runs(binary, workload, arguments.output_dir / "db")
    document = make_document(
        workload,
        runs,
        collected_at=_utc_now(),
        git=_git_metadata(),
        host=_host_metadata(),
        rustc=_command_output(["rustc", "-Vv"]),
    )
    paths = write_reports(document, arguments.output_dir)
    for phase in workload.phases:
        summary = document["summary"][phase]
        print(
            f"{phase:>9}: median {summary['median']:.0f} ops/sec; "
            f"spread {summary['relative_spread_percent']:.1f}%"
        )
    print(f"JSON: {paths[0]}")
    print(f"CSV : {paths[1]}")
    return paths


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "onda":
            run_onda(arguments)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"benchmark: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
