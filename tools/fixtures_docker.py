#!/usr/bin/env python3
"""
Runs the Docker image on every fixture with the scorer's flags (same as
starter-duckdb/scripts/docker_run.sh) and diffs against expected/ using the
official comparison code. Works on Windows hosts, where docker_run.sh can't be
driven from run_fixtures.py because of path mangling.

Usage: python tools/fixtures_docker.py [IMAGE] [CPUS] [MEM]
"""
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "starter-duckdb", "scripts"))
from run_fixtures import SCHEMAS, load_actual, load_expected, rows  # noqa: E402

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "shopping-sessions:latest"
CPUS = sys.argv[2] if len(sys.argv) > 2 else "8"
MEM = sys.argv[3] if len(sys.argv) > 3 else "11g"
FX = os.path.join(ROOT, "starter-duckdb", "fixtures")


def docker_run(data, out, scratch):
    cmd = [
        "docker", "run", "--rm", "--network", "none", "--read-only",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--pids-limit", "4096",
        "--cpus", CPUS, "--memory", MEM, "--memory-swap", MEM, "--user", "1000:1000",
        "-v", f"{data}:/data:ro", "-v", f"{out}:/output", "-v", f"{scratch}:/scratch",
        "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
        "-e", "HOME=/scratch", "-e", f"NCPU={CPUS}", "-e", f"MEM_GB={MEM.rstrip('g')}", IMAGE,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ, MSYS_NO_PATHCONV="1"))


def main():
    cases = sorted(d for d in os.listdir(FX) if os.path.isdir(os.path.join(FX, d)))
    failed = []
    for case in cases:
        data = os.path.join(FX, case, "input")
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as out, \
                tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
            p = docker_run(data, out, scratch)
            if p.returncode != 0:
                failed.append(case)
                print(f"FAIL {case}: exit {p.returncode}\n{p.stderr[-2000:]}")
                continue
            ok = True
            for table, schema in SCHEMAS.items():
                exp = rows(load_expected(os.path.join(FX, case, "expected", f"{table}.csv"), schema))
                try:
                    act = rows(load_actual(os.path.join(out, table), schema))
                except Exception as error:  # noqa: BLE001
                    ok = False
                    print(f"FAIL {case} {table}: {error}")
                    continue
                if exp != act:
                    ok = False
                    print(f"FAIL {case} {table}\n  expected {exp}\n  actual   {act}")
            print(("ok   " if ok else "FAIL ") + case)
            if not ok:
                failed.append(case)
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
