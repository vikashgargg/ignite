"""zelox-pyspark CLI — start/stop/smoke-test a Zelox server."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time


def cmd_start(args: argparse.Namespace) -> int:
    bin_path = os.environ.get("ZELOX_BIN", "zelox")
    cmd = [bin_path, "server", "--ip", args.ip, "--port", str(args.port)]
    if args.mode:
        os.environ["ZELOX_MODE"] = args.mode
    if args.workers:
        os.environ["ZELOX_CLUSTER__WORKER_INITIAL_COUNT"] = str(args.workers)
    if args.auth_token:
        cmd += ["--auth-token", args.auth_token]
    print(f"Starting Zelox server: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd)
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()
    return proc.returncode or 0


def cmd_smoke(args: argparse.Namespace) -> int:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("ERROR: pyspark not installed. Run: pip install pyspark[connect]==4.0.0")
        return 1

    remote = f"sc://{args.host}:{args.port}"
    print(f"Connecting to {remote}...")
    spark = SparkSession.builder.remote(remote).getOrCreate()

    tests = [
        ("SELECT 1 + 1 AS result", lambda df: df.collect()[0]["result"] == 2),
        ("SELECT 'Zelox' AS name", lambda df: df.collect()[0]["name"] == "Zelox"),
        ("SELECT COUNT(*) AS n FROM (SELECT EXPLODE(SEQUENCE(1, 100))) t", lambda df: df.collect()[0]["n"] == 100),
    ]

    passed = 0
    for sql, check in tests:
        try:
            df = spark.sql(sql)
            assert check(df), f"assertion failed for: {sql}"
            print(f"  PASS  {sql[:60]}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {sql[:60]}\n        {e}")

    spark.stop()
    print(f"\nSmoke test: {passed}/{len(tests)} passed")
    return 0 if passed == len(tests) else 1


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="zelox-pyspark",
        description="Zelox PySpark CLI — manage a local Zelox server",
    )
    sub = parser.add_subparsers(dest="command")

    # start
    p_start = sub.add_parser("start", help="Start a Zelox server")
    p_start.add_argument("--ip", default="0.0.0.0")
    p_start.add_argument("--port", type=int, default=50051)
    p_start.add_argument("--mode", choices=["local", "local-cluster", "kubernetes-cluster"])
    p_start.add_argument("--workers", type=int, help="Worker count for local-cluster mode")
    p_start.add_argument("--auth-token", dest="auth_token")

    # smoke
    p_smoke = sub.add_parser("smoke", help="Run a quick smoke test against a server")
    p_smoke.add_argument("--host", default="localhost")
    p_smoke.add_argument("--port", type=int, default=50051)

    args = parser.parse_args()
    if args.command == "start":
        sys.exit(cmd_start(args))
    elif args.command == "smoke":
        sys.exit(cmd_smoke(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
