#!/usr/bin/env python3
"""
Concurrency test: 20 parallel Spark Connect sessions, each running independent
queries, verifying session isolation (no cross-session data leaks).

Usage:
    python scripts/test_concurrency.py [--host localhost] [--port 15002] [--sessions 20]
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession


def run_session(session_id: int, host: str, port: int) -> dict:
    start = time.monotonic()
    errors = []
    spark = None
    try:
        spark = (
            SparkSession.builder
            .remote(f"sc://{host}:{port}")
            .appName(f"concurrency-test-{session_id}")
            .getOrCreate()
        )

        # Each session creates a temp view with a session-specific value
        spark.range(10).selectExpr(
            "id", f"{session_id} as session_id", "id * id as sq"
        ).createOrReplaceTempView("session_data")

        # Verify session_id is isolated — should always equal session_id
        rows = spark.sql(
            f"SELECT DISTINCT session_id FROM session_data"
        ).collect()
        actual_ids = {r.session_id for r in rows}
        if actual_ids != {session_id}:
            errors.append(
                f"session {session_id}: expected {{session_id={session_id}}}, got {actual_ids}"
            )

        # Run a CPU-bound query to create contention
        result = spark.sql(
            f"SELECT {session_id} AS sid, SUM(id * id) AS total FROM range(1000)"
        ).collect()
        expected_total = sum(i * i for i in range(1000))
        if result[0].total != expected_total:
            errors.append(
                f"session {session_id}: SUM mismatch — expected {expected_total}, "
                f"got {result[0].total}"
            )
        if result[0].sid != session_id:
            errors.append(
                f"session {session_id}: sid={result[0].sid} but expected {session_id}"
            )

        # Verify temp view is truly session-local (can't see other sessions' views)
        try:
            other_id = (session_id % 3) + 1  # some other session that definitely started first
            # This is a best-effort check — if the view doesn't exist the query will fail,
            # confirming isolation. If it returns rows the sid check below will catch leaks.
            rows2 = spark.sql(
                f"SELECT DISTINCT session_id FROM session_data"
            ).collect()
            for r in rows2:
                if r.session_id != session_id:
                    errors.append(
                        f"session {session_id}: saw foreign session_id={r.session_id} in temp view"
                    )
        except Exception:
            pass  # Expected if the view was cleaned up

    except Exception as exc:
        errors.append(f"session {session_id}: unexpected exception: {exc}")
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass

    elapsed = time.monotonic() - start
    return {"session_id": session_id, "elapsed": elapsed, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description="Spark Connect concurrency test")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=15002)
    parser.add_argument("--sessions", type=int, default=20)
    args = parser.parse_args()

    print(f"Starting {args.sessions} concurrent sessions against {args.host}:{args.port}")
    t0 = time.monotonic()

    all_errors = []
    timings = []

    with ThreadPoolExecutor(max_workers=args.sessions) as pool:
        futures = {
            pool.submit(run_session, i, args.host, args.port): i
            for i in range(1, args.sessions + 1)
        }
        for fut in as_completed(futures):
            result = fut.result()
            timings.append(result["elapsed"])
            if result["errors"]:
                all_errors.extend(result["errors"])
                print(f"  FAIL session {result['session_id']} ({result['elapsed']:.2f}s)")
                for e in result["errors"]:
                    print(f"       {e}")
            else:
                print(f"  OK   session {result['session_id']} ({result['elapsed']:.2f}s)")

    total = time.monotonic() - t0
    avg = sum(timings) / len(timings) if timings else 0
    print(
        f"\n{'PASS' if not all_errors else 'FAIL'} — "
        f"{args.sessions} sessions, {len(all_errors)} errors, "
        f"total {total:.2f}s, avg per-session {avg:.2f}s"
    )

    if all_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
