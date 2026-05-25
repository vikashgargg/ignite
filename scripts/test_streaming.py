"""
Vajra Streaming Integration Test
=================================
Validates the end-to-end streaming pipeline:
  rate source → count() aggregate → memory sink → spark.sql("SELECT * FROM counts")

Requires a running Vajra server (SPARK_REMOTE env) or will auto-start one.

Usage:
    SPARK_REMOTE=sc://localhost:50051 \\
    PYTHONPATH=.venvs/smoke/lib/python3.12/site-packages \\
      .venvs/smoke/bin/python scripts/test_streaming.py

Exit code: 0 = all tests passed, 1 = one or more failed.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import traceback

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "")
_proc = None

if not SPARK_REMOTE:
    vajra_bin = os.environ.get("VAJRA_BIN", "./target/release/vajra")
    _proc = subprocess.Popen(
        [vajra_bin, "server", "--ip", "0.0.0.0", "--port", "50056"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    deadline = time.time() + 30
    started = False
    while time.time() < deadline:
        line = _proc.stderr.readline().decode(errors="replace")
        if line:
            print(f"  server: {line.strip()}")
        if "ready" in line.lower() or "listening" in line.lower():
            started = True
            break
        time.sleep(0.05)
    if not started:
        print("  WARNING: server startup message not seen, proceeding anyway")
    time.sleep(2.0)
    SPARK_REMOTE = "sc://localhost:50056"

from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
spark.sql("SELECT 1").collect()  # warm up

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results: list[tuple[str, str, str]] = []
_failures = 0


def check(name: str, fn):
    global _failures
    try:
        fn()
        status = PASS
        note = ""
    except Exception as e:
        status = FAIL
        note = str(e).split("\n")[0][:120]
        _failures += 1
        traceback.print_exc()
    symbol = "✓" if "PASS" in status else "✗"
    print(f"  [{status}] {symbol} {name}")
    if note:
        print(f"         {note}")
    results.append((name, status, note))


print("\n" + "─" * 60)
print("  Vajra Streaming Integration Tests")
print("─" * 60)

# ── Test 1: rate → count → memory sink ───────────────────────────────────────

_query = None


def start_rate_to_memory():
    global _query
    df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", "5")
        .load()
    )
    count_df = df.groupBy().count()
    _query = (
        count_df.writeStream
        .format("memory")
        .queryName("counts")
        .outputMode("complete")
        .start()
    )
    # Give two micro-batches time to run
    time.sleep(4)


check("Start rate→count→memory streaming query", start_rate_to_memory)


def validate_memory_table():
    rows = spark.sql("SELECT * FROM counts").collect()
    assert len(rows) >= 1, f"expected ≥1 row from counts table, got {len(rows)}"
    total = rows[0][0]
    assert isinstance(total, int) and total >= 1, f"expected positive count, got {total!r}"
    print(f"         counts table: {total} rows accumulated")


check("Query memory table: spark.sql('SELECT * FROM counts')", validate_memory_table)


def stop_query():
    if _query is not None:
        _query.stop()


check("Stop streaming query cleanly", stop_query)

# ── Test 2: rate → project → memory sink (no aggregate) ──────────────────────

_query2 = None


def start_rate_passthrough():
    global _query2
    df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", "2")
        .load()
        .select("timestamp", "value")
    )
    _query2 = (
        df.writeStream
        .format("memory")
        .queryName("rate_rows")
        .outputMode("append")
        .start()
    )
    time.sleep(3)


check("Start rate passthrough → memory sink", start_rate_passthrough)


def validate_rate_rows():
    rows = spark.sql("SELECT * FROM rate_rows ORDER BY value").collect()
    assert len(rows) >= 1, f"expected rows in rate_rows, got {len(rows)}"
    print(f"         rate_rows table: {len(rows)} rows accumulated")


check("Query rate_rows table via spark.sql", validate_rate_rows)


def stop_query2():
    if _query2 is not None:
        _query2.stop()


check("Stop passthrough query cleanly", stop_query2)

# ── Test 3: error case — empty queryName ─────────────────────────────────────


def empty_query_name_errors():
    try:
        df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
        q = (
            df.writeStream
            .format("memory")
            .queryName("")
            .outputMode("append")
            .start()
        )
        q.stop()
        raise AssertionError("expected error for empty queryName but none raised")
    except AssertionError:
        raise
    except Exception:
        pass  # expected


check("Empty queryName raises an error", empty_query_name_errors)

# ── Summary ───────────────────────────────────────────────────────────────────

passed = sum(1 for _, s, _ in results if "PASS" in s)
failed = sum(1 for _, s, _ in results if "FAIL" in s)
total = len(results)

print(f"\n{'─' * 60}")
print(f"  Streaming tests: {passed}/{total} passed" + (f", {failed} failed" if failed else ""))
print(f"{'─' * 60}\n")

if _proc is not None:
    _proc.terminate()

sys.exit(0 if _failures == 0 else 1)
