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

# ── Test 4: F.window() batch groupBy ─────────────────────────────────────────
# Validates that the window() function produces correct struct<start,end> keys
# and that DataFusion can group by them in batch mode.

def window_batch_groupby():
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, TimestampType, LongType
    import datetime

    base = datetime.datetime(2024, 1, 1, 0, 0, 0)
    rows = [(base + datetime.timedelta(minutes=i * 10), i) for i in range(12)]
    df = spark.createDataFrame(rows, schema=["ts", "value"])

    result = (
        df.groupBy(F.window("ts", "1 hour"))
        .agg(F.sum("value").alias("total"))
        .orderBy("window")
        .collect()
    )
    assert len(result) >= 2, f"expected at least 2 hourly buckets, got {len(result)}"
    for row in result:
        w = row["window"]
        diff_mins = (w.end - w.start).total_seconds() / 60
        assert abs(diff_mins - 60) < 1, f"window width should be 60 min, got {diff_mins}"
    print(f"         F.window() produced {len(result)} hourly buckets")


check("F.window() batch groupBy produces correct hourly buckets", window_batch_groupby)


# ── Test 5: streaming × static join ──────────────────────────────────────────

_query5 = None


def start_stream_static_join():
    global _query5
    from pyspark.sql import functions as F

    static = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")],
                                   schema=["id", "label"])

    stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", "2")
        .load()
        .select((F.col("value") % 3 + 1).cast("long").alias("id"))
    )

    joined = stream.join(static, on="id", how="inner")
    _query5 = (
        joined.writeStream
        .format("memory")
        .queryName("joined_stream")
        .outputMode("append")
        .start()
    )
    time.sleep(4)


check("Start stream × static join → memory sink", start_stream_static_join)


def validate_stream_static_join():
    rows = spark.sql("SELECT * FROM joined_stream LIMIT 10").collect()
    assert len(rows) >= 1, f"expected joined rows, got {len(rows)}"
    for row in rows:
        assert row["label"] in ("one", "two", "three"), f"unexpected label: {row['label']}"
    print(f"         joined_stream: {len(rows)} rows, labels valid")


check("Query joined_stream table", validate_stream_static_join)


def stop_query5():
    if _query5 is not None:
        _query5.stop()


check("Stop stream×static join cleanly", stop_query5)

# ── Test 6: streaming checkpoint writes offset files ─────────────────────────

def checkpoint_writes_offsets():
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as ckpt_dir:
        df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
        q = (
            df.writeStream
            .format("memory")
            .queryName("ckpt_test")
            .outputMode("append")
            .option("checkpointLocation", ckpt_dir)
            .start()
        )
        time.sleep(4)
        q.stop()
        offsets_dir = os.path.join(ckpt_dir, "offsets")
        files = os.listdir(offsets_dir) if os.path.isdir(offsets_dir) else []
        assert len(files) >= 1, f"expected offset files in {offsets_dir}, found {files}"
        print(f"         checkpoint offsets: {sorted(files)}")


check("Streaming checkpoint writes offset files", checkpoint_writes_offsets)

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
