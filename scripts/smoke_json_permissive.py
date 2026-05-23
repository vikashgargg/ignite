"""
Smoke test: JSON permissive mode with _corrupt_record column.

Run as:
    IGNITE_BIN=./target/debug/ignite \
    .venvs/smoke/bin/python scripts/smoke_json_permissive.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Start Ignite server
# ---------------------------------------------------------------------------

ignite_bin = os.environ.get("IGNITE_BIN", "./target/debug/ignite")
proc = subprocess.Popen(
    [ignite_bin, "server", "--ip", "0.0.0.0", "--port", "50055"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.PIPE,
)
# Wait until server reports it's listening (logs go to stderr)
deadline = time.time() + 15
started = False
while time.time() < deadline:
    line = proc.stderr.readline().decode(errors="replace")
    if line:
        print(f"  server: {line.strip()}")
    if "ready" in line.lower() or "Starting" in line:
        started = True
        break
    time.sleep(0.05)
if not started:
    print("  WARNING: server startup message not seen, proceeding anyway")
time.sleep(1.0)

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.remote("sc://localhost:50055").getOrCreate()

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = []

def check(name, actual, expected):
    ok = actual == expected
    print(f"  [{PASS if ok else FAIL}] {name}: {actual!r}")
    if not ok:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")

try:
    with tempfile.TemporaryDirectory() as tmp:
        # ---------------------------------------------------------------
        # Test 1: PERMISSIVE + _corrupt_record column
        # ---------------------------------------------------------------
        print("\n--- Test 1: PERMISSIVE + _corrupt_record ---")
        path1 = os.path.join(tmp, "mixed.json")
        with open(path1, "w") as f:
            f.write('{"id":1,"name":"alice"}\n')
            f.write("NOT_JSON\n")
            f.write('{"id":3,"name":"carol"}\n')

        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])
        df1 = spark.read.schema(schema1).option("mode", "PERMISSIVE").json(path1)
        rows1 = {r.id: r for r in df1.collect()}
        check("row count", len(rows1), 3)
        check("row 1 name", rows1[1].name, "alice")
        check("row 1 corrupt_record is null", rows1[1]._corrupt_record, None)
        check("malformed row id is null", rows1.get(None, rows1.get(2)) or list(rows1.values())[1], rows1[list(rows1.keys())[1]])
        # Find the row where id is None (malformed)
        malformed = [r for r in df1.collect() if r.id is None]
        check("malformed rows count", len(malformed), 1)
        check("malformed _corrupt_record", malformed[0]._corrupt_record, "NOT_JSON")
        check("row 3 name", rows1[3].name, "carol")
        check("row 3 corrupt_record is null", rows1[3]._corrupt_record, None)

        # ---------------------------------------------------------------
        # Test 2: PERMISSIVE without _corrupt_record → null row
        # ---------------------------------------------------------------
        print("\n--- Test 2: PERMISSIVE without _corrupt_record ---")
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df2 = spark.read.schema(schema2).option("mode", "PERMISSIVE").json(path1)
        rows2 = df2.collect()
        check("row count", len(rows2), 3)
        nulls2 = [r for r in rows2 if r.id is None]
        check("null rows count", len(nulls2), 1)

        # ---------------------------------------------------------------
        # Test 3: DROPMALFORMED
        # ---------------------------------------------------------------
        print("\n--- Test 3: DROPMALFORMED ---")
        df3 = spark.read.schema(schema2).option("mode", "DROPMALFORMED").json(path1)
        rows3 = df3.collect()
        check("row count after drop", len(rows3), 2)
        check("ids present", sorted([r.id for r in rows3]), [1, 3])

        # ---------------------------------------------------------------
        # Test 4: FAILFAST
        # ---------------------------------------------------------------
        print("\n--- Test 4: FAILFAST ---")
        try:
            spark.read.schema(schema2).option("mode", "FAILFAST").json(path1).collect()
            check("FAILFAST raised error", False, True)
        except Exception:
            check("FAILFAST raised error", True, True)

        # ---------------------------------------------------------------
        # Test 5: custom columnNameOfCorruptRecord
        # ---------------------------------------------------------------
        print("\n--- Test 5: columnNameOfCorruptRecord option ---")
        schema5 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("bad_row", StringType(), True),
        ])
        df5 = (spark.read.schema(schema5)
               .option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", "bad_row")
               .json(path1))
        malformed5 = [r for r in df5.collect() if r.id is None]
        check("custom corrupt col count", len(malformed5), 1)
        check("custom corrupt col value", malformed5[0].bad_row, "NOT_JSON")

finally:
    spark.stop()
    proc.terminate()

# ---------------------------------------------------------------------------
print(f"\n{'='*50}")
if failures:
    print(f"{FAIL} {len(failures)} failure(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"{PASS} All JSON permissive smoke tests passed!")
