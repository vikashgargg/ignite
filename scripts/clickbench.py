"""
Zelox ClickBench Benchmark
===========================
Runs the standard ClickBench 43 OLAP queries against a running Zelox server and
reports per-query timing plus a comparison summary.

The benchmark uses the official ClickBench `hits` Parquet dataset (100M rows,
~14 GB compressed) hosted on ClickHouse's public S3 bucket. A small subset
(hits_0.parquet, ~150 MB) is used by default for quick smoke tests.

Usage:
    # Quick smoke (single Parquet shard, ~150 MB)
    SPARK_REMOTE=sc://localhost:50051 python scripts/clickbench.py

    # Full 100-file dataset (~14 GB, downloads on first run)
    SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_FULL=1 python scripts/clickbench.py

    # Use already-downloaded data directory
    SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_DATA=/path/to/hits python scripts/clickbench.py

Requirements:
    pip install pyspark[connect] pyarrow
"""
from __future__ import annotations

import os
import sys
import time
import urllib.request
import tempfile
import traceback
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("ERROR: pyspark not installed.  Run: pip install pyspark[connect]")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
DATA_DIR = os.environ.get("CLICKBENCH_DATA", "")
FULL = os.environ.get("CLICKBENCH_FULL", "0").strip() not in ("", "0", "false", "False")
QUERIES_ENV = os.environ.get("CLICKBENCH_QUERIES", "")

# Official ClickBench S3 bucket (ClickHouse public)
CLICKBENCH_S3_BASE = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/"

# For the quick/smoke mode we only pull the first shard
QUICK_FILES = ["hits_0.parquet"]

# All 100 shards for the full dataset
FULL_FILES = [f"hits_{i}.parquet" for i in range(100)]

# ---------------------------------------------------------------------------
# Queries (official ClickBench 43 queries, adapted for Spark SQL)
# ---------------------------------------------------------------------------

QUERIES = [
    # Q1
    "SELECT count(*) FROM hits",
    # Q2
    "SELECT count(*) FROM hits WHERE AdvEngineID <> 0",
    # Q3
    "SELECT sum(AdvEngineID), count(*), avg(ResolutionWidth) FROM hits",
    # Q4
    "SELECT avg(UserID) FROM hits",
    # Q5
    "SELECT count(DISTINCT UserID) FROM hits",
    # Q6
    "SELECT count(DISTINCT SearchPhrase) FROM hits",
    # Q7
    "SELECT min(EventDate), max(EventDate) FROM hits",
    # Q8
    "SELECT AdvEngineID, count(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY count(*) DESC LIMIT 10",
    # Q9
    "SELECT RegionID, count(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    # Q10
    "SELECT RegionID, sum(AdvEngineID), count(*) AS c, avg(ResolutionWidth), count(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    # Q11
    "SELECT MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q12
    "SELECT MobilePhone, MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q13
    "SELECT SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q14
    "SELECT SearchPhrase, count(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    # Q15
    "SELECT SearchEngineID, SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q16
    "SELECT UserID, count(*) FROM hits GROUP BY UserID ORDER BY count(*) DESC LIMIT 10",
    # Q17
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q18
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    # Q19
    "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, count(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q20
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    # Q21
    "SELECT count(*) FROM hits WHERE URL LIKE '%google%'",
    # Q22
    "SELECT SearchPhrase, min(URL), count(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q23
    "SELECT SearchPhrase, min(URL), min(Title), count(*) AS c, count(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q24
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    # Q25
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    # Q26
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    # Q27
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    # Q28
    "SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q29
    "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, avg(length(Referer)) AS l, count(*) AS c, min(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q30
    "SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3), sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7), sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11), sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15), sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19), sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23), sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27), sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31), sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35), sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39), sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43), sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47), sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51), sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55), sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59), sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63), sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67), sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71), sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75), sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79), sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits",
    # Q31
    "SELECT SearchEngineID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q32
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q33
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q34
    "SELECT URL, count(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    # Q35
    "SELECT 1, URL, count(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    # Q36
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    # Q37
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    # Q38
    "SELECT Title, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    # Q39
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q40
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN SearchEngineID = 0 AND AdvEngineID = 0 THEN Referer ELSE '' END AS Src, URL AS Dst, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q41
    "SELECT URLHash, EventDate, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    # Q42
    "SELECT WindowClientWidth, WindowClientHeight, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    # Q43
    "SELECT date_trunc('minute', EventTime) AS M, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= date('2013-07-01') AND EventDate <= date('2013-07-31') AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY M ORDER BY M LIMIT 10 OFFSET 1000",
]

assert len(QUERIES) == 43, f"Expected 43 queries, got {len(QUERIES)}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading {url} → {dest} ...", end="", flush=True)
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "zelox-clickbench/1.0"})
    with urllib.request.urlopen(req) as resp, dest.open("wb") as fh:
        fh.write(resp.read())
    print(f" {time.time() - t0:.1f}s, {dest.stat().st_size // 1024 // 1024} MB")


def ensure_data(data_dir: Path, files: list[str]) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    for fname in files:
        dest = data_dir / fname
        if not dest.exists():
            url = CLICKBENCH_S3_BASE + fname
            download_file(url, dest)
    return data_dir


def build_session() -> SparkSession:
    # SPARK_REMOTE=local[*] runs reference Apache Spark (classic JVM) for an
    # apples-to-apples comparison; otherwise connect to a Spark Connect server.
    if SPARK_REMOTE.startswith("local"):
        print(f"Engine    : reference Apache Spark (master={SPARK_REMOTE})")
        os.environ.pop("SPARK_REMOTE", None)  # else pyspark auto-enables Connect
        return SparkSession.builder.master(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Resolve data directory
    s3_data = DATA_DIR.startswith("s3://") or DATA_DIR.startswith("s3a://")
    if s3_data:
        # Object-store path: the server (Zelox) reads it; no local existence check.
        data_path = DATA_DIR.rstrip("/")
        print(f"Using S3 data at {data_path}")
    elif DATA_DIR:
        data_path = Path(DATA_DIR)
        files = sorted(data_path.glob("*.parquet"))
        if not files:
            print(f"ERROR: no Parquet files found in {data_path}")
            sys.exit(1)
        print(f"Using existing data at {data_path} ({len(files)} files)")
    else:
        default_cache = Path.home() / ".cache" / "clickbench"
        data_path = default_cache
        file_list = FULL_FILES if FULL else QUICK_FILES
        print(f"Ensuring {'full (100 files)' if FULL else 'quick (1 file)'} dataset at {data_path}")
        ensure_data(data_path, file_list)

    spark = build_session()

    # Register the table with proper types.
    # ClickHouse exports EventDate as UINT16 (days since epoch) and EventTime /
    # ClientEventTime / LocalEventTime as INT64 (unix seconds).  DataFusion reads
    # these as UInt16 / Int64 which Spark Connect cannot serialise to Python.
    # We fix them at view creation so all 43 queries run correctly.
    parquet_glob = f"{data_path}/*.parquet" if s3_data else str(data_path / "*.parquet")
    raw = spark.read.parquet(parquet_glob)
    raw.createOrReplaceTempView("_hits_raw")

    date_cols = {"EventDate"}
    ts_cols = {"EventTime", "ClientEventTime", "LocalEventTime"}
    cast_parts = []
    for field in raw.schema.fields:
        n = f"`{field.name}`"
        dtype = type(field.dataType).__name__
        if field.name in date_cols:
            # uint16 days-since-epoch → DATE
            cast_parts.append(
                f"date_add(CAST('1970-01-01' AS DATE), CAST({n} AS INT)) AS `{field.name}`"
            )
        elif field.name in ts_cols:
            # int64 unix seconds → TIMESTAMP
            cast_parts.append(
                f"CAST(from_unixtime(CAST({n} AS BIGINT)) AS TIMESTAMP) AS `{field.name}`"
            )
        elif dtype in ("ShortType", "ByteType"):
            # uint8/uint16 → INT to avoid Arrow conversion errors
            cast_parts.append(f"CAST({n} AS INT) AS `{field.name}`")
        else:
            cast_parts.append(n)

    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW hits AS SELECT {', '.join(cast_parts)} FROM _hits_raw"
    )
    total_rows = spark.sql("SELECT count(*) FROM hits").collect()[0][0]
    print(f"\nTable 'hits' registered: {total_rows:,} rows from {parquet_glob}\n")

    # Determine which queries to run
    if QUERIES_ENV:
        indices = [int(x.strip()) - 1 for x in QUERIES_ENV.split(",") if x.strip()]
    else:
        indices = list(range(len(QUERIES)))

    results: list[tuple[int, float | None, str]] = []

    for idx in indices:
        q_num = idx + 1
        q_sql = QUERIES[idx]
        print(f"Q{q_num:02d}  ", end="", flush=True)
        t0 = time.perf_counter()
        try:
            rows = spark.sql(q_sql).collect()
            elapsed = time.perf_counter() - t0
            print(f"{elapsed:.3f}s  ({len(rows)} rows)")
            results.append((q_num, elapsed, "PASS"))
        except Exception as e:
            elapsed = time.perf_counter() - t0
            msg = str(e).split("\n")[0][:120]
            print(f"{elapsed:.3f}s  FAIL — {msg}")
            results.append((q_num, elapsed, f"FAIL: {msg}"))

    # Summary
    passed = [r for r in results if r[2] == "PASS"]
    failed = [r for r in results if r[2] != "PASS"]
    total_time = sum(r[1] for r in results if r[1] is not None)

    print()
    print("═" * 60)
    print("  ZELOX CLICKBENCH RESULTS")
    print("═" * 60)
    print(f"  Queries run  : {len(results)}/43")
    print(f"  Passed       : {len(passed)}")
    print(f"  Failed       : {len(failed)}")
    print(f"  Total time   : {total_time:.3f}s")
    if passed:
        times = [r[1] for r in passed]
        print(f"  Avg per query: {sum(times)/len(times):.3f}s")
    print("═" * 60)

    if failed:
        print("\nFailed queries:")
        for q_num, _, msg in failed:
            print(f"  Q{q_num:02d}: {msg}")

    # Timing table
    print("\nPer-query timing:")
    line = ""
    for q_num, elapsed, status in results:
        mark = "✓" if status == "PASS" else "✗"
        cell = f"Q{q_num:02d} {mark} {elapsed:.3f}s"
        line += f"  {cell:<22}"
        if q_num % 4 == 0:
            print(line)
            line = ""
    if line:
        print(line)

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
