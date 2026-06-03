"""
Vajra TPC-H Distributed Benchmark (SF-100)
============================================
Runs all 22 TPC-H queries at scale factor 100 (~100 GB) against a running
Vajra cluster and reports query times + summary.

Usage:
    # Generate data (requires ~150 GB disk; downloads DuckDB TPC-H extension)
    SPARK_REMOTE=sc://scheduler:50051 \\
    TPCH_SF=100 \\
    TPCH_DATA_DIR=/mnt/tpch/sf100 \\
      python scripts/tpch_distributed.py

    # Run against pre-generated Parquet data
    SPARK_REMOTE=sc://scheduler:50051 \\
    TPCH_DATA_DIR=/mnt/tpch/sf100 \\
    TPCH_SKIP_GENERATE=1 \\
      python scripts/tpch_distributed.py

    # SF-1 quick smoke test (CI)
    SPARK_REMOTE=sc://localhost:50051 TPCH_SF=1 python scripts/tpch_distributed.py

Environment Variables:
    SPARK_REMOTE        Vajra server address (default: sc://localhost:50051)
    TPCH_SF             Scale factor (default: 1)
    TPCH_DATA_DIR       Directory for Parquet data files (default: /tmp/tpch_data)
    TPCH_SKIP_GENERATE  Set to 1 to skip data generation (data must already exist)
    TPCH_QUERIES        Comma-separated query numbers to run (default: all 22)
    TPCH_WARMUP         Set to 1 to run each query twice (first run warms file cache)
    TPCH_PASS_THRESHOLD Minimum queries that must pass (default: 22)

Requirements:
    pip install pyspark[connect]==4.0.0 duckdb pandas pyarrow
"""
from __future__ import annotations

import os
import sys
import time
import json
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("ERROR: pyspark not installed. Run: pip install pyspark[connect]==4.0.0")
    sys.exit(1)

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
TPCH_SF = float(os.environ.get("TPCH_SF", "1"))
TPCH_DATA_DIR = Path(os.environ.get("TPCH_DATA_DIR", "/tmp/tpch_data"))
TPCH_SKIP_GENERATE = os.environ.get("TPCH_SKIP_GENERATE", "0") == "1"
TPCH_QUERIES_ENV = os.environ.get("TPCH_QUERIES", "")
TPCH_WARMUP = os.environ.get("TPCH_WARMUP", "0") == "1"
TPCH_PASS_THRESHOLD = int(os.environ.get("TPCH_PASS_THRESHOLD", "22"))

TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# All 22 TPC-H queries (standard SQL, Spark-compatible)
TPCH_QUERIES: dict[int, str] = {
    1: """
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price,
               SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
               SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
               AVG(l_quantity) AS avg_qty,
               AVG(l_extendedprice) AS avg_price,
               AVG(l_discount) AS avg_disc,
               COUNT(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    2: """
        SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
        FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey
          AND s_suppkey = ps_suppkey
          AND p_size = 15
          AND p_type LIKE '%BRASS'
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
          AND ps_supplycost = (
              SELECT MIN(ps_supplycost)
              FROM partsupp, supplier, nation, region
              WHERE p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
          )
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100
    """,
    3: """
        SELECT l_orderkey,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < DATE '1995-03-15'
          AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
    """,
    4: """
        SELECT o_orderpriority, COUNT(*) AS order_count
        FROM orders
        WHERE o_orderdate >= DATE '1993-07-01'
          AND o_orderdate < DATE '1993-10-01'
          AND EXISTS (
              SELECT * FROM lineitem
              WHERE l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate
          )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    """,
    5: """
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= DATE '1994-01-01'
          AND o_orderdate < DATE '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    """,
    6: """
        SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1995-01-01'
          AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
          AND l_quantity < 24
    """,
    7: """
        SELECT supp_nation, cust_nation, l_year,
               SUM(volume) AS revenue
        FROM (
            SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                   YEAR(l_shipdate) AS l_year,
                   l_extendedprice * (1 - l_discount) AS volume
            FROM supplier, lineitem, orders, customer, nation n1, nation n2
            WHERE s_suppkey = l_suppkey
              AND o_orderkey = l_orderkey
              AND c_custkey = o_custkey
              AND s_nationkey = n1.n_nationkey
              AND c_nationkey = n2.n_nationkey
              AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                   OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
              AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
    """,
    8: """
        SELECT o_year,
               SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) /
               SUM(volume) AS mkt_share
        FROM (
            SELECT YEAR(o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) AS volume,
                   n2.n_name AS nation
            FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
            WHERE p_partkey = l_partkey
              AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey
              AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey
              AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA'
              AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
              AND p_type = 'ECONOMY ANODIZED STEEL'
        ) all_nations
        GROUP BY o_year
        ORDER BY o_year
    """,
    9: """
        SELECT nation, o_year, SUM(amount) AS sum_profit
        FROM (
            SELECT n_name AS nation,
                   YEAR(o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND p_partkey = l_partkey
              AND o_orderkey = l_orderkey
              AND s_nationkey = n_nationkey
              AND p_name LIKE '%green%'
        ) profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
    """,
    10: """
        SELECT c_custkey, c_name,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               c_acctbal, n_name, c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate >= DATE '1993-10-01'
          AND o_orderdate < DATE '1994-01-01'
          AND l_returnflag = 'R'
          AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC
        LIMIT 20
    """,
    11: """
        SELECT ps_partkey,
               SUM(ps_supplycost * ps_availqty) AS value
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey
          AND s_nationkey = n_nationkey
          AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > (
            SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
            FROM partsupp, supplier, nation
            WHERE ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
        )
        ORDER BY value DESC
    """,
    12: """
        SELECT l_shipmode,
               SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
               SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate
          AND l_receiptdate >= DATE '1994-01-01'
          AND l_receiptdate < DATE '1995-01-01'
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    """,
    13: """
        SELECT c_count, COUNT(*) AS custdist
        FROM (
            SELECT c_custkey, COUNT(o_orderkey) AS c_count
            FROM customer LEFT OUTER JOIN orders
            ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
            GROUP BY c_custkey
        ) c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
    """,
    14: """
        SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                                 THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
               SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey
          AND l_shipdate >= DATE '1995-09-01'
          AND l_shipdate < DATE '1995-10-01'
    """,
    15: """
        WITH revenue AS (
            SELECT l_suppkey AS supplier_no,
                   SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '1996-01-01'
              AND l_shipdate < DATE '1996-04-01'
            GROUP BY l_suppkey
        )
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        FROM supplier, revenue
        WHERE s_suppkey = supplier_no
          AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
        ORDER BY s_suppkey
    """,
    16: """
        SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM partsupp, part
        WHERE p_partkey = ps_partkey
          AND p_brand <> 'Brand#45'
          AND p_type NOT LIKE 'MEDIUM POLISHED%'
          AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND ps_suppkey NOT IN (
              SELECT s_suppkey FROM supplier
              WHERE s_comment LIKE '%Customer%Complaints%'
          )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    """,
    17: """
        SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
        FROM lineitem, part
        WHERE p_partkey = l_partkey
          AND p_brand = 'Brand#23'
          AND p_container = 'MED BOX'
          AND l_quantity < (
              SELECT 0.2 * AVG(l_quantity)
              FROM lineitem
              WHERE l_partkey = p_partkey
          )
    """,
    18: """
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
               SUM(l_quantity)
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (
            SELECT l_orderkey FROM lineitem
            GROUP BY l_orderkey
            HAVING SUM(l_quantity) > 300
        )
        AND c_custkey = o_custkey
        AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
    """,
    19: """
        SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM lineitem, part
        WHERE (
            p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 11
            AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 20
            AND p_size BETWEEN 1 AND 10
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#34'
            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 30
            AND p_size BETWEEN 1 AND 15
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        )
    """,
    20: """
        SELECT s_name, s_address
        FROM supplier, nation
        WHERE s_suppkey IN (
            SELECT ps_suppkey FROM partsupp
            WHERE ps_partkey IN (
                SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT 0.5 * SUM(l_quantity)
                FROM lineitem
                WHERE l_partkey = ps_partkey
                  AND l_suppkey = ps_suppkey
                  AND l_shipdate >= DATE '1994-01-01'
                  AND l_shipdate < DATE '1995-01-01'
            )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
        ORDER BY s_name
    """,
    21: """
        SELECT s_name, COUNT(*) AS numwait
        FROM supplier, lineitem l1, orders, nation
        WHERE s_suppkey = l1.l_suppkey
          AND o_orderkey = l1.l_orderkey
          AND o_orderstatus = 'F'
          AND l1.l_receiptdate > l1.l_commitdate
          AND EXISTS (
              SELECT * FROM lineitem l2
              WHERE l2.l_orderkey = l1.l_orderkey
                AND l2.l_suppkey <> l1.l_suppkey
          )
          AND NOT EXISTS (
              SELECT * FROM lineitem l3
              WHERE l3.l_orderkey = l1.l_orderkey
                AND l3.l_suppkey <> l1.l_suppkey
                AND l3.l_receiptdate > l3.l_commitdate
          )
          AND s_nationkey = n_nationkey
          AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name
        ORDER BY numwait DESC, s_name
        LIMIT 100
    """,
    22: """
        SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
        FROM (
            SELECT SUBSTR(c_phone, 1, 2) AS cntrycode, c_acctbal
            FROM customer
            WHERE SUBSTR(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
              AND c_acctbal > (
                  SELECT AVG(c_acctbal)
                  FROM customer
                  WHERE c_acctbal > 0.00
                    AND SUBSTR(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
              )
              AND NOT EXISTS (
                  SELECT * FROM orders WHERE o_custkey = c_custkey
              )
        ) custsale
        GROUP BY cntrycode
        ORDER BY cntrycode
    """,
}


def generate_tpch_data(spark: "SparkSession", sf: float, data_dir: Path) -> None:
    """Generate TPC-H data via DuckDB and write as Parquet, then load into Spark."""
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    print(f"  Generating TPC-H SF={sf} via DuckDB...")
    con.execute(f"CALL dbgen(sf={sf});")
    for table in TPCH_TABLES:
        table_dir = data_dir / table
        if table_dir.exists() and any(table_dir.glob("*.parquet")):
            print(f"  Skipping {table} (already exists)")
            continue
        table_dir.mkdir(exist_ok=True)
        out = str(table_dir / "data.parquet")
        con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET)")
        print(f"  Written {table} → {out}")
    con.close()


def load_tables(spark: "SparkSession", data_dir: Path) -> None:
    """Load Parquet files from data_dir as temp views."""
    for table in TPCH_TABLES:
        parquet_path = str(data_dir / table / "*.parquet")
        spark.read.parquet(parquet_path).createOrReplaceTempView(table)


def run_query(spark: "SparkSession", q_num: int, sql: str, warmup: bool = False) -> tuple[bool, str, float]:
    if warmup:
        try:
            spark.sql(sql.strip()).collect()
        except Exception:
            pass
    t0 = time.time()
    try:
        spark.sql(sql.strip()).collect()
        elapsed = time.time() - t0
        return True, "", elapsed
    except Exception as e:
        elapsed = time.time() - t0
        msg = str(e).split("\n")[0][:120]
        return False, msg, elapsed


def main() -> None:
    if TPCH_QUERIES_ENV:
        query_nums = [int(x.strip()) for x in TPCH_QUERIES_ENV.split(",") if x.strip()]
    else:
        query_nums = list(range(1, 23))

    print(f"Vajra TPC-H Distributed Benchmark (SF={TPCH_SF})")
    print(f"Server    : {SPARK_REMOTE}")
    print(f"Data dir  : {TPCH_DATA_DIR}")
    print(f"Queries   : {len(query_nums)}")
    print(f"Warmup    : {'yes' if TPCH_WARMUP else 'no'}")
    print()

    spark = (
        SparkSession.builder
        .remote(SPARK_REMOTE)
        .getOrCreate()
    )

    if not TPCH_SKIP_GENERATE:
        print("Generating TPC-H data...")
        generate_tpch_data(spark, TPCH_SF, TPCH_DATA_DIR)
        print()

    print("Loading tables into Spark...")
    load_tables(spark, TPCH_DATA_DIR)
    print()

    results: list[dict] = []
    print(f"{'Q':>3}  {'Status':6}  {'Time':>8}  Error")
    print("-" * 72)

    for q_num in query_nums:
        sql = TPCH_QUERIES.get(q_num)
        if sql is None:
            print(f"Q{q_num:>2}  SKIP    (no query defined)")
            continue
        ok, err, elapsed = run_query(spark, q_num, sql, warmup=TPCH_WARMUP)
        status = "PASS" if ok else "FAIL"
        err_snippet = "" if ok else err[:55]
        print(f"Q{q_num:>2}  {status:6}  {elapsed:>7.2f}s  {err_snippet}")
        results.append({"query": q_num, "status": status, "elapsed_s": round(elapsed, 3), "error": err})

    passed = [r for r in results if r["status"] == "PASS"]
    failed = [r for r in results if r["status"] == "FAIL"]
    total_time = sum(r["elapsed_s"] for r in results)

    print()
    print("=" * 72)
    print(f"TPC-H Result : {len(passed)}/{len(results)} queries passed")
    print(f"Total time   : {total_time:.3f}s")
    if passed:
        print(f"Avg per query: {total_time/len(results):.3f}s")
    if failed:
        failed_list = ", ".join("Q{}".format(r["query"]) for r in failed)
        print(f"\nFailed: {failed_list}")

    # Write JSON summary
    summary_path = TPCH_DATA_DIR / "benchmark_result.json"
    try:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_path, "w") as f:
            json.dump({
                "sf": TPCH_SF,
                "server": SPARK_REMOTE,
                "passed": len(passed),
                "total": len(results),
                "total_s": round(total_time, 3),
                "queries": results,
            }, f, indent=2)
        print(f"\nSummary written to {summary_path}")
    except Exception as e:
        print(f"WARNING: could not write summary: {e}")

    spark.stop()

    if len(passed) < TPCH_PASS_THRESHOLD:
        print(f"\nERROR: only {len(passed)} queries passed, threshold is {TPCH_PASS_THRESHOLD}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
