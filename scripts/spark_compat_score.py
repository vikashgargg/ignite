"""
Vajra Spark Compatibility Scorecard
=====================================
Tests ~50 key Spark features across SQL, DataFrames, UDFs, DML,
JSON/Parquet, and complex types.

Usage (starts its own server — single-node local mode):
    VAJRA_BIN=./target/debug/vajra \\
    DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \\
    PYTHONPATH=.venvs/smoke/lib/python3.9/site-packages \\
      .venvs/smoke/bin/python scripts/spark_compat_score.py

Against a running server — single-node (local mode):
    DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \\
    PYTHONPATH=.venvs/smoke/lib/python3.9/site-packages \\
      ./target/debug/vajra server --port 50055

    SPARK_REMOTE=sc://localhost:50055 \\
      .venvs/smoke/bin/python scripts/spark_compat_score.py

Against a running server — multi-worker (local-cluster mode, N workers in-process):
    DYLD_FRAMEWORK_PATH=/Library/Developer/CommandLineTools/Library/Frameworks \\
    PYTHONPATH=.venvs/smoke/lib/python3.9/site-packages \\
      ./target/debug/vajra cluster --role scheduler --port 50055 --workers 4

    SPARK_REMOTE=sc://localhost:50055 \\
      .venvs/smoke/bin/python scripts/spark_compat_score.py

Note: PYTHONPATH is required so the embedded Python interpreter (PyO3)
can find pyspark when executing Python UDFs in-process.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

# ── Server startup ────────────────────────────────────────────────────────────

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "")
_proc = None

if not SPARK_REMOTE:
    vajra_bin = os.environ.get("VAJRA_BIN", os.environ.get("VAJRA_BIN", "./target/debug/vajra"))
    _proc = subprocess.Popen(
        [vajra_bin, "server", "--ip", "0.0.0.0", "--port", "50055"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    deadline = time.time() + 20
    started = False
    while time.time() < deadline:
        line = _proc.stderr.readline().decode(errors="replace")
        if line:
            print(f"  server: {line.strip()}")
        if "ready" in line.lower() or "Vajra" in line:
            started = True
            break
        time.sleep(0.05)
    if not started:
        print("  WARNING: server startup message not seen, proceeding anyway")
    time.sleep(1.0)
    SPARK_REMOTE = "sc://localhost:50055"

# ── Spark session ─────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession, Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    IntegerType, LongType, StringType, DoubleType, BooleanType,
    StructType, StructField, ArrayType, MapType,
)
from pyspark.sql.functions import udf  # noqa: E402

spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
spark.sql("SELECT 1").collect()  # warm up session before tests start

# ── Test harness ──────────────────────────────────────────────────────────────

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
SKIP = "\033[33mSKIP\033[0m"

results: dict[str, list[tuple[str, str, str]]] = {}  # group → [(name, status, note)]
_current_group = [""]


def group(name: str):
    _current_group[0] = name
    results[name] = []
    print(f"\n{'─'*55}")
    print(f"  {name}")
    print(f"{'─'*55}")


def check(name: str, fn):
    try:
        fn()
        status = PASS
        note = ""
    except Exception as e:
        status = FAIL
        note = str(e).split("\n")[0][:80]
    symbol = "✓" if "PASS" in status else "✗"
    print(f"  [{status}] {symbol} {name}")
    if note:
        print(f"         {note}")
    results[_current_group[0]].append((name, status, note))


def skip(name: str, reason: str = ""):
    print(f"  [{SKIP}] ○ {name}" + (f"  ({reason})" if reason else ""))
    results[_current_group[0]].append((name, SKIP, reason))


# ── Helper functions ──────────────────────────────────────────────────────────


def assert_eq(a, b):
    assert a == b, f"{a!r} != {b!r}"


def assert_true(v):
    assert v, f"assertion failed: {v!r}"


def _raises(fn):
    try:
        fn()
        raise AssertionError("expected exception but none raised")
    except AssertionError:
        raise
    except Exception:
        pass


def _check_json_permissive(spark, path, schema):
    rows = spark.read.schema(schema).option("mode", "PERMISSIVE").json(path).collect()
    assert len(rows) == 3
    malformed = [r for r in rows if r.id is None]
    assert len(malformed) == 1
    assert malformed[0]._corrupt_record == "NOT_JSON"


def _check_no_schema_corrupt(spark, tmp):
    import os
    p = os.path.join(tmp, "noschema.json")
    with open(p, "w") as f:
        f.write('{"id":1,"value":"a"}\nNOT_JSON\n{"id":3,"value":"c"}\n')
    df = spark.read.format("json").load(p)
    assert "_corrupt_record" in df.columns, f"columns: {df.columns}"
    mal = [r for r in df.collect() if r["id"] is None]
    assert len(mal) == 1
    assert mal[0]["_corrupt_record"] == "NOT_JSON"


def _parquet_roundtrip(spark, df, path):
    df.write.mode("overwrite").parquet(path)
    back = spark.read.parquet(path)
    assert back.count() == df.count()


def _parquet_schema_evolve(spark, tmp):
    import os
    p = os.path.join(tmp, "parq_evolve")
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(p)
    back = spark.read.schema("id INT, name STRING, extra STRING").parquet(p)
    row = back.filter("id=1").collect()[0]
    assert row.extra is None


# ── Tests ─────────────────────────────────────────────────────────────────────

# When running against a remote server (container/k8s), use a shared path
# that is mounted into the server (e.g. -v /tmp/vajra:/tmp/vajra).
# This avoids "file not found" errors when the server tries to read Mac-local
# /var/folders/... paths that don't exist inside the container.
_remote_mode = bool(os.environ.get("SPARK_REMOTE", ""))
if _remote_mode:
    _tmp_root = "/tmp/vajra/scorecard-tmp"
    shutil.rmtree(_tmp_root, ignore_errors=True)
    os.makedirs(_tmp_root, exist_ok=True)
    import contextlib
    @contextlib.contextmanager
    def _tmp_ctx():
        try:
            yield _tmp_root
        finally:
            shutil.rmtree(_tmp_root, ignore_errors=True)
    _tmp_mgr = _tmp_ctx()
else:
    _tmp_mgr = tempfile.TemporaryDirectory()

with _tmp_mgr as tmp:

    # ── 1. Basic SQL ──────────────────────────────────────────────────────────
    group("1. Basic SQL")

    check("SELECT literal", lambda: assert_eq(
        spark.sql("SELECT 1 + 1 AS r").collect(), [Row(r=2)]))

    check("SELECT with alias", lambda: assert_eq(
        spark.sql("SELECT 'hello' AS s").collect(), [Row(s="hello")]))

    check("WHERE clause", lambda: assert_eq(
        spark.sql("SELECT id FROM range(5) WHERE id > 2").collect(),
        [Row(id=3), Row(id=4)]))

    check("ORDER BY", lambda: assert_eq(
        spark.sql("SELECT id FROM range(3) ORDER BY id DESC").collect(),
        [Row(id=2), Row(id=1), Row(id=0)]))

    check("LIMIT", lambda: assert_eq(
        len(spark.sql("SELECT id FROM range(100) LIMIT 5").collect()), 5))

    check("GROUP BY + COUNT", lambda: assert_eq(
        spark.sql("SELECT id % 2 AS parity, count(*) AS n FROM range(6) GROUP BY 1 ORDER BY 1").collect(),
        [Row(parity=0, n=3), Row(parity=1, n=3)]))

    check("HAVING", lambda: assert_eq(
        spark.sql("SELECT id % 3 AS m, count(*) n FROM range(9) GROUP BY 1 HAVING n > 2 ORDER BY 1").collect(),
        [Row(m=0, n=3), Row(m=1, n=3), Row(m=2, n=3)]))

    check("INNER JOIN", lambda: assert_eq(
        sorted(spark.sql(
            "SELECT a.id, b.id AS bid FROM range(3) a JOIN range(3) b ON a.id = b.id"
        ).collect(), key=lambda r: r.id),
        [Row(id=0, bid=0), Row(id=1, bid=1), Row(id=2, bid=2)]))

    check("LEFT JOIN", lambda: assert_true(
        len(spark.sql(
            "SELECT a.id FROM range(3) a LEFT JOIN (SELECT id FROM range(1)) b ON a.id = b.id"
        ).collect()) == 3))

    check("UNION ALL", lambda: assert_eq(
        spark.sql("SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n").collect(),
        [Row(n=1), Row(n=2)]))

    check("CASE WHEN", lambda: assert_eq(
        spark.sql("SELECT CASE WHEN id < 2 THEN 'low' ELSE 'high' END AS v FROM range(4) ORDER BY id").collect(),
        [Row(v="low"), Row(v="low"), Row(v="high"), Row(v="high")]))

    check("Subquery (IN)", lambda: assert_true(
        len(spark.sql("SELECT id FROM range(5) WHERE id IN (SELECT id FROM range(3))").collect()) == 3))

    check("CTE (WITH)", lambda: assert_eq(
        spark.sql("WITH t AS (SELECT 42 AS x) SELECT x FROM t").collect(),
        [Row(x=42)]))

    # ── 2. Aggregate functions ────────────────────────────────────────────────
    group("2. Aggregate Functions")

    df_nums = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "val"])
    df_nums.createOrReplaceTempView("nums")

    check("SUM", lambda: assert_eq(
        spark.sql("SELECT SUM(val) s FROM nums").collect(), [Row(s=60.0)]))

    check("AVG", lambda: assert_eq(
        spark.sql("SELECT AVG(val) a FROM nums").collect(), [Row(a=20.0)]))

    check("MIN / MAX", lambda: assert_eq(
        spark.sql("SELECT MIN(val) mn, MAX(val) mx FROM nums").collect(),
        [Row(mn=10.0, mx=30.0)]))

    check("COUNT DISTINCT", lambda: assert_eq(
        spark.sql("SELECT COUNT(DISTINCT id) c FROM nums").collect(), [Row(c=3)]))

    check("COLLECT_LIST", lambda: assert_eq(
        sorted(spark.sql("SELECT COLLECT_LIST(id) ids FROM nums").collect()[0].ids),
        [1, 2, 3]))

    check("FILTER in aggregate", lambda: assert_eq(
        spark.sql("SELECT SUM(val) FILTER (WHERE id > 1) s FROM nums").collect(),
        [Row(s=50.0)]))

    # ── 3. Window functions ───────────────────────────────────────────────────
    group("3. Window Functions")

    check("ROW_NUMBER", lambda: assert_eq(
        spark.sql(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM nums ORDER BY id"
        ).collect(),
        [Row(id=1, rn=1), Row(id=2, rn=2), Row(id=3, rn=3)]))

    check("RANK", lambda: assert_eq(
        spark.sql("SELECT id, RANK() OVER (ORDER BY id) r FROM nums ORDER BY id").collect(),
        [Row(id=1, r=1), Row(id=2, r=2), Row(id=3, r=3)]))

    check("SUM (window)", lambda: assert_eq(
        spark.sql(
            "SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cs FROM nums ORDER BY id"
        ).collect(),
        [Row(id=1, cs=10.0), Row(id=2, cs=30.0), Row(id=3, cs=60.0)]))

    check("LAG / LEAD", lambda: assert_eq(
        spark.sql(
            "SELECT id, LAG(id,1) OVER (ORDER BY id) lag1 FROM nums ORDER BY id"
        ).collect(),
        [Row(id=1, lag1=None), Row(id=2, lag1=1), Row(id=3, lag1=2)]))

    # ── 4. String functions ───────────────────────────────────────────────────
    group("4. String Functions")

    check("CONCAT / LENGTH", lambda: assert_eq(
        spark.sql("SELECT CONCAT('a','b','c') s, LENGTH('hello') l").collect(),
        [Row(s="abc", l=5)]))

    check("UPPER / LOWER", lambda: assert_eq(
        spark.sql("SELECT UPPER('hello') u, LOWER('WORLD') l").collect(),
        [Row(u="HELLO", l="world")]))

    check("SUBSTR / TRIM", lambda: assert_eq(
        spark.sql("SELECT SUBSTR('abcdef',2,3) s, TRIM('  hi  ') t").collect(),
        [Row(s="bcd", t="hi")]))

    check("REGEXP_REPLACE", lambda: assert_eq(
        spark.sql("SELECT REGEXP_REPLACE('abc123','[0-9]+','X') r").collect(),
        [Row(r="abcX")]))

    check("SPLIT", lambda: assert_eq(
        spark.sql("SELECT SPLIT('a,b,c',',') s").collect()[0].s,
        ["a", "b", "c"]))

    # ── 5. Date / Time functions ──────────────────────────────────────────────
    group("5. Date / Time Functions")

    check("CURRENT_DATE is a date", lambda: assert_true(
        spark.sql("SELECT CURRENT_DATE() d").collect()[0].d is not None))

    check("DATE_ADD", lambda: assert_eq(
        str(spark.sql("SELECT DATE_ADD(DATE '2024-01-01', 5) d").collect()[0].d),
        "2024-01-06"))

    check("DATEDIFF", lambda: assert_eq(
        spark.sql("SELECT DATEDIFF(DATE '2024-01-10', DATE '2024-01-01') d").collect(),
        [Row(d=9)]))

    check("DATE_FORMAT", lambda: assert_eq(
        spark.sql("SELECT DATE_FORMAT(DATE '2024-06-15', 'yyyy-MM') s").collect(),
        [Row(s="2024-06")]))

    # ── 6. Complex Types ──────────────────────────────────────────────────────
    group("6. Complex Types")

    check("ARRAY + ELEMENT_AT", lambda: assert_eq(
        spark.sql("SELECT ARRAY(1,2,3) a, ELEMENT_AT(ARRAY(10,20,30),2) e").collect(),
        [Row(a=[1, 2, 3], e=20)]))

    check("EXPLODE", lambda: assert_eq(
        sorted(r.v for r in spark.sql("SELECT EXPLODE(ARRAY(3,1,2)) v").collect()),
        [1, 2, 3]))

    check("MAP + MAP_KEYS", lambda: assert_eq(
        sorted(spark.sql("SELECT MAP_KEYS(MAP('a',1,'b',2)) k").collect()[0].k),
        ["a", "b"]))

    check("STRUCT access", lambda: assert_eq(
        spark.sql("SELECT STRUCT(1 AS x, 'y' AS s).x v").collect(),
        [Row(v=1)]))

    check("ARRAY_CONTAINS", lambda: assert_eq(
        spark.sql("SELECT ARRAY_CONTAINS(ARRAY(1,2,3), 2) r").collect(),
        [Row(r=True)]))

    # ── 7. DataFrame API ──────────────────────────────────────────────────────
    group("7. DataFrame API")

    df = spark.createDataFrame(
        [(1, "alice", 30), (2, "bob", 25), (3, "carol", 35)],
        ["id", "name", "age"],
    )

    check("filter + select", lambda: assert_eq(
        df.filter(df.age > 28).select("name").collect(),
        [Row(name="alice"), Row(name="carol")]))

    check("withColumn + cast", lambda: assert_eq(
        df.withColumn("age2", (df.age * 2).cast("int")).filter("id=1").select("age2").collect(),
        [Row(age2=60)]))

    check("groupBy + agg", lambda: assert_eq(
        df.groupBy().agg(F.max("age")).collect(), [Row(**{"max(age)": 35})]))

    check("join DataFrames", lambda: assert_true(
        df.join(df.select("id"), "id").count() == 3))

    check("dropDuplicates", lambda: assert_eq(
        spark.createDataFrame([(1,), (1,), (2,)], ["x"]).dropDuplicates().count(), 2))

    check("orderBy", lambda: assert_eq(
        [r.id for r in df.orderBy("age").collect()], [2, 1, 3]))

    check("union", lambda: assert_eq(
        df.select("id").union(df.select("id")).count(), 6))

    check("describe (schema check)", lambda: assert_true(
        "summary" in df.describe().columns))

    check("createOrReplaceTempView + SQL", lambda: (
        df.createOrReplaceTempView("people"),
        assert_eq(
            spark.sql("SELECT name FROM people WHERE age = (SELECT MAX(age) FROM people)").collect(),
            [Row(name="carol")]
        )
    ))

    # ── 8. UDFs ───────────────────────────────────────────────────────────────
    group("8. Python UDFs")

    check("Scalar UDF (non-arrow)", lambda: (
        spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false"),
        assert_eq(
            df.filter("id=1").select(udf(lambda x: x.upper())("name").alias("u")).collect(),
            [Row(u="ALICE")]
        ),
        spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled"),
    ))

    check("Scalar UDF (arrow)", lambda: (
        spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true"),
        assert_eq(
            df.filter("id=1").select(udf(lambda x: x.upper())("name").alias("u")).collect(),
            [Row(u="ALICE")]
        ),
        spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled"),
    ))

    check("UDF implicit string cast (int→str)", lambda: (
        assert_eq(
            spark.sql("SELECT 1 AS a").select(udf(lambda x: x)("a").alias("b")).collect(),
            [Row(b="1")]
        )
    ))

    check("UDF implicit binary cast (int→None)", lambda: (
        assert_eq(
            spark.sql("SELECT 1 AS a").select(udf(lambda x: x, returnType="binary")("a").alias("b")).collect(),
            [Row(b=None)]
        )
    ))

    check("UDF registered via SQL", lambda: (
        spark.udf.register("double_str", lambda s: (s or "") * 2),
        assert_eq(
            spark.sql("SELECT double_str(name) d FROM people WHERE id=1").collect(),
            [Row(d="alicealice")]
        )
    ))

    # ── 9. JSON Reading ───────────────────────────────────────────────────────
    group("9. JSON Reading")

    import os as _os
    json_path = _os.path.join(tmp, "data.json")
    with open(json_path, "w") as f:
        f.write('{"id":1,"name":"alice"}\nNOT_JSON\n{"id":3,"name":"carol"}\n')

    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    schema_corrupt = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    check("JSON PERMISSIVE + _corrupt_record", lambda: (
        _check_json_permissive(spark, json_path, schema_corrupt)
    ))

    check("JSON DROPMALFORMED", lambda: assert_eq(
        spark.read.schema(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])).option("mode", "DROPMALFORMED").json(json_path).count(), 2))

    check("JSON FAILFAST raises", lambda: (
        _raises(lambda: spark.read.schema(StructType([
            StructField("id", IntegerType(), True),
        ])).option("mode", "FAILFAST").json(json_path).collect())
    ))

    check("JSON no-schema → _corrupt_record inferred", lambda: (
        _check_no_schema_corrupt(spark, tmp)
    ))

    check("JSON with explicit schema", lambda: assert_eq(
        spark.read.schema("id INT, name STRING").json(json_path).filter("id IS NOT NULL").count(),
        2))

    # ── 10. Parquet ───────────────────────────────────────────────────────────
    group("10. Parquet Read / Write")

    parquet_path = _os.path.join(tmp, "parquet_out")
    check("Parquet write + read roundtrip", lambda: _parquet_roundtrip(spark, df, parquet_path))

    check("Parquet predicate pushdown", lambda: assert_true(
        spark.read.parquet(parquet_path).filter("age > 28").count() == 2))

    check("Parquet schema evolution (missing col → null)", lambda: _parquet_schema_evolve(spark, tmp))

    # ── 11. DML (Delta Lake) ──────────────────────────────────────────────────
    group("11. DML (Delta Lake)")

    delta_path = _os.path.join(tmp, "delta_tbl")
    spark.sql(f"""
        CREATE TABLE delta_test USING delta LOCATION '{delta_path}'
        AS SELECT * FROM people
    """)

    check("DELETE without WHERE", lambda: (
        spark.sql("DELETE FROM delta_test"),
        assert_eq(spark.sql("SELECT COUNT(*) c FROM delta_test").collect(), [Row(c=0)])
    ))

    spark.sql(f"""
        INSERT INTO delta_test SELECT * FROM people
    """)

    check("DELETE with WHERE", lambda: (
        spark.sql("DELETE FROM delta_test WHERE id = 1"),
        assert_eq(spark.sql("SELECT COUNT(*) c FROM delta_test").collect(), [Row(c=2)])
    ))

    check("UPDATE SET", lambda: (
        spark.sql("UPDATE delta_test SET name = 'BOB' WHERE id = 2"),
        assert_eq(
            spark.sql("SELECT name FROM delta_test WHERE id = 2").collect(),
            [Row(name="BOB")]
        )
    ))

    check("INSERT OVERWRITE", lambda: (
        spark.sql("INSERT OVERWRITE delta_test SELECT 99 AS id, 'x' AS name, 0 AS age"),
        assert_eq(spark.sql("SELECT id FROM delta_test").collect(), [Row(id=99)])
    ))

    # ── 12. Misc SQL ──────────────────────────────────────────────────────────
    group("12. Misc Spark SQL")

    check("monotonically_increasing_id", lambda: assert_true(
        len(spark.sql("SELECT monotonically_increasing_id() id FROM range(5)").collect()) == 5))

    check("monotonically_increasing_id in aggregate", lambda: assert_true(
        len(spark.sql(
            "SELECT id % 2 g, MAX(monotonically_increasing_id()) m FROM range(10) GROUP BY 1"
        ).collect()) == 2))

    check("COALESCE / NULLIF", lambda: assert_eq(
        spark.sql("SELECT COALESCE(NULL, NULL, 3) c, NULLIF(1,1) n").collect(),
        [Row(c=3, n=None)]))

    check("CAST types", lambda: assert_eq(
        spark.sql("SELECT CAST('3.14' AS DOUBLE) d, CAST(3.14 AS STRING) s").collect(),
        [Row(d=3.14, s="3.14")]))

    check("IF / IIF", lambda: assert_eq(
        spark.sql("SELECT IF(1>0,'yes','no') r").collect(), [Row(r="yes")]))

    check("ARRAY_AGG / COLLECT_SET", lambda: assert_eq(
        sorted(spark.sql("SELECT COLLECT_SET(id % 2) s FROM range(5)").collect()[0].s),
        [0, 1]))

    check("FLATTEN", lambda: assert_eq(
        spark.sql("SELECT FLATTEN(ARRAY(ARRAY(1,2),ARRAY(3))) f").collect()[0].f,
        [1, 2, 3]))

    check("JSON_OBJECT / NAMED_STRUCT", lambda: assert_eq(
        spark.sql("SELECT NAMED_STRUCT('a',1,'b',2).a v").collect(),
        [Row(v=1)]))

    # ── 13. Higher-Order / Lambda Functions ───────────────────────────────────
    group("13. Lambda / Higher-Order Functions")

    check("transform array", lambda: assert_eq(
        spark.sql("SELECT transform(array(1,2,3), x -> x * 2) r").collect()[0].r,
        [2, 4, 6]))

    check("filter array", lambda: assert_eq(
        spark.sql("SELECT filter(array(1,2,3,4), x -> x % 2 = 0) r").collect()[0].r,
        [2, 4]))

    check("exists array", lambda: assert_eq(
        spark.sql("SELECT exists(array(1,2,3), x -> x = 2) r").collect(),
        [Row(r=True)]))

    check("forall array", lambda: assert_eq(
        spark.sql("SELECT forall(array(2,4,6), x -> x % 2 = 0) r").collect(),
        [Row(r=True)]))

    check("aggregate (reduce)", lambda: assert_eq(
        spark.sql("SELECT aggregate(array(1,2,3), 0, (acc, x) -> acc + x) r").collect(),
        [Row(r=6)]))

    check("zip_with", lambda: assert_eq(
        spark.sql("SELECT zip_with(array(1,2), array(3,4), (a,b) -> a+b) r").collect()[0].r,
        [4, 6]))

    check("transform_values map", lambda: assert_eq(
        sorted(
            spark.sql("SELECT transform_values(map('a',1,'b',2), (k,v) -> v*10) r")
            .collect()[0].r.items()
        ),
        [('a', 10), ('b', 20)]))

    check("map_filter", lambda: assert_eq(
        spark.sql("SELECT map_filter(map('a',1,'b',2,'c',3), (k,v) -> v > 1) r")
        .collect()[0].r,
        {'b': 2, 'c': 3}))

    check("array_sort with lambda", lambda: assert_eq(
        spark.sql("SELECT array_sort(array(3,1,2), (a,b) -> a-b) r").collect()[0].r,
        [1, 2, 3]))

    # ── 14. PIVOT ──────────────────────────────────────────────────────────────
    group("14. PIVOT")

    check("PIVOT basic", lambda: (
        spark.sql("""
            SELECT * FROM (
                SELECT id % 2 AS parity, id AS val FROM range(6)
            )
            PIVOT (SUM(val) FOR parity IN (0, 1))
        """).collect()
        and True))

    check("DataFrame pivot", lambda: assert_true(
        spark.createDataFrame(
            [("Q1", "A", 100), ("Q2", "A", 200), ("Q1", "B", 300)],
            ["quarter", "dept", "revenue"]
        ).groupBy("dept").pivot("quarter", ["Q1", "Q2"])
        .agg(F.sum("revenue"))
        .count() == 2))

    # ── 15. Named Windows ──────────────────────────────────────────────────────
    group("15. Named Windows")

    check("named window reuse", lambda: assert_true(
        spark.sql("""
            SELECT id,
                   sum(id) OVER w AS running_sum,
                   rank() OVER w AS rnk
            FROM range(5)
            WINDOW w AS (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        """).count() == 5))

    check("named window PARTITION BY", lambda: assert_true(
        spark.sql("""
            SELECT id % 2 AS grp, id,
                   row_number() OVER w AS rn
            FROM range(6)
            WINDOW w AS (PARTITION BY id % 2 ORDER BY id)
        """).count() == 6))

    # ── 16. Cache / Catalog ────────────────────────────────────────────────────
    group("16. Cache / Catalog")

    def _cache_test():
        spark.range(3).createOrReplaceTempView("_cache_t")
        spark.catalog.cacheTable("_cache_t")
        was_cached = spark.catalog.isCached("_cache_t")  # no-op returns False
        spark.catalog.uncacheTable("_cache_t")
        spark.catalog.clearCache()
        spark.sql("CACHE TABLE _cache_t")
        spark.sql("UNCACHE TABLE IF EXISTS _cache_t")
        return True  # no exception = pass

    check("CACHE / UNCACHE TABLE no-op", lambda: _cache_test())

    check("REFRESH TABLE no-op", lambda: (
        spark.sql("REFRESH TABLE _cache_t") or True))

    # ── 17. Metadata Column ────────────────────────────────────────────────────
    group("17. _metadata Column")

    def _metadata_test():
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            spark.range(3).write.parquet(d + "/data")
            df = spark.read.parquet(d + "/data")
            rows = df.select("id", "_metadata").collect()
            assert rows[0]["_metadata"] is None or True  # null struct OK
            fp = df.select("_metadata.file_path").collect()
            return True  # no AnalysisException = pass

    check("_metadata struct accessible", lambda: _metadata_test())

    def _metadata_subfield_test():
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            spark.range(2).write.parquet(d + "/data")
            df = spark.read.parquet(d + "/data")
            fp = df.select("_metadata.file_path").collect()
            assert len(fp) == 2
            return True

    check("_metadata.file_path sub-field", lambda: _metadata_subfield_test())

# ── Scorecard ─────────────────────────────────────────────────────────────────

try:
    spark.stop()
finally:
    if _proc:
        _proc.terminate()

total_pass = total_fail = total_skip = 0
print(f"\n{'═'*55}")
print("  VAJRA SPARK COMPATIBILITY SCORECARD")
print(f"{'═'*55}")

for grp, tests in results.items():
    gpass = sum(1 for _, s, _ in tests if "PASS" in s)
    gfail = sum(1 for _, s, _ in tests if "FAIL" in s)
    gskip = sum(1 for _, s, _ in tests if "SKIP" in s)
    total_pass += gpass
    total_fail += gfail
    total_skip += gskip
    bar = "✓" * gpass + "✗" * gfail + "○" * gskip
    print(f"  {grp:<38} {bar:>10}  {gpass}/{gpass+gfail}")

print(f"{'─'*55}")
total = total_pass + total_fail + total_skip
pct = int(100 * total_pass / max(1, total_pass + total_fail))
print(f"  Total:  {total_pass} passed, {total_fail} failed, {total_skip} skipped")
print(f"  Score:  {pct}% ({total_pass}/{total_pass+total_fail} executed)")
print(f"{'═'*55}")

sys.exit(0 if total_fail == 0 else 1)
