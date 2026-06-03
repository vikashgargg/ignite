"""Differential-testing workloads: real Spark-pipeline patterns.

Each workload is (name, [setup_sql...], query_sql). We run the SAME SQL on the
reference engine (real Apache Spark, JVM) and the candidate (Vajra via Spark
Connect), then assert byte-identical results. This proves a production pipeline
can switch to Vajra and get the same answers — the trust artifact.

Cover the COMMON PATH that real pipelines actually use, not edge cases.
"""

WORKLOADS = [
    # ── Basic projection / filter / literals ──────────────────────────────
    ("select_arithmetic", [], "SELECT 1+1 AS a, 2*3 AS b, 10/4 AS c, 7%3 AS d"),
    ("string_funcs", [],
     "SELECT upper('abc') u, length('hello') l, concat('a','b') c, substring('hello',2,3) s, trim('  x  ') t"),
    ("case_when", [],
     "SELECT id, CASE WHEN id%2=0 THEN 'even' ELSE 'odd' END AS parity FROM range(6)"),

    # ── Aggregations ──────────────────────────────────────────────────────
    ("agg_basic", [],
     "SELECT count(*) c, sum(id) s, avg(id) a, min(id) mn, max(id) mx FROM range(100)"),
    ("group_by", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',10),('a',20),('b',30),('b',40),('c',50) AS v(k,n)",
     ],
     "SELECT k, count(*) c, sum(n) s, avg(n) a FROM t GROUP BY k ORDER BY k"),
    ("count_distinct", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(1),(2),(3),(3),(3) AS v(x)",
     ],
     "SELECT count(DISTINCT x) cd, count(x) c FROM t"),
    ("having", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',10),('a',20),('b',30) AS v(k,n)",
     ],
     "SELECT k, sum(n) s FROM t GROUP BY k HAVING sum(n) > 15 ORDER BY k"),

    # ── Joins ─────────────────────────────────────────────────────────────
    ("inner_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1,'x'),(2,'y'),(3,'z') AS v(id,a)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (1,'p'),(2,'q') AS v(id,b)",
     ],
     "SELECT l.id, l.a, r.b FROM l JOIN r ON l.id=r.id ORDER BY l.id"),
    ("left_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1,'x'),(2,'y'),(3,'z') AS v(id,a)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (1,'p') AS v(id,b)",
     ],
     "SELECT l.id, l.a, r.b FROM l LEFT JOIN r ON l.id=r.id ORDER BY l.id"),

    # ── Window functions ──────────────────────────────────────────────────
    ("window_rank", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',10),('a',30),('a',20),('b',5),('b',15) AS v(k,n)",
     ],
     "SELECT k, n, row_number() OVER (PARTITION BY k ORDER BY n DESC) rn, "
     "rank() OVER (PARTITION BY k ORDER BY n DESC) rk FROM t ORDER BY k, rn"),
    ("window_agg", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',10),('a',20),('b',30) AS v(k,n)",
     ],
     "SELECT k, n, sum(n) OVER (PARTITION BY k) tot, "
     "lag(n) OVER (PARTITION BY k ORDER BY n) lg FROM t ORDER BY k, n"),

    # ── Dates / timestamps ────────────────────────────────────────────────
    ("date_funcs", [],
     "SELECT date_add(DATE '2026-01-15', 10) da, datediff(DATE '2026-02-01', DATE '2026-01-01') dd, "
     "year(DATE '2026-03-09') y, month(DATE '2026-03-09') m"),
    ("date_trunc_types", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (TIMESTAMP '2026-02-02 13:45:00') AS v(ts)",
     ],
     "SELECT date_trunc('YEAR', ts) y, date_trunc('MONTH', ts) m FROM t"),

    # ── Complex types ─────────────────────────────────────────────────────
    ("arrays", [],
     "SELECT array(1,2,3) a, size(array(1,2,3)) sz, array_contains(array(1,2,3),2) ac, "
     "sort_array(array(3,1,2)) sa"),
    ("lambda_hof", [],
     "SELECT transform(array(1,2,3), x -> x*10) t, filter(array(1,2,3,4), x -> x>2) f, "
     "aggregate(array(1,2,3,4), 0, (acc,x) -> acc+x) ag"),
    ("maps", [],
     "SELECT map('a',1,'b',2) m, map_keys(map('a',1,'b',2)) mk"),

    # ── Numeric / aggregate functions (the percentile fix etc.) ───────────
    ("percentile", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (10),(20),(30),(40),(50) AS v(val)",
     ],
     "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) pd, "
     "percentile_cont(0.5) WITHIN GROUP (ORDER BY val) pc FROM t"),

    # ── Subqueries / CTEs ─────────────────────────────────────────────────
    ("cte", [],
     "WITH a AS (SELECT id FROM range(5)) SELECT sum(id) s FROM a WHERE id > 1"),
    ("subquery_in", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(3),(4) AS v(x)",
     ],
     "SELECT x FROM t WHERE x IN (SELECT x FROM t WHERE x%2=0) ORDER BY x"),
    ("union_all", [],
     "SELECT id FROM range(3) UNION ALL SELECT id FROM range(2) ORDER BY id"),

    # ── More string functions ─────────────────────────────────────────────
    ("string_funcs2", [],
     "SELECT regexp_replace('hello123','[0-9]+','#') rr, split('a,b,c',',') sp, "
     "lpad('5',3,'0') lp, rpad('5',3,'0') rp, replace('aaa','a','b') rep, "
     "instr('hello','ll') ins, locate('l','hello') loc, reverse('abc') rev"),
    ("string_case_pad", [],
     "SELECT initcap('hello world') ic, ascii('A') asc_, char(65) ch, "
     "repeat('ab',3) rpt, translate('abc','ab','xy') tr"),

    # ── Math functions ────────────────────────────────────────────────────
    ("math_funcs", [],
     "SELECT round(3.14159,2) rnd, ceil(2.1) cl, floor(2.9) fl, abs(-5) ab, "
     "power(2,10) pw, sqrt(16.0) sq, mod(17,5) md, greatest(1,5,3) gt, least(1,5,3) lt"),
    ("math_funcs2", [],
     "SELECT sign(-3) sg, exp(0) ex, cast(round(ln(exp(2.0)),6) as double) l, "
     "pmod(-7,3) pm, factorial(5) fac"),

    # ── Null handling ─────────────────────────────────────────────────────
    ("null_handling", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,NULL),(NULL,2),(3,3) AS v(a,b)",
     ],
     "SELECT coalesce(a,b,-1) c, nvl(a,0) n, ifnull(b,0) i, nullif(a,3) nf FROM t ORDER BY c"),

    # ── Casts / type conversions ──────────────────────────────────────────
    ("casts", [],
     "SELECT cast('123' AS INT) i, cast(45.67 AS INT) ti, cast(1 AS STRING) s, "
     "cast('2026-03-09' AS DATE) d, cast('true' AS BOOLEAN) b"),
    ("try_cast", [],
     "SELECT try_cast('abc' AS INT) bad, try_cast('42' AS INT) good"),

    # ── More aggregates ───────────────────────────────────────────────────
    ("agg_stats", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1.0),(2.0),(3.0),(4.0),(5.0) AS v(x)",
     ],
     "SELECT round(stddev(x),6) sd, round(variance(x),6) vr, round(stddev_pop(x),6) sdp FROM t"),
    ("collect", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',1),('a',2),('b',3) AS v(k,n)",
     ],
     "SELECT k, sort_array(collect_list(n)) cl, sort_array(collect_set(n)) cs FROM t GROUP BY k ORDER BY k"),
    ("first_last", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',1),('a',2),('a',3) AS v(k,n)",
     ],
     "SELECT k, first(n) f, last(n) l, count_if(n>1) ci FROM t GROUP BY k ORDER BY k"),

    # ── distinct / dedup ──────────────────────────────────────────────────
    ("distinct", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,'a'),(1,'a'),(2,'b') AS v(x,y)",
     ],
     "SELECT DISTINCT x, y FROM t ORDER BY x, y"),

    # ── More dates ────────────────────────────────────────────────────────
    ("date_funcs2", [],
     "SELECT to_date('2026-03-09') td, date_format(DATE '2026-03-09','yyyy/MM/dd') df, "
     "last_day(DATE '2026-02-15') ld, dayofweek(DATE '2026-03-09') dw, "
     "weekofyear(DATE '2026-03-09') wy, quarter(DATE '2026-08-01') q"),

    # ── Structs ───────────────────────────────────────────────────────────
    ("structs", [],
     "SELECT named_struct('a',1,'b','x') ns, struct(1,2,3) st"),
    ("struct_field", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT named_struct('x',10,'y','hi') AS s",
     ],
     "SELECT s.x sx, s.y sy FROM t"),

    # ── explode / lateral ─────────────────────────────────────────────────
    ("explode", [],
     "SELECT explode(array(10,20,30)) AS e ORDER BY e"),
    ("posexplode", [],
     "SELECT pos, col FROM (SELECT posexplode(array('a','b','c'))) ORDER BY pos"),

    # ── Conditional / predicates ──────────────────────────────────────────
    ("predicates", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(5),(10),(15) AS v(x)",
     ],
     "SELECT x, x BETWEEN 5 AND 10 bt, x IN (1,15) inl, x IS NOT NULL nn FROM t ORDER BY x"),
]
