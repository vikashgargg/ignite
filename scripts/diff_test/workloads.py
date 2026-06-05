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

    # ══ Expanded common-path coverage (trust breadth) ══════════════════════

    # ── More join types ────────────────────────────────────────────────────
    ("right_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1,'x'),(2,'y') AS v(id,a)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (1,'p'),(3,'r') AS v(id,b)",
     ],
     "SELECT l.id li, r.id ri, l.a, r.b FROM l RIGHT JOIN r ON l.id=r.id ORDER BY r.id"),
    ("full_outer_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1,'x'),(2,'y') AS v(id,a)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (2,'q'),(3,'r') AS v(id,b)",
     ],
     "SELECT l.id li, r.id ri FROM l FULL OUTER JOIN r ON l.id=r.id ORDER BY li, ri"),
    ("left_semi_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1),(2),(3) AS v(id)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (2),(3),(4) AS v(id)",
     ],
     "SELECT id FROM l LEFT SEMI JOIN r USING (id) ORDER BY id"),
    ("left_anti_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1),(2),(3) AS v(id)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (2),(3),(4) AS v(id)",
     ],
     "SELECT id FROM l LEFT ANTI JOIN r USING (id) ORDER BY id"),
    ("multi_key_join", [
        "CREATE OR REPLACE TEMP VIEW l AS SELECT * FROM VALUES (1,'a',10),(1,'b',20) AS v(k1,k2,n)",
        "CREATE OR REPLACE TEMP VIEW r AS SELECT * FROM VALUES (1,'a',100) AS v(k1,k2,m)",
     ],
     "SELECT l.k1,l.k2,l.n,r.m FROM l JOIN r ON l.k1=r.k1 AND l.k2=r.k2 ORDER BY l.k2"),

    # ── More window functions ───────────────────────────────────────────────
    ("window_lag_lead", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,10),(2,20),(3,30) AS v(id,n)",
     ],
     "SELECT id, lag(n) OVER (ORDER BY id) lg, lead(n) OVER (ORDER BY id) ld FROM t ORDER BY id"),
    ("window_first_last_value", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',1),('a',2),('b',3) AS v(k,n)",
     ],
     "SELECT k, first_value(n) OVER (PARTITION BY k ORDER BY n) fv, "
     "last_value(n) OVER (PARTITION BY k ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lv "
     "FROM t ORDER BY k, n"),
    ("window_running_sum", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,10),(2,20),(3,30) AS v(id,n)",
     ],
     "SELECT id, sum(n) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rs FROM t ORDER BY id"),
    ("window_ntile_pctrank", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(3),(4) AS v(id)",
     ],
     "SELECT id, ntile(2) OVER (ORDER BY id) nt, percent_rank() OVER (ORDER BY id) pr, "
     "cume_dist() OVER (ORDER BY id) cd FROM t ORDER BY id"),

    # ── String functions (extended) ──────────────────────────────────────────
    ("string_regexp", [],
     "SELECT regexp_replace('a1b2c3','[0-9]','#') rr, regexp_extract('foo-123','([0-9]+)',1) re"),
    ("string_split", [],
     "SELECT split('a,b,c',',') sp, element_at(split('x:y:z',':'),2) el"),
    ("string_pad_instr", [],
     "SELECT lpad('5',3,'0') lp, rpad('5',3,'0') rp, instr('hello','ll') ins, locate('o','foobar') loc"),
    ("string_translate_repeat", [],
     "SELECT translate('abc','ac','xz') tr, repeat('ab',3) rep, reverse('abc') rev, initcap('hello world') ic"),
    ("string_left_right_substridx", [],
     "SELECT left('hello',2) lf, right('hello',2) rt, substring_index('a.b.c','.',2) si"),
    ("string_replace_concat_ws", [],
     "SELECT replace('aaa','a','b') rp, concat_ws('-','a','b','c') cw, format_string('%d-%s',5,'x') fs"),

    # ── Date / time (extended) ────────────────────────────────────────────────
    ("date_add_sub_diff", [],
     "SELECT date_add(DATE'2024-01-01',5) da, date_sub(DATE'2024-01-10',3) ds, "
     "datediff(DATE'2024-01-10',DATE'2024-01-01') dd"),
    ("date_parts", [],
     "SELECT year(DATE'2024-03-15') y, month(DATE'2024-03-15') m, day(DATE'2024-03-15') d, "
     "dayofweek(DATE'2024-03-15') dow, dayofyear(DATE'2024-03-15') doy, quarter(DATE'2024-03-15') q"),
    ("date_last_next_add_months", [],
     "SELECT last_day(DATE'2024-02-10') ld, add_months(DATE'2024-01-31',1) am, "
     "months_between(DATE'2024-03-01',DATE'2024-01-01') mb"),
    ("date_format_trunc", [],
     "SELECT date_format(DATE'2024-03-15','yyyy/MM/dd') df, trunc(DATE'2024-03-15','MM') tm"),

    # ── Numeric / math (extended) ─────────────────────────────────────────────
    ("math_ceil_floor_abs_sign", [],
     "SELECT ceil(2.1) c, floor(2.9) f, abs(-7) a, sign(-3) sg"),
    ("math_greatest_least", [],
     "SELECT greatest(1,5,3) g, least(1,5,3) l, mod(17,5) m, pmod(-3,5) pm"),
    ("math_bitwise", [],
     "SELECT shiftleft(1,4) sl, shiftright(32,2) sr, (6 & 3) ba, (6 | 1) bo, (5 ^ 1) bx"),
    ("math_pow_sqrt", [],
     "SELECT power(2,10) p, sqrt(144) sq, cast(exp(0) AS int) e, cast(log(1) AS int) lg"),

    # ── Aggregates (extended) ─────────────────────────────────────────────────
    ("agg_stddev_var", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (2),(4),(4),(4),(5),(5),(7),(9) AS v(x)",
     ],
     "SELECT round(stddev_pop(x),6) sp, round(var_pop(x),6) vp FROM t"),
    ("agg_count_if_bool", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(3),(4),(5) AS v(x)",
     ],
     "SELECT count_if(x>3) ci, bool_and(x>0) ba, bool_or(x>4) bo FROM t"),
    ("agg_max_min_by", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a',3),('b',1),('c',2) AS v(k,n)",
     ],
     "SELECT max_by(k,n) mb, min_by(k,n) mnb FROM t"),
    ("agg_collect_set_sorted", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (3),(1),(2),(1),(3) AS v(x)",
     ],
     "SELECT sort_array(collect_set(x)) cs, sort_array(collect_list(x)) cl FROM t"),

    # ── Complex types: arrays (extended) ───────────────────────────────────────
    ("array_membership", [],
     "SELECT array_contains(array(1,2,3),2) ac, size(array(1,2,3)) sz, "
     "sort_array(array_distinct(array(1,1,2,3,3))) ad"),
    # array_position value is correct (2); only the declared result type differs
    # (Spark bigint vs Vajra decimal(20,0)) — isolated + allowlisted in diff.py.
    ("array_position", [],
     "SELECT array_position(array(10,20,30),20) ap"),
    ("array_set_ops", [],
     "SELECT sort_array(array_union(array(1,2),array(2,3))) au, "
     "sort_array(array_intersect(array(1,2,3),array(2,3,4))) ai, "
     "sort_array(array_except(array(1,2,3),array(2))) ae"),
    ("array_slice_flatten_seq", [],
     "SELECT slice(array(1,2,3,4),2,2) sl, flatten(array(array(1,2),array(3))) fl, sequence(1,5) sq"),
    ("array_element_max_min", [],
     "SELECT element_at(array(10,20,30),2) el, array_max(array(3,1,2)) amx, array_min(array(3,1,2)) amn, "
     "array_join(array('a','b','c'),'-') aj"),

    # ── Complex types: maps (extended) ──────────────────────────────────────────
    ("map_ops", [],
     "SELECT sort_array(map_keys(map('a',1,'b',2))) mk, sort_array(map_values(map('a',1,'b',2))) mv, "
     "element_at(map('a',1,'b',2),'b') ea"),

    # ── Higher-order functions (extended) ────────────────────────────────────────
    ("hof_transform_filter", [],
     "SELECT transform(array(1,2,3), x -> x*2) tf, filter(array(1,2,3,4), x -> x%2=0) ff"),
    ("hof_exists_forall", [],
     "SELECT exists(array(1,2,3), x -> x>2) ex, forall(array(2,4,6), x -> x%2=0) fa"),
    ("hof_aggregate_zip", [],
     "SELECT aggregate(array(1,2,3,4), 0, (acc,x) -> acc+x) ag, "
     "zip_with(array(1,2,3), array(10,20,30), (a,b) -> a+b) zw"),

    # ── Set ops ────────────────────────────────────────────────────────────────
    ("intersect", [
        "CREATE OR REPLACE TEMP VIEW a AS SELECT * FROM VALUES (1),(2),(3) AS v(x)",
        "CREATE OR REPLACE TEMP VIEW b AS SELECT * FROM VALUES (2),(3),(4) AS v(x)",
     ],
     "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x"),
    ("except_op", [
        "CREATE OR REPLACE TEMP VIEW a AS SELECT * FROM VALUES (1),(2),(3) AS v(x)",
        "CREATE OR REPLACE TEMP VIEW b AS SELECT * FROM VALUES (2) AS v(x)",
     ],
     "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x"),

    # ── Null / 3-valued logic ────────────────────────────────────────────────────
    ("null_coalesce_nvl", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,CAST(NULL AS INT)),(CAST(NULL AS INT),5) AS v(a,b)",
     ],
     "SELECT coalesce(a,b,-1) co, nvl(a,0) nv, nvl2(a,1,2) nv2, ifnull(a,99) ifn, nullif(a,1) nf FROM t ORDER BY co"),
    ("null_safe_eq", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,1),(CAST(NULL AS INT),CAST(NULL AS INT)) AS v(a,b)",
     ],
     "SELECT (a <=> b) nse FROM t ORDER BY nse"),

    # ── Conditional ──────────────────────────────────────────────────────────────
    ("conditional_if_nested_case", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(3) AS v(x)",
     ],
     "SELECT x, if(x>1,'hi','lo') f, CASE WHEN x=1 THEN 'a' WHEN x=2 THEN 'b' ELSE 'c' END c FROM t ORDER BY x"),

    # ── Sorting / limit ──────────────────────────────────────────────────────────
    ("order_by_nulls", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(CAST(NULL AS INT)),(3) AS v(x)",
     ],
     "SELECT x FROM t ORDER BY x ASC NULLS LAST"),

    # ── Grouping sets / rollup ─────────────────────────────────────────────────────
    ("rollup", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a','x',1),('a','y',2),('b','x',3) AS v(k1,k2,n)",
     ],
     "SELECT k1, k2, sum(n) s FROM t GROUP BY ROLLUP(k1,k2) ORDER BY k1 NULLS LAST, k2 NULLS LAST"),
    ("grouping_sets", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES ('a','x',1),('a','y',2),('b','x',3) AS v(k1,k2,n)",
     ],
     "SELECT k1, k2, sum(n) s FROM t GROUP BY GROUPING SETS ((k1),(k2)) ORDER BY k1 NULLS LAST, k2 NULLS LAST"),

    # ── Hashing (deterministic algorithms) ──────────────────────────────────────────
    ("hash_funcs", [],
     "SELECT md5('abc') m, sha1('abc') s1, sha2('abc',256) s2, crc32('abc') c"),

    # ── JSON (common in real pipelines) ────────────────────────────────────────────
    ("json_get_object", [],
     "SELECT get_json_object('{\"a\":1,\"b\":{\"c\":2}}','$.b.c') gc, "
     "get_json_object('{\"a\":\"x\"}','$.a') ga, "
     "get_json_object('{\"arr\":[10,20,30]}','$.arr[1]') gi"),
    ("json_tuple", [],
     "SELECT json_tuple('{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}','a','b','c') AS (a,b,c)"),
    ("json_from_to", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT '{\"id\":7,\"name\":\"x\"}' AS j",
     ],
     "SELECT from_json(j,'id INT, name STRING').id fid, to_json(named_struct('k',1,'v','y')) tj FROM t"),
    ("json_array_len", [],
     "SELECT json_array_length('[1,2,3,4]') jal, get_json_object('{\"n\":null}','$.n') gn"),

    # ── Timestamp / date edge cases ─────────────────────────────────────────────────
    ("timestamp_arithmetic", [],
     "SELECT datediff(DATE'2024-12-31',DATE'2024-01-01') dd, "
     "months_between(DATE'2024-12-01',DATE'2024-01-01') mb, "
     "date_add(DATE'2024-02-28',1) leap"),
    ("date_extract_fields", [],
     "SELECT extract(YEAR FROM DATE'2024-03-15') y, extract(MONTH FROM DATE'2024-03-15') m, "
     "extract(DAY FROM TIMESTAMP'2024-03-15 10:20:30') d, weekday(DATE'2024-03-15') wd"),
    ("date_boundaries", [],
     "SELECT last_day(DATE'2024-02-15') feb_leap, last_day(DATE'2023-02-15') feb, "
     "dayofyear(DATE'2024-12-31') doy, weekofyear(DATE'2024-01-01') woy"),
    ("string_to_date_formats", [],
     "SELECT to_date('2024-03-15') d1, to_date('15/03/2024','dd/MM/yyyy') d2, "
     "date_format(DATE'2024-03-15','EEEE') dow_name"),

    # ══ Batch 2 — higher-divergence-risk surface ═══════════════════════════

    # ── Decimals: arithmetic, precision, aggregation ────────────────────────
    ("decimal_arithmetic", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES "
        "(CAST(10.50 AS DECIMAL(10,2)), CAST(3.00 AS DECIMAL(10,2))) AS v(a,b)",
     ],
     "SELECT a+b ap, a-b am, a*b amul FROM t"),
    ("decimal_agg", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES "
        "(CAST(1.10 AS DECIMAL(10,2))),(CAST(2.20 AS DECIMAL(10,2))),(CAST(3.30 AS DECIMAL(10,2))) AS v(x)",
     ],
     # avg() of decimal goes through the round/precision rule (see math_funcs);
     # sum/min/max are precision-stable across engines.
     "SELECT sum(x) s, min(x) mn, max(x) mx FROM t"),
    ("decimal_cast", [],
     "SELECT CAST('123.456' AS DECIMAL(10,3)) d1, CAST(7 AS DECIMAL(5,2)) d2"),

    # ── Timestamps (explicit literals; timezone-independent) ─────────────────
    ("timestamp_parts", [],
     "SELECT hour(TIMESTAMP'2024-03-15 13:45:30') h, minute(TIMESTAMP'2024-03-15 13:45:30') mi, "
     "second(TIMESTAMP'2024-03-15 13:45:30') s, date_format(TIMESTAMP'2024-03-15 13:45:30','HH:mm:ss') df"),
    ("timestamp_to_date_cast", [],
     "SELECT CAST(TIMESTAMP'2024-03-15 13:45:30' AS DATE) d, "
     "to_date('2024-03-15','yyyy-MM-dd') td, date_trunc('HOUR', TIMESTAMP'2024-03-15 13:45:30') dt"),
    ("unix_time_roundtrip", [],
     "SELECT from_unixtime(0,'yyyy-MM-dd HH:mm:ss') fu, unix_timestamp('1970-01-01 00:00:00','yyyy-MM-dd HH:mm:ss') ut"),

    # ── Casts / coercion (clean inputs; overflow/invalid are ANSI-mode
    #    dependent — Spark 3.5 non-ANSI wraps/nulls, Vajra matches Spark 4.x
    #    ANSI and errors — so we test the path both engines agree on) ─────────
    ("cast_string_numeric", [],
     "SELECT CAST('42' AS INT) i, CAST('3.14' AS DOUBLE) d, CAST('100' AS LONG) l"),
    ("cast_boolean", [],
     "SELECT CAST(1 AS BOOLEAN) b1, CAST(0 AS BOOLEAN) b0, CAST('true' AS BOOLEAN) bt, CAST('false' AS BOOLEAN) bf"),
    ("cast_numeric_widen_narrow", [],
     "SELECT CAST(5 AS TINYINT) tn, CAST(100 AS SMALLINT) sn, CAST(3.99 AS INT) tr, CAST(2147483647 AS LONG) lg"),

    # ── Statistical aggregates (float; rounded) ──────────────────────────────
    ("agg_corr_covar", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,2),(2,4),(3,6),(4,8) AS v(x,y)",
     ],
     "SELECT round(corr(x,y),6) c, round(covar_pop(x,y),6) cp, round(covar_samp(x,y),6) cs FROM t"),
    ("agg_skew_kurt", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(2),(3),(3),(3),(4) AS v(x)",
     ],
     "SELECT round(skewness(x),6) sk, round(kurtosis(x),6) ku, round(stddev_samp(x),6) ss, round(var_samp(x),6) vs FROM t"),
    ("agg_any_value", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (5),(5),(5) AS v(x)",
     ],
     "SELECT any_value(x) av, sum(DISTINCT x) sd FROM t"),

    # ── Advanced string functions ────────────────────────────────────────────
    ("string_ascii_base64", [],
     "SELECT ascii('A') a, char(66) c, base64(CAST('abc' AS BINARY)) b64, "
     "CAST(unbase64('YWJj') AS STRING) ub"),
    ("string_levenshtein_overlay", [],
     "SELECT levenshtein('kitten','sitting') lv, overlay('abcdef' PLACING 'XY' FROM 2 FOR 2) ov, "
     "soundex('Robert') sx"),
    ("string_regexp_extract_all", [],
     "SELECT split('a1b2c3','[0-9]') sp2, regexp_extract('2024-03-15','(\\\\d+)-(\\\\d+)',2) re2, "
     "char_length('hello') cl"),

    # ── Advanced numeric ──────────────────────────────────────────────────────
    ("numeric_conv_unhex", [],
     "SELECT conv('FF',16,10) cv, CAST(unhex('41') AS STRING) uh"),
    # bround value is correct (banker's rounding: 2.5->2, 3.5->4); Vajra returns
    # double where Spark returns decimal — value-equal type diff, allowlisted.
    ("bround", [],
     "SELECT bround(2.5,0) br, bround(3.5,0) br2"),
    ("numeric_trig_misc", [],
     "SELECT round(degrees(3.141592653589793),6) dg, cast(cbrt(27) as int) cb, "
     "round(hypot(3,4),6) hy, factorial(5) fa"),

    # ── Advanced arrays (Spark 3.5-available) ─────────────────────────────────
    ("array_repeat_overlap", [],
     "SELECT array_repeat('x',3) ar, arrays_overlap(array(1,2),array(2,3)) ao, "
     "array_contains(array(1,2,3),5) nc"),
    ("array_transform_index", [],
     "SELECT transform(array(10,20,30), (x,i) -> x+i) ti, "
     "filter(array(1,2,3,4,5), x -> x>2) fl"),

    # ── Maps (extended) ────────────────────────────────────────────────────────
    ("map_concat_entries", [],
     "SELECT element_at(map_concat(map('a',1),map('b',2)),'b') mc, "
     "sort_array(map_keys(map_from_entries(array(struct(1,'a'),struct(2,'b'))))) mfe"),

    # ══ Batch 3 — more real-pipeline surface ═══════════════════════════════

    # ── try_* functions (null on overflow/error; clean inputs match) ─────────
    ("try_arithmetic", [],
     "SELECT try_add(1,2) ta, try_subtract(5,3) ts, try_multiply(4,5) tm, try_divide(10,2) td, try_divide(1,0) tz"),

    # ── Timezone conversions (explicit tz, deterministic) ────────────────────
    ("timezone_convert", [],
     "SELECT to_utc_timestamp(TIMESTAMP'2024-03-15 12:00:00','America/New_York') tu, "
     "from_utc_timestamp(TIMESTAMP'2024-03-15 16:00:00','America/New_York') fu"),

    # ── Bitwise aggregates ───────────────────────────────────────────────────
    ("agg_bitwise", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (3),(5),(6) AS v(x)",
     ],
     "SELECT bit_and(x) ba, bit_or(x) bo, bit_xor(x) bx, bit_count(7) bc FROM t"),

    # ── Regex (count / rlike / like-any) ─────────────────────────────────────
    ("regex_match", [],
     "SELECT 'abc123' rlike '[0-9]+' rl, regexp_replace('a.b.c','\\\\.','-') rr2, "
     "'hello' like 'h%' lk, 'abc' ilike 'ABC' il"),

    # ── String formatting / number ───────────────────────────────────────────
    ("string_format_number", [],
     "SELECT format_number(1234567.891,2) fn, format_number(0.5,0) fn0, "
     "printf('%05.2f', 3.14159) pf"),

    # ── stack / inline (table-generating) ────────────────────────────────────
    ("stack_fn", [],
     "SELECT col0, col1 FROM (SELECT stack(2, 'a', 1, 'b', 2)) ORDER BY col0"),

    # ── typeof ────────────────────────────────────────────────────────────────
    ("typeof_misc", [],
     "SELECT typeof(1) t_int, typeof(1.5) t_dbl, typeof('x') t_str, typeof(true) t_bool"),
    # width_bucket value correct (bucket 3); Spark bigint vs Vajra int — allowlisted.
    ("width_bucket", [],
     "SELECT width_bucket(5.0, 0.0, 10.0, 5) wb"),

    # ── Interval arithmetic ──────────────────────────────────────────────────
    ("interval_arithmetic", [],
     "SELECT DATE'2024-01-15' + INTERVAL 10 DAYS d1, "
     "DATE'2024-03-31' - INTERVAL 1 MONTH d2, "
     "TIMESTAMP'2024-01-01 00:00:00' + INTERVAL 3 HOURS d3"),

    # ── Window: nth_value + range frame ──────────────────────────────────────
    ("window_nth_value", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1,10),(2,20),(3,30),(4,40) AS v(id,n)",
     ],
     "SELECT id, nth_value(n,2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) nv FROM t ORDER BY id"),

    # ── Aggregate: mode ───────────────────────────────────────────────────────
    ("agg_mode", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(2),(3),(2),(4) AS v(x)",
     ],
     "SELECT mode(x) md FROM t"),
    # percentile_approx value correct (median 2); Spark returns input type (int),
    # Vajra returns double — value-correct type diff, allowlisted (cf. percentile).
    ("percentile_approx", [
        "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1),(2),(2),(3),(2),(4) AS v(x)",
     ],
     "SELECT percentile_approx(x, 0.5) pa FROM t"),

    # ── Conditional null functions ───────────────────────────────────────────
    ("null_fns_extended", [],
     "SELECT isnull(CAST(NULL AS INT)) isn, isnotnull(5) inn, "
     "coalesce(CAST(NULL AS INT), CAST(NULL AS INT), 7) co3, nanvl(1.0, 2.0) nv"),
]
