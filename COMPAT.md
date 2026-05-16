# Ignite — Spark SQL Compatibility Triage

> Generated: 2026-05-17 (Day 3 audit)  
> Source: `python/pysail/tests/spark/` skip/xfail markers + source code scan  
> Note: GitHub Issues are disabled on this repo — tracking here instead.

---

## Summary

| Category | Tests affected | Priority | Status |
|---|---|---|---|
| [C1] DML — DELETE / UPDATE | 8 | P1 | Open |
| [C2] monotonically_increasing_id in aggregates | 5 | P1 | Open |
| [C3] UDF implicit type casting | 3 | P2 | Open |
| [C4] FILTER clause in aggregations | 2 | P1 | Open |
| [C5] JSON reader — `_corrupt_record` compat | 4 | P2 | Open |
| [C6] INSERT OVERWRITE | 1 | P1 | Open |
| [C7] GeometryType / GeographyType | 2 | P3 | Open |
| [C8] Persistent tables default to EXTERNAL | 2 | P2 | Open |
| [C9] Structured Streaming (readStream) | 1 | Phase 2 | Deferred |
| [C10] `monotonically_increasing_id` in GROUP BY projection | 5 | P1 | Open |

**Total skipped/xfail tests audited:** 94 annotations across 34 files  
**Ignite-only tests (correct behaviour that JVM Spark doesn't match):** ~15 (not bugs)

---

## C1 — DELETE FROM / UPDATE SET not implemented

**Priority:** P1 — needed for Delta Lake ETL workloads and TPC-DS  
**Files:** `test_dml.py`  
**Count:** 8 tests

### Failing operations
```sql
DELETE FROM table_name [WHERE condition]
UPDATE table_name [AS alias] SET col = expr [WHERE condition]
```

### Root cause
SQL parser recognises the statements. Execution layer has no physical plan. `delta-rs` supports both via `DeltaOps::delete()` and `DeltaOps::update()`.

### Fix path
1. Implement `DeleteFromExec` in `sail-execution` using `DeltaOps::delete()`
2. Implement `UpdateExec` in `sail-execution` using `DeltaOps::update()`
3. Route from `sail-plan` physical planner
4. Remove skips from `test_dml.py`

---

## C2 / C10 — monotonically_increasing_id in aggregate / GROUP BY contexts

**Priority:** P1  
**Files:** `test_monotonic_id.py`  
**Count:** 5 tests

### Failing operations
```sql
-- monotonically_increasing_id() inside MAX() in GROUP BY
SELECT id, max(monotonically_increasing_id()) AS id1 FROM range(10) GROUP BY id

-- monotonically_increasing_id() inside HAVING
SELECT id FROM range(10) GROUP BY id HAVING max(monotonically_increasing_id()) > 0

-- monotonically_increasing_id() with ORDER BY across partitions
SELECT monotonically_increasing_id() FROM range(10) ORDER BY 1 DESC
```

### Root cause
`monotonically_increasing_id()` is a non-deterministic function that must be evaluated once per row before aggregation. The current implementation likely re-evaluates inside aggregate context or loses partition offset information when used with ORDER BY.

### Fix path
1. Mark `monotonically_increasing_id` as `VolatilityClass::Volatile` in DataFusion
2. Add a pre-aggregation projection pass that materialises volatile expressions into columns
3. Ensure ORDER BY respects partition-relative IDs correctly

---

## C3 — UDF implicit type casting (Arrow + non-Arrow modes)

**Priority:** P2  
**Files:** `test_udf.py`  
**Count:** 3 tests

### Failing operations
```python
# Default UDF return type is "string" — should auto-cast INT → STRING
udf(lambda x: x)("int_col")  # expects "1" not 1

# BINARY return type — should return None for incompatible types
udf(lambda x: x, returnType="binary")("int_col")  # expects None

# BINARY from string — should return bytearray
udf(lambda x: x, returnType="binary")("str_col")  # expects bytearray(b"1")
```

### Root cause
Sail's PyO3 UDF bridge doesn't apply Spark's implicit output type coercion rules when the Python return value doesn't match the declared `returnType`. Spark does a best-effort cast; Sail likely errors or returns the raw value.

### Fix path
1. In `sail-python-udf`, after calling the Python fn and converting the result RecordBatch, apply a `CastExec` matching the declared `returnType`
2. Handle `None` semantics for BINARY type when input is non-bytes-compatible
3. Test both `spark.sql.execution.pythonUDF.arrow.enabled=true` and `false`

---

## C4 — FILTER clause in aggregate functions

**Priority:** P1 — SQL:2003 standard, widely used  
**Files:** `test_group_by.py`  
**Count:** 2 tests

### Failing operation
```sql
SELECT id, SUM(quantity) FILTER (WHERE car_model IN ('Honda Civic', 'Honda CRV'))
FROM dealer
GROUP BY id
```

### Root cause
The `FILTER (WHERE ...)` clause on aggregate functions is parsed but not lowered into a physical aggregate plan. DataFusion supports this via `AggregateExpr::filter`.

### Fix path
1. In `sail-plan`, when converting `AggregateFunction` with a `filter` field, pass the filter expression to `DataFusion`'s `AggregateExpr`
2. Verify `sail-sql-parser` correctly parses the `FILTER (WHERE ...)` syntax
3. Remove skips in `test_group_by.py`

---

## C5 — JSON reader: `_corrupt_record` vs hard error

**Priority:** P2  
**Files:** `datasource/test_mixed_directory.py`, others  
**Count:** 4 tests

### Behaviour difference
- **Spark:** Malformed JSON lines are written to a `_corrupt_record` column; valid lines are processed
- **Ignite (Sail):** Malformed JSON causes a hard error / the entire file read fails

### Fix path
1. In `sail-data-source` JSON reader, wrap per-row parse errors
2. On JSON parse failure, emit a `null` row with `_corrupt_record` populated (matching Spark's `PERMISSIVE` mode default)
3. Respect `spark.sql.columnNameOfCorruptRecord` config key

---

## C6 — INSERT OVERWRITE not supported

**Priority:** P1  
**Files:** `test_write_table.py`  
**Count:** 1 test (skipif not Ignite)

### Failing operation
```sql
INSERT OVERWRITE table_name SELECT ...
```

### Root cause
`INSERT OVERWRITE` is distinct from `INSERT INTO` — it replaces partition data or the whole table. Delta Lake supports this via `DeltaOps::write()` with `SaveMode::Overwrite`.

### Fix path
1. Parse `INSERT OVERWRITE` → set `overwrite: true` in the logical plan node
2. In `sail-execution` writer, use `SaveMode::Overwrite` when `overwrite=true`

---

## C7 — GeometryType / GeographyType not implemented

**Priority:** P3 — niche, not in top-80% usage  
**Files:** `test_toddl.py`  
**Count:** 2 xfail tests

### Note
These are Databricks-specific spatial types not in open-source Spark. Low priority.

---

## C8 — Persistent tables default to EXTERNAL

**Priority:** P2  
**Files:** `test_write_table.py`, `test_catalog.py`  
**Count:** 2 tests

### Behaviour difference
- **Spark:** `CREATE TABLE` / `df.write.saveAsTable()` creates MANAGED tables (data under warehouse dir)
- **Ignite:** Creates EXTERNAL tables (data at user-specified or default external path)

### Root cause
The default `TableType` in `sail-catalog-memory` / `sail-catalog-system` is set to `External` rather than `Managed`.

### Fix path
1. Check `CreateTableStatement.table_type` defaulting in `sail-plan`
2. When no `LOCATION` is specified and `USING` is a managed format, default to `TableType::Managed`
3. Map managed table path to `spark.sql.warehouse.dir` config value

---

## C9 — Structured Streaming (readStream) — DEFERRED to Phase 2

**Files:** `test_basic.py::test_stream`  
**Note:** `spark.readStream` is Phase 2 scope per PLAN.md §3.9. Expected skip.

---

## Gold Test Results (Rust)

**Ran:** `cargo test -p sail-gold-test`  
**Result:** ✅ All passing

Categories covered by gold tests:
- SQL plan parsing (DDL, DML, SELECT, JOIN, GROUP BY, ORDER BY, hints, set ops)
- Expression parsing (cast, case, date, interval, window, numeric, string)
- Function deserialization (agg, array, bitwise, collection, conditional, conversion, datetime, hash, json, lambda, map, math, misc, predicate, string, struct)
- Data type mapping
- Table schema

---

## Workspace Unit Tests

**Ran:** `cargo test --workspace --lib`  
**Infrastructure note:** Local Mac requires `RUSTFLAGS="-L <CLT-Python-lib>"` due to missing `python3-config` on system Python 3.9 (no Xcode). CI runs fine (uses properly installed Python 3.11).

---

## Priority Queue for Phase 1 Fixes

| # | Issue | Effort | Impact |
|---|---|---|---|
| 1 | C4 — FILTER in aggregates | Small (DataFusion already supports it) | High — SQL:2003 standard |
| 2 | C6 — INSERT OVERWRITE | Small | High — common ETL pattern |
| 3 | C1 — DELETE / UPDATE | Medium | High — Delta Lake DML |
| 4 | C10 — monotonically_increasing_id agg | Medium | High — common id pattern |
| 5 | C8 — Managed table default | Small | Medium — affects catalog tests |
| 6 | C5 — JSON _corrupt_record | Medium | Medium — data quality pattern |
| 7 | C3 — UDF type casting | Medium | Medium — affects UDF users |
| 8 | C7 — GeometryType | Large | Low — Databricks-only |
