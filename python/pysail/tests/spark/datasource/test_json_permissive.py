"""Spark-parity tests for JSON permissive read modes.

Covers PERMISSIVE (with and without _corrupt_record column), DROPMALFORMED,
FAILFAST, and the columnNameOfCorruptRecord option.
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def mixed_json_file(tmp_path):
    """A JSON file containing two valid lines and one invalid (CSV) line."""
    path = tmp_path / "mixed.json"
    path.write_text('{"id":1,"name":"alice"}\nNOT_JSON\n{"id":3,"name":"carol"}\n')
    return str(path)


_SCHEMA_WITH_CORRUPT = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

_SCHEMA_WITHOUT_CORRUPT = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])


def test_permissive_with_corrupt_record(spark, mixed_json_file):
    """PERMISSIVE mode: malformed row captured in _corrupt_record column."""
    df = (
        spark.read
        .schema(_SCHEMA_WITH_CORRUPT)
        .option("mode", "PERMISSIVE")
        .json(mixed_json_file)
    )
    rows = {r.id: r for r in df.collect()}
    assert len(rows) == 3
    assert rows[1].name == "alice"
    assert rows[1]._corrupt_record is None
    assert rows[3].name == "carol"
    assert rows[3]._corrupt_record is None
    malformed = [r for r in df.collect() if r.id is None]
    assert len(malformed) == 1
    assert malformed[0]._corrupt_record == "NOT_JSON"


def test_permissive_without_corrupt_record(spark, mixed_json_file):
    """PERMISSIVE without _corrupt_record: malformed row becomes all-null."""
    df = (
        spark.read
        .schema(_SCHEMA_WITHOUT_CORRUPT)
        .option("mode", "PERMISSIVE")
        .json(mixed_json_file)
    )
    rows = df.collect()
    assert len(rows) == 3
    null_rows = [r for r in rows if r.id is None]
    assert len(null_rows) == 1


def test_dropmalformed(spark, mixed_json_file):
    """DROPMALFORMED: malformed rows are silently skipped."""
    df = (
        spark.read
        .schema(_SCHEMA_WITHOUT_CORRUPT)
        .option("mode", "DROPMALFORMED")
        .json(mixed_json_file)
    )
    rows = df.collect()
    assert len(rows) == 2
    assert sorted(r.id for r in rows) == [1, 3]


def test_failfast(spark, mixed_json_file):
    """FAILFAST: any malformed row raises an exception."""
    with pytest.raises(Exception):
        (
            spark.read
            .schema(_SCHEMA_WITHOUT_CORRUPT)
            .option("mode", "FAILFAST")
            .json(mixed_json_file)
            .collect()
        )


def test_custom_corrupt_record_column(spark, mixed_json_file):
    """columnNameOfCorruptRecord option wires malformed content to a custom column."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("bad_row", StringType(), True),
    ])
    df = (
        spark.read
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "bad_row")
        .json(mixed_json_file)
    )
    malformed = [r for r in df.collect() if r.id is None]
    assert len(malformed) == 1
    assert malformed[0].bad_row == "NOT_JSON"


def test_no_schema_infers_corrupt_record_column(spark, tmp_path):
    """No-schema PERMISSIVE read: _corrupt_record is added to inferred schema."""
    path = tmp_path / "noschema.json"
    path.write_text('{"id":1,"value":"a"}\nNOT_JSON\n{"id":3,"value":"c"}\n')
    df = spark.read.format("json").load(str(path))
    assert "_corrupt_record" in df.columns
    malformed = [r for r in df.collect() if r["id"] is None]
    assert len(malformed) == 1
    assert malformed[0]["_corrupt_record"] == "NOT_JSON"
