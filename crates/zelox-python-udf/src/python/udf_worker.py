"""
Zelox UDF Worker — standalone Python subprocess for executing PySpark UDFs.

Protocol (stdin → python, python → stdout):

stdin:
  [4 LE] header_len
  [header_len] JSON: {"eval_type": int, "udf_kind": str,
                      "input_types": [...], "output_type": ...,
                      "number_rows": int,
                      "config": {"session_timezone": str, ...}}
  [4 LE] udf_payload_len
  [udf_payload_len] raw UDF payload bytes (Spark serializer format)
  [4 LE] arrow_batch_len  (0 = done / no-input batch)
  [arrow_batch_len] Arrow IPC stream bytes (RecordBatch of input columns)

stdout:
  [1] status: 0x00 = ok, 0x01 = error
  [4 LE] result_len
  [result_len] Arrow IPC stream bytes (single array) on ok
             | UTF-8 error message on error
"""
from __future__ import annotations

import io
import json
import os
import struct
import sys
import traceback
import types

import pyarrow as pa

# ---------------------------------------------------------------------------
# Embed spark.py source — injected by the Rust build via include_str!().
# The sentinel __SPARK_PY_SOURCE__ is replaced at runtime by Rust before
# spawning the worker (written to a temp file referenced via ZELOX_SPARK_PY).
# Alternatively, we accept the source on the env var ZELOX_SPARK_PY_SRC.
# ---------------------------------------------------------------------------

_SPARK_NS: dict = {}


def _load_spark_module() -> None:
    """Load spark.py from the path in ZELOX_SPARK_PY env var."""
    spark_py_path = os.environ.get("ZELOX_SPARK_PY")
    if spark_py_path and os.path.exists(spark_py_path):
        with open(spark_py_path, "r", encoding="utf-8") as fh:
            source = fh.read()
    else:
        # Fallback: try to import from the package
        try:
            import importlib.resources as _ir  # noqa: PLC0415

            ref = _ir.files("sail_python_udf").joinpath("spark.py")  # type: ignore[attr-defined]
            source = ref.read_text(encoding="utf-8")
        except Exception:
            raise RuntimeError(
                "Cannot locate spark.py. Set ZELOX_SPARK_PY to its path."
            )

    module = types.ModuleType("utils.spark")
    exec(compile(source, "spark.py", "exec"), module.__dict__)  # noqa: S102
    sys.modules["utils.spark"] = module
    _SPARK_NS.update(module.__dict__)


# ---------------------------------------------------------------------------
# Config shim — a simple namespace that duck-types PySparkUdfConfig
# ---------------------------------------------------------------------------

class _Config:
    def __init__(self, d: dict):
        self.session_timezone = d.get("session_timezone", "UTC")
        self.pandas_window_bound_types = d.get("pandas_window_bound_types")
        self.assign_columns_by_name = d.get("assign_columns_by_name", True)
        self.arrow_convert_safely = d.get("arrow_convert_safely", False)
        self.arrow_max_records_per_batch = int(d.get("arrow_max_records_per_batch", 10000))
        self.python_udf_pandas_conversion_enabled = d.get("python_udf_pandas_conversion_enabled", False)
        self.python_udtf_pandas_conversion_enabled = d.get("python_udtf_pandas_conversion_enabled", False)
        self.python_udf_pandas_int_to_decimal_coercion_enabled = d.get(
            "python_udf_pandas_int_to_decimal_coercion_enabled", False
        )
        self.binary_as_bytes = d.get("binary_as_bytes", True)
        # compat aliases used by spark.py
        self.pandas_grouped_map_assign_columns_by_name = self.assign_columns_by_name
        self.pandas_convert_to_arrow_array_safely = self.arrow_convert_safely


# ---------------------------------------------------------------------------
# Low-level I/O helpers
# ---------------------------------------------------------------------------

def _read_exact(stream: io.RawIOBase, n: int) -> bytes:
    if n == 0:
        return b""
    buf = bytearray(n)
    pos = 0
    while pos < n:
        chunk = stream.read(n - pos)
        if not chunk:
            raise EOFError(f"Unexpected EOF reading {n} bytes (got {pos})")
        buf[pos : pos + len(chunk)] = chunk
        pos += len(chunk)
    return bytes(buf)


def _read_u32_le(stream: io.RawIOBase) -> int:
    return struct.unpack("<I", _read_exact(stream, 4))[0]


def _write_u8(stream: io.RawIOBase, val: int) -> None:
    stream.write(struct.pack("B", val))


def _write_u32_le(stream: io.RawIOBase, val: int) -> None:
    stream.write(struct.pack("<I", val))


# ---------------------------------------------------------------------------
# UDF loading
# ---------------------------------------------------------------------------

def _load_udf(payload: bytes, eval_type: int):
    """Deserialise a PySpark UDF payload using pyspark.worker.read_udfs."""
    from pyspark.serializers import CPickleSerializer  # noqa: PLC0415
    from pyspark.worker import read_udfs  # noqa: PLC0415

    infile = io.BytesIO(payload)
    serializer = CPickleSerializer()
    udfs = read_udfs(serializer, infile, eval_type)
    return udfs[0]


# ---------------------------------------------------------------------------
# Wrapper construction
# ---------------------------------------------------------------------------

_KIND_TO_CLASS = {
    "batch": "PySparkBatchUdf",
    "arrow_batch": "PySparkArrowBatchUdf",
    "scalar_pandas": "PySparkScalarPandasUdf",
    "scalar_pandas_iter": "PySparkScalarPandasIterUdf",
    "scalar_arrow": "PySparkScalarArrowUdf",
    "scalar_arrow_iter": "PySparkScalarArrowIterUdf",
}


def _make_wrapper(udf_func, udf_kind: str, input_types_arrow, output_type_arrow, config: _Config):
    cls_name = _KIND_TO_CLASS.get(udf_kind)
    if cls_name is None:
        raise ValueError(f"Unknown UDF kind: {udf_kind!r}")
    cls = _SPARK_NS[cls_name]

    if udf_kind == "batch":
        return cls(udf_func, input_types_arrow, output_type_arrow)
    else:
        return cls(udf_func, config)


# ---------------------------------------------------------------------------
# Arrow type helpers
# ---------------------------------------------------------------------------

def _arrow_type_from_json(js) -> pa.DataType:
    """Reconstruct a pyarrow DataType from a JSON-serialised Arrow schema field."""
    if isinstance(js, str):
        # Simple string like "int32", "utf8", etc.
        field = pa.field("x", pa.lib.ensure_type(js))
        return field.type
    if isinstance(js, dict):
        # Encoded as a single-field schema JSON produced by pyarrow
        schema = pa.ipc.read_schema(pa.BufferReader(json.dumps(js).encode()))
        return schema.field(0).type
    raise TypeError(f"Cannot convert {js!r} to Arrow type")


def _deserialise_arrow_type(encoded) -> pa.DataType:
    """
    Types are sent as pyarrow IPC-serialised single-field schema bytes (base64-free,
    raw bytes) prefixed with a 4-byte LE length, already extracted before this call.
    Here `encoded` is raw bytes of a serialised pa.Schema with one field named '_'.
    """
    schema = pa.ipc.read_schema(pa.BufferReader(encoded))
    return schema.field(0).type


# ---------------------------------------------------------------------------
# Arrow IPC helpers
# ---------------------------------------------------------------------------

def _decode_ipc_array(data: bytes) -> pa.Array:
    """Read an Arrow IPC stream containing a single RecordBatch with one column."""
    reader = pa.ipc.open_stream(pa.BufferReader(data))
    batch = reader.read_next_batch()
    return batch.column(0)


def _encode_ipc_array(array: pa.Array) -> bytes:
    """Write an Arrow IPC stream with a single RecordBatch containing one column."""
    schema = pa.schema([pa.field("result", array.type)])
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, schema)
    batch = pa.record_batch([array], schema=schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def _decode_ipc_batch(data: bytes) -> list[pa.Array]:
    """Read an Arrow IPC stream and return a list of column arrays."""
    reader = pa.ipc.open_stream(pa.BufferReader(data))
    batch = reader.read_next_batch()
    return [batch.column(i) for i in range(batch.num_columns)]


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def main() -> None:
    _load_spark_module()

    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        # --- read header ---
        try:
            header_len = _read_u32_le(stdin)
        except EOFError:
            break  # clean shutdown

        header_bytes = _read_exact(stdin, header_len)
        header = json.loads(header_bytes.decode("utf-8"))

        eval_type: int = header["eval_type"]
        udf_kind: str = header["udf_kind"]
        number_rows: int = header["number_rows"]
        config_dict: dict = header.get("config", {})
        # input_types and output_type are sent as raw Arrow IPC schema bytes (length-prefixed)
        input_type_blobs: list = header.get("input_type_blobs", [])  # list of base64? no — see below
        output_type_blob: str = header.get("output_type_blob", "")  # see below

        # --- read UDF payload ---
        payload_len = _read_u32_le(stdin)
        payload_bytes = _read_exact(stdin, payload_len)

        # --- read Arrow batch (may be 0-column if no args) ---
        batch_len = _read_u32_le(stdin)
        if batch_len > 0:
            batch_bytes = _read_exact(stdin, batch_len)
            args = _decode_ipc_batch(batch_bytes)
        else:
            args = []

        # --- deserialise Arrow types (sent as raw IPC schema bytes, hex-encoded in JSON) ---
        import base64  # noqa: PLC0415

        input_types_arrow = []
        for blob_b64 in input_type_blobs:
            raw = base64.b64decode(blob_b64)
            input_types_arrow.append(_deserialise_arrow_type(raw))

        output_type_arrow: pa.DataType | None = None
        if output_type_blob:
            raw = base64.b64decode(output_type_blob)
            output_type_arrow = _deserialise_arrow_type(raw)

        config = _Config(config_dict)

        try:
            udf_func = _load_udf(payload_bytes, eval_type)
            wrapper = _make_wrapper(udf_func, udf_kind, input_types_arrow, output_type_arrow, config)
            result_array: pa.Array = wrapper(args, number_rows)
            result_bytes = _encode_ipc_array(result_array)

            _write_u8(stdout, 0x00)  # status ok
            _write_u32_le(stdout, len(result_bytes))
            stdout.write(result_bytes)
            stdout.flush()
        except Exception:  # noqa: BLE001
            err_msg = traceback.format_exc().encode("utf-8")
            _write_u8(stdout, 0x01)  # status error
            _write_u32_le(stdout, len(err_msg))
            stdout.write(err_msg)
            stdout.flush()


if __name__ == "__main__":
    main()
