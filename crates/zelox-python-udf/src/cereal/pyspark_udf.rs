use datafusion::arrow::datatypes::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{intern, Bound, IntoPyObject, PyAny, Python};
use zelox_common::spec;

use crate::cereal::{
    build_input_types_json, check_python_udf_version, get_pyspark_version, should_write_config,
    supports_kwargs, write_kwarg, PySparkVersion,
};
use crate::config::PySparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdfPayload;

impl PySparkUdfPayload {
    pub fn load<'py>(py: Python<'py>, data: &[u8]) -> PyUdfResult<Bound<'py, PyAny>> {
        let (eval_type, v) = data
            .split_at_checked(size_of::<i32>())
            .ok_or_else(|| PyUdfError::invalid("missing eval_type"))?;
        let eval_type = eval_type
            .try_into()
            .map_err(|e| PyValueError::new_err(format!("eval_type bytes: {e}")))?;
        let eval_type = i32::from_be_bytes(eval_type);
        let infile = PyModule::import(py, intern!(py, "io"))?
            .getattr(intern!(py, "BytesIO"))?
            .call1((v,))?;
        let serializer = PyModule::import(py, intern!(py, "pyspark.serializers"))?
            .getattr(intern!(py, "CPickleSerializer"))?
            .call0()?;
        let worker = PyModule::import(py, intern!(py, "pyspark.worker"))?;
        let read_udfs = worker.getattr(intern!(py, "read_udfs"))?;
        // PySpark 4.2 splits the two config blocks out of the payload and passes
        // them to `read_udfs`; earlier versions read them inside `read_udfs`.
        let tuple = if get_pyspark_version()?.is_v4_2() {
            let runner_conf = worker
                .getattr(intern!(py, "RunnerConf"))?
                .call1((&infile,))?;
            let eval_conf = worker.getattr(intern!(py, "EvalConf"))?.call1((&infile,))?;
            read_udfs.call1((serializer, infile, eval_type, runner_conf, eval_conf))?
        } else {
            read_udfs.call1((serializer, infile, eval_type))?
        };
        tuple
            .get_item(0)?
            .into_pyobject(py)
            .map_err(|e| PyUdfError::PythonError(e.into()))
    }

    pub fn build(
        python_version: &str,
        command: &[u8],
        eval_type: spec::PySparkUdfType,
        arg_offsets: &[usize],
        input_types: &[DataType],
        // Per-argument kwarg name: None for positional, Some(key) for keyword
        kwarg_names: &[Option<String>],
        config: &PySparkUdfConfig,
    ) -> PyUdfResult<Vec<u8>> {
        check_python_udf_version(python_version)?;
        let pyspark_version = get_pyspark_version()?;
        let mut data: Vec<u8> = Vec::new();

        data.extend(i32::from(eval_type).to_be_bytes());

        if pyspark_version.is_v4_2() {
            // PySpark 4.2: the worker `main()` reads two config blocks unconditionally,
            // right after `eval_type` — `RunnerConf` then `EvalConf`. Carry the runner
            // config (timezone/safecheck/arrow settings) and an empty eval config
            // (populated only for stateful UDFs, which are handled elsewhere).
            crate::cereal::write_conf_block(&mut data, &config.to_key_value_pairs());
            crate::cereal::write_conf_block(&mut data, &[]);
        } else if should_write_config(eval_type) {
            crate::cereal::write_conf_block(&mut data, &config.to_key_value_pairs());
        }

        // PySpark 4.1 reads a separate input-types block for ArrowBatched UDFs.
        // PySpark 4.0.x does not read input types; 4.2 derives them from each UDF's
        // return type instead, so no block is written for either.
        if matches!(pyspark_version, PySparkVersion::V4_1)
            && matches!(eval_type, spec::PySparkUdfType::ArrowBatched)
        {
            let schema_json = build_input_types_json(input_types)?;
            data.extend((schema_json.len() as i32).to_be_bytes());
            data.extend(schema_json.as_bytes());
        }

        // 4.0/4.1 read a per-stream profiling byte here; 4.2 moved profiling into
        // RunnerConf and dropped the byte.
        if pyspark_version.is_v4() && !pyspark_version.is_v4_2() {
            data.extend(0u8.to_be_bytes()); // profiling is not enabled
        }

        data.extend(1i32.to_be_bytes()); // number of UDFs

        let num_arg_offsets: i32 = arg_offsets
            .len()
            .try_into()
            .map_err(|e| PyUdfError::invalid(format!("num args: {e}")))?;
        data.extend(num_arg_offsets.to_be_bytes()); // number of argument offsets

        // PySpark 4.2 `read_single_udf` reads a keyword-argument flag byte after *every*
        // argument offset, regardless of whether the UDF type supports kwargs; 4.0/4.1 only
        // read it for kwarg-capable types. Always emit the flag on 4.2 (positional args
        // write a `0` byte via `write_kwarg`).
        let allow_kwargs = pyspark_version.is_v4_2()
            || (pyspark_version.is_v4() && supports_kwargs(eval_type));

        for (i, offset) in arg_offsets.iter().enumerate() {
            let offset: i32 = (*offset)
                .try_into()
                .map_err(|e| PyUdfError::invalid(format!("arg offset: {e}")))?;
            data.extend(offset.to_be_bytes()); // argument offset
            if allow_kwargs {
                write_kwarg(&mut data, kwarg_names, i);
            }
        }

        data.extend(1i32.to_be_bytes()); // number of functions
        data.extend((command.len() as i32).to_be_bytes()); // length of the function
        data.extend_from_slice(command);

        // PySpark 4.2 `read_single_udf` reads an 8-byte `result_id` (a `read_long`)
        // after the command; earlier versions do not.
        if pyspark_version.is_v4_2() {
            data.extend(0i64.to_be_bytes()); // result_id (unused without profiling)
        }

        Ok(data)
    }
}
