use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common::config::AppConfig;
use tokio::sync::oneshot;

use crate::python::Modules;
use crate::spark::server::with_spark_connect_server;

const SPARK_RUN_SRC: &str = include_str!("../python/spark_run.py");

pub fn run_pyspark_script(file: String) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let (tx, rx) = oneshot::channel::<()>();
    let address = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let shutdown = async {
        let _ = rx.await;
    };
    with_spark_connect_server(config, address, shutdown, |addr| async move {
        let _tx = tx;
        if let Ok(python) = std::env::var("VAJRA_PYTHON") {
            // Preferred path: spawn a subprocess using the venv's Python (set by install.sh
            // wrapper). Decouples client Python version from the server's embedded Python 3.9,
            // allowing pyspark 3.5+ with Python 3.11/3.12 to run as the Spark Connect client.
            run_via_subprocess(&python, addr.port(), &file)?;
        } else {
            // Fallback: use PyO3 embedded Python (requires pyspark compatible with Python 3.9).
            Python::attach(|py| -> PyResult<_> {
                let runner = Modules::SPARK_RUN.load(py)?;
                runner
                    .getattr("run_pyspark_script")?
                    .call1((addr.port(), file))?;
                Ok(())
            })?;
        }
        Ok(())
    })?;
    Ok(())
}

fn run_via_subprocess(
    python: &str,
    port: u16,
    file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tmp = tempfile::Builder::new().suffix(".py").tempfile()?;
    tmp.write_all(SPARK_RUN_SRC.as_bytes())?;
    tmp.flush()?;
    let status = std::process::Command::new(python)
        .arg(tmp.path())
        .arg(port.to_string())
        .arg(file)
        .status()?;
    if !status.success() {
        return Err(
            format!("PySpark exited with code {}", status.code().unwrap_or(-1)).into(),
        );
    }
    Ok(())
}
