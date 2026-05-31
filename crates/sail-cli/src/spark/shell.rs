use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common::config::AppConfig;
use tokio::sync::oneshot;

use crate::python::Modules;
use crate::spark::server::with_spark_connect_server;

const SPARK_SHELL_SRC: &str = include_str!("../python/spark_shell.py");

pub fn run_pyspark_shell() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let (tx, rx) = oneshot::channel::<()>();
    let address = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let shutdown = async {
        // Shutdown when the Python shell exits (tx dropped).
        let _ = rx.await;
    };
    with_spark_connect_server(config, address, shutdown, |addr| async move {
        let _tx = tx;
        if let Ok(python) = std::env::var("VAJRA_PYTHON") {
            run_shell_via_subprocess(&python, addr.port())?;
        } else {
            Python::attach(|py| -> PyResult<_> {
                let shell = Modules::SPARK_SHELL.load(py)?;
                shell
                    .getattr("run_pyspark_shell")?
                    .call((addr.port(),), None)?;
                Ok(())
            })?;
        }
        Ok(())
    })?;
    Ok(())
}

fn run_shell_via_subprocess(python: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut tmp = tempfile::Builder::new().suffix(".py").tempfile()?;
    tmp.write_all(SPARK_SHELL_SRC.as_bytes())?;
    tmp.flush()?;
    // Ignore exit code — user may Ctrl+D/exit() normally.
    std::process::Command::new(python)
        .arg(tmp.path())
        .arg(port.to_string())
        .status()?;
    Ok(())
}
