mod mcp_server;
pub(crate) mod run;
mod server;
mod shell;

pub(crate) use mcp_server::{run_spark_mcp_server, McpSettings, McpTransport};
pub(crate) use server::{
    run_spark_connect_server, run_spark_connect_server_kubernetes_ha,
    run_spark_connect_server_local_cluster,
};
pub(crate) use shell::run_pyspark_shell;
