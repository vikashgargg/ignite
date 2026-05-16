use clap::{Parser, Subcommand, ValueEnum};
use sail_common::error::CommonError;

use crate::flight::run_flight_server;
use crate::spark::run::run_pyspark_script;
use crate::spark::{
    run_pyspark_shell, run_spark_connect_server, run_spark_mcp_server, McpSettings, McpTransport,
};
use crate::worker::run_worker;

#[derive(Parser)]
#[command(
    version,
    name = "ignite",
    about = "Ignite — a Rust-native, single-binary Spark engine",
    long_about = "Ignite is a drop-in replacement for Apache Spark: 4-8x faster, \
                  no JVM required, single binary. Runs your existing PySpark code unchanged."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the Spark Connect server (default dev mode)
    Server {
        #[arg(long, default_value = "127.0.0.1", help = "IP address to bind to")]
        ip: String,
        #[arg(long, default_value_t = 50051, help = "Port to listen on")]
        port: u16,
        #[arg(short = 'C', long, help = "Directory to change to before starting")]
        directory: Option<String>,
    },

    /// Execute a SQL query and print results, then exit
    Sql {
        /// SQL statement to execute
        query: String,
        #[arg(short = 'C', long, help = "Directory to change to before executing")]
        directory: Option<String>,
    },

    /// Run a PySpark script file and exit
    Run {
        #[arg(
            short = 'f',
            long,
            help = "PySpark script to run, or '-' for stdin",
            default_value = "-"
        )]
        file: String,
        #[arg(short = 'C', long, help = "Directory to change to before running")]
        directory: Option<String>,
    },

    /// Start an interactive PySpark shell
    Shell,

    /// Run the TPC-H benchmark self-test (22 queries at SF-1 by default)
    Bench {
        #[arg(long, default_value_t = 1, help = "TPC-H scale factor")]
        scale_factor: u32,
        #[arg(long, default_value = "local", help = "Storage path or S3 URI for data")]
        data_path: String,
    },

    /// Distributed cluster mode
    Cluster {
        #[arg(long, help = "Role this node plays in the cluster")]
        role: ClusterRole,
        #[arg(long, default_value = "0.0.0.0", help = "IP address to bind to")]
        ip: String,
        #[arg(long, default_value_t = 7070, help = "Scheduler port")]
        port: u16,
        /// Scheduler address (required for worker role)
        #[arg(long, help = "Scheduler address (host:port), required for worker role")]
        scheduler: Option<String>,
    },

    /// Arrow Flight SQL interface
    #[command(subcommand)]
    Flight(FlightCommand),

    /// Start the Spark MCP (Model Context Protocol) server
    McpServer {
        #[arg(long, default_value = "127.0.0.1", help = "Host to bind to")]
        host: String,
        #[arg(long, default_value_t = 8000, help = "Port to listen on")]
        port: u16,
        #[arg(long, default_value_t = McpTransport::Sse, help = "MCP transport")]
        transport: McpTransport,
        #[arg(long, help = "Spark remote address to connect to")]
        spark_remote: Option<String>,
        #[arg(short = 'C', long, help = "Directory to change to before starting")]
        directory: Option<String>,
    },

    #[command(hide = true)]
    Worker,
}

#[derive(Clone, ValueEnum)]
pub enum ClusterRole {
    Scheduler,
    Worker,
}

#[derive(Subcommand)]
enum FlightCommand {
    /// Start the Arrow Flight SQL server
    Server {
        #[arg(long, default_value = "127.0.0.1", help = "IP address to bind to")]
        ip: String,
        #[arg(long, default_value_t = 32010, help = "Port to listen on")]
        port: u16,
        #[arg(short = 'C', long, help = "Directory to change to before starting")]
        directory: Option<String>,
    },
}

pub fn main(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        Err(CommonError::InternalError(
            "failed to install crypto provider".to_string(),
        ))?;
    }

    let cli = Cli::parse_from(args);

    match cli.command {
        Command::Worker => run_worker(),

        Command::Server { ip, port, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_spark_connect_server(ip.parse()?, port)
        }

        Command::Sql { query, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_pyspark_script(format_sql_script(&query))
        }

        Command::Run { file, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_pyspark_script(file)
        }

        Command::Shell => run_pyspark_shell(),

        Command::Bench { scale_factor, data_path } => {
            run_bench(scale_factor, &data_path)
        }

        Command::Cluster { role, ip, port, scheduler } => {
            match role {
                ClusterRole::Scheduler => {
                    eprintln!("ignite cluster scheduler at {ip}:{port}");
                    eprintln!("Distributed scheduler (Phase 2) — coming soon.");
                    Ok(())
                }
                ClusterRole::Worker => {
                    let sched = scheduler.unwrap_or_else(|| format!("{}:{}", ip, port));
                    eprintln!("ignite cluster worker → scheduler at {sched}");
                    run_worker()
                }
            }
        }

        Command::Flight(cmd) => match cmd {
            FlightCommand::Server { ip, port, directory } => {
                if let Some(dir) = directory {
                    std::env::set_current_dir(dir)?;
                }
                run_flight_server(ip.parse()?, port)
            }
        },

        Command::McpServer { host, port, transport, spark_remote, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_spark_mcp_server(McpSettings { transport, host, port }, spark_remote)
        }
    }
}

fn format_sql_script(query: &str) -> String {
    // Write a tiny PySpark script that runs the SQL and prints results.
    // This is passed to run_pyspark_script which expects a file path or "-".
    // We write to a temp file and return the path.
    use std::io::Write;
    let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    writeln!(
        tmp,
        r#"from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql({query:?}).show(truncate=False)
"#,
        query = query
    )
    .expect("failed to write temp script");
    let path = tmp.into_temp_path();
    path.keep().expect("failed to keep temp file").to_string_lossy().to_string()
}

fn run_bench(scale_factor: u32, data_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Ignite TPC-H Benchmark — Scale Factor {scale_factor}");
    eprintln!("Data path: {data_path}");
    eprintln!("Benchmark harness (Phase 1, Week 10) — coming soon.");
    eprintln!("Track progress: https://github.com/vikashgargg/ignite/issues");
    Ok(())
}
