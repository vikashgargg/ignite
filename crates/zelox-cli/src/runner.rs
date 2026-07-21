use clap::{Parser, Subcommand, ValueEnum};
use zelox_common::error::CommonError;

use crate::flight::run_flight_server;
use crate::spark::run::run_pyspark_script;
use crate::spark::{
    run_pyspark_shell, run_spark_connect_server, run_spark_connect_server_kubernetes_ha,
    run_spark_connect_server_local_cluster, run_spark_mcp_server, McpSettings, McpTransport,
};
use crate::worker::run_worker;

#[derive(Parser)]
#[command(
    version,
    name = "zelox",
    about = "Zelox (वज्र) — thunderbolt-fast, single-binary Spark engine",
    long_about = "Zelox is a drop-in replacement for Apache Spark: 5-10x faster, \
                  no JVM required, single static binary. Runs your existing PySpark code \
                  unchanged via the Spark Connect protocol. Apple Container + Kubernetes native."
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
        #[arg(
            long,
            help = "Execution mode: local | local-cluster | kubernetes-cluster (overrides ZELOX_MODE)"
        )]
        mode: Option<String>,
        #[arg(
            long,
            default_value_t = 0,
            help = "Number of local workers for local-cluster mode (0 = config default)"
        )]
        workers: usize,
        #[arg(
            long,
            default_value_t = false,
            help = "Enable Kubernetes Lease-based leader election for scheduler HA (kubernetes-cluster mode only)"
        )]
        ha: bool,
        #[arg(
            long,
            help = "Require clients to present this Bearer token in every gRPC call (also settable via ZELOX_AUTH__TOKEN env var)"
        )]
        auth_token: Option<String>,
        #[arg(
            long,
            help = "Path to PEM-encoded server TLS certificate (enables TLS; also ZELOX_AUTH__TLS__CERT)"
        )]
        tls_cert: Option<String>,
        #[arg(
            long,
            help = "Path to PEM-encoded server TLS private key (also ZELOX_AUTH__TLS__KEY)"
        )]
        tls_key: Option<String>,
        #[arg(
            long,
            help = "Path to PEM-encoded CA certificate for client verification (enables mTLS; also ZELOX_AUTH__TLS__CA)"
        )]
        tls_ca: Option<String>,
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
        #[arg(
            long,
            default_value = "local",
            help = "Storage path or S3 URI for data"
        )]
        data_path: String,
    },

    /// Distributed cluster mode
    Cluster {
        #[arg(long, help = "Role this node plays in the cluster")]
        role: ClusterRole,
        #[arg(long, default_value = "0.0.0.0", help = "IP address to bind to")]
        ip: String,
        #[arg(
            long,
            default_value_t = 50051,
            help = "Spark Connect port (scheduler role)"
        )]
        port: u16,
        #[arg(long, help = "Scheduler address (host:port), required for worker role")]
        scheduler: Option<String>,
        #[arg(
            long,
            default_value_t = 0,
            help = "Number of local workers to launch (0 = config default, scheduler role only)"
        )]
        workers: usize,
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

/// Optional CPU profiler (`ZELOX_PPROF_SECS=<n>` [`ZELOX_PPROF_OUT=<path>`]): sample the whole process for
/// `<n>`s then dump folded stacks (headless flamegraph input) to instrument the shuffle/source hotspot
/// without a native profiler. Zero-cost when unset. Called by BOTH the server (in-process local-cluster
/// workers) and the worker (distributed kind/EKS pods), so the actual shuffle CPU is always captured.
fn maybe_start_pprof() {
    let Ok(Some(secs)) = std::env::var("ZELOX_PPROF_SECS").map(|s| s.parse::<u64>().ok()) else {
        return;
    };
    if secs == 0 {
        return;
    }
    match pprof::ProfilerGuardBuilder::default()
        .frequency(197)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(guard) => {
            let out =
                std::env::var("ZELOX_PPROF_OUT").unwrap_or_else(|_| "/tmp/zelox_prof.folded".to_string());
            std::thread::spawn(move || {
                use std::fmt::Write as _;
                // Dump PERIODICALLY (cumulative) every `secs`, so an ephemeral worker pod that dies mid-run
                // still logged at least one folded-stack dump covering the shuffle (the last one is richest).
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(secs));
                    let Ok(report) = guard.report().build() else {
                        continue;
                    };
                    let mut s = String::new();
                    for (frames, count) in report.data.iter() {
                        let mut stack: Vec<String> = Vec::new();
                        for frame in frames.frames.iter() {
                            for sym in frame.iter() {
                                stack.push(sym.to_string());
                            }
                        }
                        stack.reverse();
                        let _ = writeln!(s, "{} {count}", stack.join(";"));
                    }
                    let _ = std::fs::write(&out, &s);
                    // ALSO emit to stderr so the folded stacks survive an ephemeral pod's death (captured
                    // by `kubectl logs`) — worker pods complete before a file cp can run. Markers delimit it.
                    eprintln!("ZELOX_PPROF_FOLD_BEGIN\n{s}ZELOX_PPROF_FOLD_END");
                }
            });
        }
        Err(e) => log::error!("ZELOX_PPROF guard: {e}"),
    }
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
        Command::Worker => {
            maybe_start_pprof();
            run_worker()
        }

        Command::Server {
            ip,
            port,
            directory,
            mode,
            workers,
            ha,
            auth_token,
            tls_cert,
            tls_key,
            tls_ca,
        } => {
            maybe_start_pprof();
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            if let Some(token) = auth_token {
                // Inject into figment env-var namespace so AppConfig::load() picks it up.
                std::env::set_var("ZELOX_AUTH__TOKEN", token);
            }
            if let Some(cert) = tls_cert {
                std::env::set_var("ZELOX_AUTH__TLS__CERT", cert);
            }
            if let Some(key) = tls_key {
                std::env::set_var("ZELOX_AUTH__TLS__KEY", key);
            }
            if let Some(ca) = tls_ca {
                std::env::set_var("ZELOX_AUTH__TLS__CA", ca);
            }
            match mode.as_deref() {
                Some("local-cluster") | Some("local_cluster") => {
                    run_spark_connect_server_local_cluster(ip.parse()?, port, workers)
                }
                Some("kubernetes-cluster") | Some("kubernetes_cluster") if ha => {
                    run_spark_connect_server_kubernetes_ha(ip.parse()?, port)
                }
                Some(other) => {
                    // For other modes (local, kubernetes-cluster) honour ZELOX_MODE or the
                    // config file; the --mode flag is informational / a convenience alias.
                    eprintln!(
                        "note: --mode {other} — use ZELOX_MODE env var for full config control"
                    );
                    run_spark_connect_server(ip.parse()?, port)
                }
                None if ha => run_spark_connect_server_kubernetes_ha(ip.parse()?, port),
                None => run_spark_connect_server(ip.parse()?, port),
            }
        }

        Command::Sql { query, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_pyspark_script(format_sql_script(&query)?)
        }

        Command::Run { file, directory } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_pyspark_script(file)
        }

        Command::Shell => run_pyspark_shell(),

        Command::Bench {
            scale_factor,
            data_path,
        } => run_bench(scale_factor, &data_path),

        Command::Cluster {
            role,
            ip,
            port,
            scheduler,
            workers,
        } => {
            match role {
                ClusterRole::Scheduler => {
                    // The "scheduler" role runs the Spark Connect server in
                    // local-cluster mode: the driver actor lives in-process and
                    // spawns N LocalWorkerManager worker threads.
                    run_spark_connect_server_local_cluster(ip.parse()?, port, workers)
                }
                ClusterRole::Worker => {
                    // Standalone worker: connects back to the driver gRPC endpoint.
                    // ZELOX_CLUSTER__DRIVER_EXTERNAL_HOST / PORT must be set (or
                    // the defaults from application.yaml apply).
                    let sched = scheduler.unwrap_or_else(|| format!("{}:{}", ip, port));
                    eprintln!("zelox cluster worker → scheduler at {sched}");
                    run_worker()
                }
            }
        }

        Command::Flight(cmd) => match cmd {
            FlightCommand::Server {
                ip,
                port,
                directory,
            } => {
                if let Some(dir) = directory {
                    std::env::set_current_dir(dir)?;
                }
                run_flight_server(ip.parse()?, port)
            }
        },

        Command::McpServer {
            host,
            port,
            transport,
            spark_remote,
            directory,
        } => {
            if let Some(dir) = directory {
                std::env::set_current_dir(dir)?;
            }
            run_spark_mcp_server(
                McpSettings {
                    transport,
                    host,
                    port,
                },
                spark_remote,
            )
        }
    }
}

fn format_sql_script(query: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Write a tiny PySpark script that runs the SQL and prints results.
    // This is passed to run_pyspark_script which expects a file path or "-".
    // We write to a temp file and return the path.
    use std::io::Write;
    let mut tmp = tempfile::NamedTempFile::new()?;
    writeln!(
        tmp,
        r#"from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql({query:?}).show(truncate=False)
"#,
        query = query
    )?;
    let path = tmp.into_temp_path();
    Ok(path.keep()?.to_string_lossy().to_string())
}

fn run_bench(scale_factor: u32, data_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let script = format_bench_script(scale_factor, data_path);
    let mut tmp = tempfile::NamedTempFile::new()?;
    writeln!(tmp, "{script}")?;
    let path = tmp.into_temp_path().keep()?;
    run_pyspark_script(path.to_string_lossy().to_string())
}

fn format_bench_script(scale_factor: u32, data_path: &str) -> String {
    // spark is injected into scope by spark_run.py — no SparkSession.builder needed.
    format!(
        r#"
import sys, time

SF = {sf}
DATA = {data_path:?}

LINE = "=" * 62
print(f"\n{{LINE}}")
print(f"  Zelox TPC-H Benchmark  —  Scale Factor {{SF}}")
print(f"{{LINE}}\n")

# ── 1. Generate / load data ──────────────────────────────────
try:
    import duckdb
except ImportError:
    print("ERROR: duckdb is required for zelox bench.")
    print("       pip install 'duckdb>=1.0'")
    sys.exit(1)

conn = duckdb.connect()
try:
    conn.sql("INSTALL tpch; LOAD tpch;")
except Exception:
    pass  # already installed in some bundles

if DATA == "local":
    import tempfile
    print(f"Generating TPC-H SF={{SF}} with DuckDB...")
    conn.sql(f"CALL dbgen(sf={{SF}})")
    tables = [r[0] for r in conn.sql("SHOW TABLES").fetchall()]
    q_rows  = conn.sql("SELECT query_nr, query FROM tpch_queries()").fetchall()
    queries = dict(q_rows)
    print(f"Generated {{len(tables)}} tables.\n")

    # Write each table to Parquet via DuckDB, then read it back through Zelox.
    # This avoids an Arrow->pandas->createDataFrame round-trip (which breaks on
    # multi-chunk Arrow tables) and exercises the realistic Parquet read path.
    gen_dir = tempfile.mkdtemp(prefix="zelox_tpch_")
    print(f"Writing Parquet to {{gen_dir}} and loading into Zelox...")
    t_load = time.time()
    for tbl in tables:
        pq = f"{{gen_dir}}/{{tbl}}.parquet"
        conn.sql(f"COPY {{tbl}} TO '{{pq}}' (FORMAT PARQUET)")
        spark.read.parquet(pq).createOrReplaceTempView(tbl)
    print(f"Load time: {{time.time()-t_load:.2f}}s\n")
else:
    import os
    TABLES = ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]
    queries = dict(conn.sql("SELECT query_nr, query FROM tpch_queries()").fetchall())
    print(f"Loading Parquet files from {{DATA}}...")
    t_load = time.time()
    for tbl in TABLES:
        p = os.path.join(DATA, f"{{tbl}}.parquet")
        if os.path.isfile(p):
            spark.read.parquet(p).createOrReplaceTempView(tbl)
        else:
            # try directory of part files
            spark.read.parquet(os.path.join(DATA, tbl)).createOrReplaceTempView(tbl)
    print(f"Load time: {{time.time()-t_load:.2f}}s\n")

# ── 2. Run all 22 TPC-H queries ──────────────────────────────
print(f"  {{' Q':>4}}  {{' Time':>8}}  {{' Rows':>10}}  Status")
print(f"  {{'-'*4}}  {{'-'*8}}  {{'-'*10}}  ------")
results = []
for q_num in range(1, 23):
    sql = queries.get(q_num, "")
    if not sql:
        print(f"  Q{{q_num:02d}}  {{'(no query)':>8}}")
        results.append((q_num, None, 0, "SKIP"))
        continue
    last_df = None
    try:
        t0 = time.time()
        for stmt in (s.strip() for s in sql.split(";") if s.strip()):
            stmt = stmt.replace("CREATE VIEW", "CREATE TEMP VIEW")
            last_df = spark.sql(stmt)
        count = last_df.count() if last_df is not None else 0
        elapsed = time.time() - t0
        print(f"  Q{{q_num:02d}}  {{elapsed:>7.3f}}s  {{count:>10}}  OK")
        results.append((q_num, elapsed, count, "OK"))
    except Exception as exc:
        print(f"  Q{{q_num:02d}}  {{' FAILED':>8}}  {{' ':>10}}  {{str(exc)[:55]}}")
        results.append((q_num, None, 0, "FAIL"))

# ── 3. Summary ───────────────────────────────────────────────
ok     = [r for r in results if r[3] == "OK"]
failed = [r[0] for r in results if r[3] == "FAIL"]
total  = sum(r[1] for r in ok)
times  = sorted(r[1] for r in ok)
median = times[len(times)//2] if times else 0

print(f"\n{{LINE}}")
print(f"  Passed : {{len(ok):>2}}/22    Failed : {{len(failed):>2}}/22")
print(f"  Total  : {{total:>8.3f}}s   Median : {{median:>7.3f}}s")
if failed:
    print(f"  Failed queries: {{failed}}")
print(f"{{LINE}}\n")
"#,
        sf = scale_factor,
        data_path = data_path,
    )
}
