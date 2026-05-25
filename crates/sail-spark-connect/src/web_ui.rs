use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Clone, Serialize)]
pub struct QueryEntry {
    pub id: String,
    pub name: String,
    pub is_active: bool,
    pub batches_committed: u64,
}

type Registry = std::sync::Arc<RwLock<Vec<QueryEntry>>>;

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static START: OnceLock<Instant> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| std::sync::Arc::new(RwLock::new(Vec::new())))
}

pub async fn register_query(id: String, name: String) {
    registry().write().await.push(QueryEntry { id, name, is_active: true, batches_committed: 0 });
}

pub async fn increment_batch(id: &str) {
    if let Some(q) = registry().write().await.iter_mut().find(|q| q.id == id) {
        q.batches_committed += 1;
    }
}

pub async fn mark_stopped(id: &str) {
    if let Some(q) = registry().write().await.iter_mut().find(|q| q.id == id) {
        q.is_active = false;
    }
}

async fn handle_index(State(reg): State<Registry>) -> Html<String> {
    let uptime = START.get().map(|t| t.elapsed().as_secs()).unwrap_or(0);
    let queries = reg.read().await;
    let rows: String = if queries.is_empty() {
        "<tr><td colspan=\"4\" style=\"text-align:center;color:#888\">No streaming queries</td></tr>".to_string()
    } else {
        queries.iter().map(|q| format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            q.id, q.name,
            if q.is_active { "<span style='color:#2a2'>Active</span>" } else { "<span style='color:#888'>Stopped</span>" },
            q.batches_committed
        )).collect()
    };
    let active = queries.iter().filter(|q| q.is_active).count();
    let total = queries.len();
    drop(queries);

    Html(format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta http-equiv="refresh" content="5"/>
<title>Vajra Web UI</title>
<style>
  body {{ font-family: sans-serif; margin: 2rem; background: #f8f8f8; color: #222; }}
  h1 {{ color: #1a1a6e; }}
  .badge {{ display:inline-block; padding:2px 10px; border-radius:4px; font-size:0.85em; }}
  .up {{ background:#d4edda; color:#155724; }}
  table {{ border-collapse:collapse; width:100%; background:#fff; box-shadow:0 1px 4px #0001; }}
  th {{ background:#1a1a6e; color:#fff; padding:8px 12px; text-align:left; }}
  td {{ padding:8px 12px; border-bottom:1px solid #eee; }}
  tr:last-child td {{ border-bottom:none; }}
  .stat {{ display:inline-block; margin:0 1rem 1rem 0; padding:0.7rem 1.5rem; background:#fff;
           border-radius:6px; box-shadow:0 1px 3px #0001; }}
  .stat .n {{ font-size:2em; font-weight:bold; color:#1a1a6e; }}
  .stat .l {{ font-size:0.8em; color:#666; }}
  footer {{ margin-top:2rem; font-size:0.8em; color:#999; }}
</style>
</head>
<body>
<h1>⚡ Vajra Spark Engine <span class="badge up">Running</span></h1>
<div>
  <div class="stat"><div class="n">{active}</div><div class="l">Active Queries</div></div>
  <div class="stat"><div class="n">{total}</div><div class="l">Total Queries</div></div>
  <div class="stat"><div class="n">{uptime}s</div><div class="l">Uptime</div></div>
</div>
<h2>Streaming Queries</h2>
<table>
<thead><tr><th>ID</th><th>Name</th><th>Status</th><th>Batches Committed</th></tr></thead>
<tbody>{rows}</tbody>
</table>
<footer>Auto-refreshes every 5 seconds &bull; <a href="/api/streaming">JSON API</a></footer>
</body>
</html>"#))
}

async fn handle_api(State(reg): State<Registry>) -> Json<Vec<QueryEntry>> {
    Json(reg.read().await.clone())
}

pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    START.get_or_init(Instant::now);
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await
        .map_err(|e| format!("Web UI bind failed on {addr}: {e}"))?;
    log::info!("Vajra Web UI at http://{addr}");
    let app = Router::new()
        .route("/", get(handle_index))
        .route("/api/streaming", get(handle_api))
        .with_state(registry().clone());
    axum::serve(listener, app).await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })
}
