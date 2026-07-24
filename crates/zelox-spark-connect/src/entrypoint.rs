use std::future::Future;
use std::sync::Arc;

use zelox_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use zelox_common::runtime::RuntimeHandle;
use zelox_server::{ServerBuilder, ServerBuilderOptions, TlsOptions};
pub use zelox_session::session_manager::SessionManagerOptions;
use secrecy::ExposeSecret;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Status};

use crate::server::SparkConnectServer;
use crate::session_manager::create_spark_session_manager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

fn build_tls_options(
    tls: &zelox_common::config::TlsConfig,
) -> Result<Option<TlsOptions>, Box<dyn std::error::Error + Send + Sync>> {
    let (Some(cert_path), Some(key_path)) = (&tls.cert, &tls.key) else {
        return Ok(None);
    };
    let cert_pem = std::fs::read(cert_path)
        .map_err(|e| format!("failed to read TLS cert {cert_path:?}: {e}"))?;
    let key_pem =
        std::fs::read(key_path).map_err(|e| format!("failed to read TLS key {key_path:?}: {e}"))?;
    let ca_pem = tls
        .ca
        .as_deref()
        .map(|p| std::fs::read(p).map_err(|e| format!("failed to read TLS CA {p:?}: {e}")))
        .transpose()?;
    Ok(Some(TlsOptions {
        cert_pem,
        key_pem,
        ca_pem,
    }))
}

/// gRPC interceptor that enforces Bearer token auth when configured.
/// When `expected` is `None` every call is allowed through.
#[derive(Clone)]
struct BearerTokenInterceptor {
    expected: Option<String>,
}

impl tonic::service::Interceptor for BearerTokenInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let Some(ref expected) = self.expected else {
            return Ok(req);
        };
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided = auth.strip_prefix("Bearer ").unwrap_or("");
        // Constant-time comparison so the token cannot be recovered byte-by-byte
        // via response-timing analysis. `ct_eq` on byte slices returns false for
        // unequal lengths without short-circuiting on content.
        let matches: bool = provided.as_bytes().ct_eq(expected.as_bytes()).into();
        if matches {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid or missing Bearer token"))
        }
    }
}

/// The meat of the gRPC server.
pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()>,
{
    let expected_token = config
        .auth
        .token
        .as_ref()
        .map(|s| s.expose_secret().to_string());
    let tls = build_tls_options(&config.auth.tls)?;

    // F4: a Bearer token without TLS travels in cleartext and can be sniffed or
    // replayed. Refuse to start in that configuration unless explicitly allowed.
    if expected_token.is_some() && tls.is_none() && !config.auth.allow_insecure_token {
        return Err("refusing to start: an auth token is set without TLS, so the \
            token would be sent in cleartext. Enable TLS (ZELOX_AUTH__TLS__CERT and \
            ZELOX_AUTH__TLS__KEY), or set ZELOX_AUTH__ALLOW_INSECURE_TOKEN=true to \
            override on a trusted network."
            .into());
    }

    let server_opts = ServerBuilderOptions {
        tls,
        // F2: don't expose anonymous gRPC reflection when auth is enabled.
        reflection: expected_token.is_none(),
        ..Default::default()
    };

    let interceptor = BearerTokenInterceptor {
        expected: expected_token,
    };

    // F3: the Web UI is unauthenticated; it binds to a loopback host by default
    // (see UiConfig) and can be disabled entirely.
    if config.ui.enabled {
        let ui_host = config.ui.host.clone();
        let ui_port = config.ui.port;
        tokio::spawn(async move {
            if let Err(e) = crate::web_ui::serve(&ui_host, ui_port).await {
                log::warn!("Web UI error: {e}");
            }
        });
    }

    let session_manager = create_spark_session_manager(config, runtime)?;
    let server = SparkConnectServer::new(session_manager);
    // Configure message size and compression on the inner service first, then
    // wrap with the auth interceptor — InterceptedService doesn't proxy those methods.
    let configured = SparkConnectServiceServer::new(server)
        // The original Spark Connect server seems to have configuration for inbound (decoding) message size only.
        // .max_encoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    let service = tonic::service::interceptor::InterceptedService::new(configured, interceptor);

    let result = ServerBuilder::new("zelox_spark_connect", server_opts)
        .map_err(|e| e.to_string())?
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await;
    result.map_err(|e| e.to_string().into())
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use tonic::service::Interceptor;
    use tonic::Request;

    use super::BearerTokenInterceptor;

    fn with_auth(header: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(h) = header {
            req.metadata_mut().insert("authorization", h.parse().unwrap());
        }
        req
    }

    #[test]
    fn no_token_configured_allows_everything() {
        let mut i = BearerTokenInterceptor { expected: None };
        assert!(i.call(with_auth(None)).is_ok());
        assert!(i.call(with_auth(Some("Bearer whatever"))).is_ok());
    }

    #[test]
    fn correct_token_is_accepted() {
        let mut i = BearerTokenInterceptor {
            expected: Some("s3cr3t".to_string()),
        };
        assert!(i.call(with_auth(Some("Bearer s3cr3t"))).is_ok());
    }

    #[test]
    fn wrong_or_missing_token_is_rejected() {
        let mut i = BearerTokenInterceptor {
            expected: Some("s3cr3t".to_string()),
        };
        assert!(i.call(with_auth(Some("Bearer wrong"))).is_err());
        assert!(i.call(with_auth(Some("s3cr3t"))).is_err()); // missing "Bearer " prefix
        assert!(i.call(with_auth(None)).is_err());
        // Different-length token must not panic and must be rejected (ct_eq path).
        assert!(i.call(with_auth(Some("Bearer short"))).is_err());
    }
}
