use std::future::Future;
use std::sync::Arc;

use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeHandle;
use sail_server::ServerBuilder;
pub use sail_session::session_manager::SessionManagerOptions;
use secrecy::ExposeSecret;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Status};

use crate::server::SparkConnectServer;
use crate::session_manager::create_spark_session_manager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

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
        if provided == expected {
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
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let expected_token = config
        .auth
        .token
        .as_ref()
        .map(|s| s.expose_secret().to_string());

    let interceptor = BearerTokenInterceptor { expected: expected_token };

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
    ServerBuilder::new("sail_spark_connect", Default::default())
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
}
