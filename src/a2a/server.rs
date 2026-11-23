use std::collections::BTreeMap;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use tokio::net::TcpListener;

use super::server_response::{classify, failure_response, method_class, terminal_event};
use super::wire::{AgentCard, JsonRpcRequest, JsonRpcResponse};
use super::{A2aError, ListenPolicy, validate_listener};

pub type HandlerFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<Value, A2aError>> + Send + 'a>>;

pub trait A2aHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        peer: &'a str,
        endpoint: &'a str,
        request: JsonRpcRequest,
    ) -> HandlerFuture<'a>;
}

#[derive(Clone)]
pub struct A2aServerConfig {
    pub bind: SocketAddr,
    pub allow_private_plaintext: bool,
    pub max_body_bytes: usize,
    pub card: AgentCard,
    pub peers: BTreeMap<String, PeerAccess>,
}

#[derive(Clone)]
pub struct PeerAccess {
    pub token: crate::config::SecretString,
    pub allowed_endpoints: std::collections::BTreeSet<String>,
}

pub struct A2aServer {
    listener: ServerListener,
    router: Router,
    unix_path: Option<(std::path::PathBuf, u64)>,
}

enum ServerListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
}

pub struct A2aServerHandle {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    owner: tokio::task::JoinHandle<std::io::Result<()>>,
    unix_path: Option<(std::path::PathBuf, u64)>,
    local_addr: Option<SocketAddr>,
    listener_inode: Option<u64>,
}

#[derive(Clone)]
struct ServerState {
    card: AgentCard,
    peers: BTreeMap<String, PeerAccess>,
    handler: Arc<dyn A2aHandler>,
    max_body_bytes: usize,
}

impl A2aServer {
    pub async fn bind(
        config: A2aServerConfig,
        handler: Arc<dyn A2aHandler>,
    ) -> Result<Self, A2aError> {
        validate_listener(
            config.bind,
            ListenPolicy {
                allow_private_plaintext: config.allow_private_plaintext,
            },
        )?;
        let listener = TcpListener::bind(config.bind)
            .await
            .map_err(|_| A2aError::Config("listener bind failed"))?;
        let state = ServerState {
            card: config.card,
            peers: config.peers,
            handler,
            max_body_bytes: config.max_body_bytes,
        };
        let router = Router::new()
            .route("/.well-known/agent-card.json", get(card))
            .route("/a2a/{endpoint}/rpc", post(rpc))
            .with_state(state);
        Ok(Self {
            listener: ServerListener::Tcp(listener),
            router,
            unix_path: None,
        })
    }

    #[cfg(unix)]
    pub fn bind_unix(
        path: &std::path::Path,
        config: A2aServerConfig,
        handler: Arc<dyn A2aHandler>,
    ) -> Result<Self, A2aError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
                return Err(A2aError::Config(
                    "Unix listener path is not a removable socket",
                ));
            }
            return Err(A2aError::Config("Unix listener socket already exists"));
        }
        let listener = tokio::net::UnixListener::bind(path)
            .map_err(|_| A2aError::Config("Unix listener bind failed"))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| A2aError::Config("Unix listener permission update failed"))?;
        let inode = std::fs::metadata(path)
            .map_err(|_| A2aError::Config("Unix listener metadata failed"))?
            .ino();
        let state = ServerState {
            card: config.card,
            peers: config.peers,
            handler,
            max_body_bytes: config.max_body_bytes,
        };
        let router = Router::new()
            .route("/.well-known/agent-card.json", get(card))
            .route("/a2a/{endpoint}/rpc", post(rpc))
            .with_state(state);
        Ok(Self {
            listener: ServerListener::Unix(listener),
            router,
            unix_path: Some((path.to_path_buf(), inode)),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match &self.listener {
            ServerListener::Tcp(listener) => listener.local_addr(),
            #[cfg(unix)]
            ServerListener::Unix(_) => {
                Err(std::io::Error::other("Unix listener has no TCP address"))
            }
        }
    }

    pub async fn serve(self) -> std::io::Result<()> {
        match self.listener {
            ServerListener::Tcp(listener) => axum::serve(listener, self.router).await,
            #[cfg(unix)]
            ServerListener::Unix(listener) => axum::serve(listener, self.router).await,
        }
    }

    pub fn start(self) -> A2aServerHandle {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let local_addr = self.local_addr().ok();
        let listener_inode = match &self.listener {
            ServerListener::Tcp(listener) => socket_inode(listener),
            #[cfg(unix)]
            ServerListener::Unix(_) => None,
        };
        let unix_path = self.unix_path;
        let owner = tokio::spawn(async move {
            match self.listener {
                ServerListener::Tcp(listener) => {
                    axum::serve(listener, self.router)
                        .with_graceful_shutdown(async {
                            let _ = stopped.await;
                        })
                        .await
                }
                #[cfg(unix)]
                ServerListener::Unix(listener) => {
                    axum::serve(listener, self.router)
                        .with_graceful_shutdown(async {
                            let _ = stopped.await;
                        })
                        .await
                }
            }
        });
        A2aServerHandle {
            stop: Some(stop),
            owner,
            unix_path,
            local_addr,
            listener_inode,
        }
    }
}

impl A2aServerHandle {
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    pub fn listener_inode(&self) -> Option<u64> {
        self.listener_inode
    }

    pub async fn shutdown(mut self) -> Result<(), A2aError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.owner
            .await
            .map_err(|_| A2aError::RecoveryNeeded)?
            .map_err(|_| A2aError::RecoveryNeeded)?;
        #[cfg(unix)]
        if let Some((path, inode)) = self.unix_path {
            use std::os::unix::fs::MetadataExt;
            if std::fs::symlink_metadata(&path)
                .map(|metadata| metadata.ino())
                .ok()
                == Some(inode)
            {
                std::fs::remove_file(path).map_err(|_| A2aError::RecoveryNeeded)?;
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn socket_inode(listener: &TcpListener) -> Option<u64> {
    let link = std::fs::read_link(format!("/proc/self/fd/{}", listener.as_raw_fd())).ok()?;
    let text = link.to_string_lossy();
    text.strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

#[cfg(not(unix))]
fn socket_inode(_listener: &TcpListener) -> Option<u64> {
    None
}

async fn card(State(state): State<ServerState>) -> Json<AgentCard> {
    Json(state.card)
}

fn authorized_request(
    state: &ServerState,
    endpoint: &str,
    headers: &HeaderMap,
    bytes: &[u8],
    correlation: uuid::Uuid,
) -> Result<(String, JsonRpcRequest), Box<axum::response::Response>> {
    let Some((peer, access)) = state.peers.iter().find(|(_, access)| {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(&format!("Bearer {}", access.token.expose()))
    }) else {
        return Err(Box::new(failure_response(
            correlation,
            Value::Null,
            "other",
            crate::a2a::IngressFailure::new(
                crate::a2a::IngressStage::Authentication,
                crate::a2a::IngressCause::AuthenticationFailed,
                crate::a2a::Retryability::CredentialsChange,
            ),
        )));
    };
    if !access.allowed_endpoints.contains(endpoint) {
        return Err(Box::new(failure_response(
            correlation,
            Value::Null,
            "other",
            crate::a2a::IngressFailure::new(
                crate::a2a::IngressStage::Authorization,
                crate::a2a::IngressCause::TargetUnavailable,
                crate::a2a::Retryability::PolicyChange,
            ),
        )));
    }
    let request = serde_json::from_slice(bytes).map_err(|_| {
        Box::new(failure_response(
            correlation,
            Value::Null,
            "other",
            crate::a2a::IngressFailure::new(
                crate::a2a::IngressStage::RpcEnvelope,
                crate::a2a::IngressCause::MalformedJson,
                crate::a2a::Retryability::Never,
            ),
        ))
    })?;
    Ok((peer.clone(), request))
}

async fn rpc(
    State(state): State<ServerState>,
    Path(endpoint): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    let correlation = uuid::Uuid::now_v7();
    let bytes = match to_bytes(body, state.max_body_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return failure_response(
                correlation,
                Value::Null,
                "other",
                crate::a2a::IngressFailure::new(
                    crate::a2a::IngressStage::HttpBody,
                    crate::a2a::IngressCause::BodyTooLarge,
                    crate::a2a::Retryability::Never,
                ),
            );
        }
    };
    let (peer, request) = match authorized_request(&state, &endpoint, &headers, &bytes, correlation)
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let method_class = method_class(&request.method);
    if request.jsonrpc != "2.0" {
        return failure_response(
            correlation,
            request.id,
            method_class,
            crate::a2a::IngressFailure::new(
                crate::a2a::IngressStage::RpcEnvelope,
                crate::a2a::IngressCause::InvalidParams,
                crate::a2a::Retryability::Never,
            ),
        );
    }
    let id = request.id.clone();
    match state.handler.handle(&peer, &endpoint, request).await {
        Ok(value) => {
            terminal_event(correlation, method_class, None, StatusCode::OK, 0);
            (
                StatusCode::OK,
                Json(JsonRpcResponse {
                    jsonrpc: "2.0".into(),
                    id,
                    result: Some(value),
                    error: None,
                }),
            )
                .into_response()
        }
        Err(error) => failure_response(correlation, id, method_class, classify(error)),
    }
}
