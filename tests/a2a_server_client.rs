use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use guigu_agent_bridge::a2a::server::{A2aHandler, HandlerFuture};
use guigu_agent_bridge::a2a::wire::{
    AgentCard, Artifact, Capabilities, JsonRpcRequest, TaskSnapshot, TaskState,
};
use guigu_agent_bridge::a2a::{
    A2aClient, A2aError, A2aServer, A2aServerConfig, ClientPeer, PeerAccess, TrustPolicy,
};
use guigu_agent_bridge::config::SecretString;

struct Handler;
impl A2aHandler for Handler {
    fn handle<'a>(
        &'a self,
        peer: &'a str,
        endpoint: &'a str,
        request: JsonRpcRequest,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            assert_eq!(peer, "peer-a");
            assert_eq!(endpoint, "worker");
            if request.method != "message/send" {
                return Err(A2aError::Unsupported);
            }
            let params: guigu_agent_bridge::a2a::wire::SendParams =
                serde_json::from_value(request.params).unwrap();
            Ok(serde_json::to_value(TaskSnapshot {
                id: "remote-1".into(),
                context_id: params.context_id,
                status: TaskState::Submitted,
                artifacts: Vec::<Artifact>::new(),
                metadata: None,
            })
            .unwrap())
        })
    }
}

#[tokio::test]
async fn real_loopback_send_authenticates_and_returns_acceptance() {
    let mut tokens = BTreeMap::new();
    tokens.insert(
        "peer-a".into(),
        PeerAccess {
            token: SecretString::new("secret".into()),
            allowed_endpoints: ["worker".to_string()].into_iter().collect(),
        },
    );
    let server = A2aServer::bind(
        A2aServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            allow_private_plaintext: false,
            max_body_bytes: 4096,
            card: AgentCard {
                name: "bridge".into(),
                description: "test".into(),
                url: "http://127.0.0.1".into(),
                protocol_version: "0.3.0".into(),
                capabilities: Capabilities {
                    streaming: false,
                    push_notifications: false,
                },
                skills: vec![],
            },
            peers: tokens,
        },
        Arc::new(Handler),
    )
    .await
    .unwrap();
    let address = server.local_addr().unwrap();
    let owner = tokio::spawn(server.serve());
    let client = A2aClient::new(ClientPeer {
        id: "peer-a".into(),
        rpc_url: format!("http://{address}/a2a/worker/rpc").parse().unwrap(),
        token: SecretString::new("secret".into()),
        resolved: vec![address],
        trust: TrustPolicy::WebPki,
        danger_accept_invalid_certs: false,
        timeout: Duration::from_secs(2),
        max_response_bytes: 4096,
    })
    .unwrap();
    let result = client
        .send(guigu_agent_bridge::a2a::wire::SendParams {
            protocol_version: "0.3.0".into(),
            request_id: "req".into(),
            context_id: "ctx".into(),
            message: guigu_agent_bridge::a2a::wire::Message {
                message_id: "msg".into(),
                role: guigu_agent_bridge::a2a::wire::Role::User,
                parts: vec![guigu_agent_bridge::a2a::wire::Part::Text {
                    text: "hello".into(),
                }],
            },
        })
        .await
        .unwrap();
    assert_eq!(result.status, TaskState::Submitted);
    owner.abort();
    let _ = owner.await;
}

#[cfg(unix)]
#[tokio::test]
async fn unix_listener_is_private_and_removes_only_its_socket_on_shutdown() {
    use std::os::unix::fs::PermissionsExt;
    let path = std::env::temp_dir().join(format!("a2a-{}.sock", uuid::Uuid::now_v7()));
    let server = A2aServer::bind_unix(
        &path,
        A2aServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            allow_private_plaintext: false,
            max_body_bytes: 4096,
            card: AgentCard {
                name: "bridge".into(),
                description: "test".into(),
                url: "http+unix://local".into(),
                protocol_version: "0.3.0".into(),
                capabilities: Capabilities {
                    streaming: false,
                    push_notifications: false,
                },
                skills: vec![],
            },
            peers: BTreeMap::new(),
        },
        Arc::new(Handler),
    )
    .unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let handle = server.start();
    handle.shutdown().await.unwrap();
    assert!(!path.exists());
}

struct OpenSslServer {
    child: Child,
    directory: PathBuf,
    address: std::net::SocketAddr,
}

impl OpenSslServer {
    fn start() -> Self {
        let directory = std::env::temp_dir().join(format!("a2a-tls-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let certificate = directory.join("certificate.pem");
        let key = directory.join("key.pem");
        let card_directory = directory.join(".well-known");
        std::fs::create_dir(&card_directory).unwrap();
        let card = card_directory.join("agent-card.json");
        let body = r#"{"name":"tls-peer","description":"test","url":"https://localhost","protocolVersion":"0.3.0","capabilities":{"streaming":false,"pushNotifications":false},"skills":[]}"#;
        std::fs::write(
            &card,
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        )
        .unwrap();
        let generated = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&certificate)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(generated.success(), "could not generate test certificate");

        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let child = Command::new("openssl")
            .args(["s_server", "-quiet", "-HTTP", "-accept"])
            .arg(address.to_string())
            .arg("-cert")
            .arg(&certificate)
            .arg("-key")
            .arg(&key)
            .current_dir(&directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            directory,
            address,
        };
        for _ in 0..10_000 {
            if std::net::TcpStream::connect(address).is_ok() {
                return server;
            }
            std::thread::yield_now();
        }
        panic!("OpenSSL test server did not bind");
    }
}

impl Drop for OpenSslServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        remove_if_exists(&self.directory.join(".well-known/agent-card.json"));
        let _ = std::fs::remove_dir(self.directory.join(".well-known"));
        remove_if_exists(&self.directory.join("certificate.pem"));
        remove_if_exists(&self.directory.join("key.pem"));
        let _ = std::fs::remove_dir(&self.directory);
    }
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
}

fn tls_client(server: &OpenSslServer, danger_accept_invalid_certs: bool) -> A2aClient {
    A2aClient::new(ClientPeer {
        id: "safe-peer-id".into(),
        rpc_url: format!("https://localhost:{}/rpc", server.address.port())
            .parse()
            .unwrap(),
        token: SecretString::new("test-token".into()),
        resolved: vec![server.address],
        trust: TrustPolicy::WebPki,
        danger_accept_invalid_certs,
        timeout: Duration::from_secs(2),
        max_response_bytes: 1_048_576,
    })
    .unwrap()
}

#[tokio::test]
async fn real_https_rejects_self_signed_by_default_and_explicit_bypass_connects() {
    let verified_server = OpenSslServer::start();
    let verified = tls_client(&verified_server, false).agent_card().await;
    assert!(matches!(verified, Err(A2aError::Network(_))));
    drop(verified_server);

    let bypass_server = OpenSslServer::start();
    let bypassed = tls_client(&bypass_server, true).agent_card().await.unwrap();
    assert_eq!(bypassed.name, "tls-peer");
}
