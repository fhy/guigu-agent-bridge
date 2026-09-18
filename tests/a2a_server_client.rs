use std::collections::BTreeMap;
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
