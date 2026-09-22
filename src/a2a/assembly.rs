use super::{
    A2aClient, A2aDispatcher, A2aDispatcherRouter, A2aError, A2aIngress, A2aServer,
    A2aServerConfig, A2aServerHandle, A2aStore, ClientPeer, PeerAccess, TrustPolicy,
};
use crate::{
    app::PersistingBus,
    bus::{Cancellation, EndpointRegistry},
    config::Config,
    storage::{Repository, SqliteRepository},
};
use sqlx::SqlitePool;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

pub(crate) async fn dispatchers(
    config: &Config,
    owner: crate::storage::BusinessStore,
) -> Result<A2aDispatcherRouter, A2aError> {
    let mut dispatchers = HashMap::new();
    let store = A2aStore::new_owner(owner);
    for (peer_key, peer) in &config.transports.a2a.peers {
        if peer.danger_accept_invalid_certs {
            warn_insecure_peer(peer_key);
        }
        let url: url::Url = peer
            .url
            .parse()
            .map_err(|_| A2aError::Config("invalid peer URL"))?;
        let host = url
            .host_str()
            .ok_or(A2aError::Config("peer URL missing host"))?;
        let port = url
            .port_or_known_default()
            .ok_or(A2aError::Config("peer URL missing port"))?;
        let resolved = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| A2aError::Config("peer DNS resolution failed"))?
            .collect();
        let trust = trust_policy(peer)?;
        let client = A2aClient::new(ClientPeer {
            id: peer.expected_peer_id.clone(),
            rpc_url: url,
            token: peer.token.clone(),
            resolved,
            trust,
            danger_accept_invalid_certs: peer.danger_accept_invalid_certs,
            timeout: Duration::from_secs(30),
            max_response_bytes: config.transports.a2a.max_body_bytes,
        })?;
        dispatchers.insert(
            peer_key.clone(),
            Arc::new(A2aDispatcher::new(
                Arc::new(client),
                store.clone(),
                peer.expected_peer_id.clone(),
                Duration::from_millis(250),
                120,
            )),
        );
    }
    Ok(A2aDispatcherRouter::new(dispatchers))
}

fn trust_policy(peer: &crate::config::A2aPeerConfig) -> Result<TrustPolicy, A2aError> {
    match &peer.private_ca {
        Some(path) => Ok(TrustPolicy::PrivateCa(A2aClient::load_private_ca(
            path, 262_144,
        )?)),
        None => Ok(TrustPolicy::WebPki),
    }
}

fn warn_insecure_peer(peer_key: &str) {
    tracing::error!(
        peer = %peer_key,
        "A2A HTTPS certificate verification is disabled; peer identity is unauthenticated"
    );
}

#[cfg(test)]
mod tests {
    use super::{trust_policy, warn_insecure_peer};
    use crate::{
        a2a::A2aError,
        config::{A2aPeerConfig, SecretString},
    };
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(Arc::clone(&self.0))
        }
    }

    impl Write for CapturedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn insecure_peer_warning_contains_only_the_safe_identifier() {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(captured.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || warn_insecure_peer("safe-peer-key"));
        let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(output.contains("ERROR"));
        assert!(output.contains("safe-peer-key"));
        for secret in ["https://", "Bearer", "certificate.pem", "task content"] {
            assert!(
                !output.contains(secret),
                "warning leaked {secret}: {output}"
            );
        }
    }

    #[test]
    fn missing_private_ca_never_falls_back_to_dangerous_bypass() {
        let peer = A2aPeerConfig {
            url: "https://localhost/rpc".into(),
            expected_peer_id: "peer".into(),
            token: SecretString::new("test-token".into()),
            allowed_targets: Vec::new(),
            private_ca: Some(std::path::PathBuf::from(
                "/definitely/missing/a2a-private-ca.pem",
            )),
            danger_accept_invalid_certs: true,
        };
        assert!(matches!(
            trust_policy(&peer),
            Err(A2aError::Config("private CA unavailable"))
        ));
    }
}

pub(crate) async fn server(
    config: &Config,
    pool: SqlitePool,
    repository: Arc<dyn Repository>,
    concrete: SqliteRepository,
    bus: Arc<PersistingBus>,
    cancellation: Cancellation,
    registry: Arc<EndpointRegistry>,
) -> Result<A2aServerHandle, A2aError> {
    let unix_path = config.transports.a2a.listen.strip_prefix("unix:");
    let bind = if unix_path.is_some() {
        "127.0.0.1:0".parse().unwrap()
    } else {
        config
            .transports
            .a2a
            .listen
            .strip_prefix("tcp:")
            .unwrap_or(&config.transports.a2a.listen)
            .parse()
            .map_err(|_| A2aError::Config("listen must be a literal TCP socket or unix:path"))?
    };
    let mut endpoints = BTreeMap::new();
    for name in &config.transports.a2a.exposed_endpoints {
        let endpoint = registry
            .get_by_agent_id(name)
            .ok_or(A2aError::Config("exposed endpoint missing"))?;
        if !endpoint.is_enabled() {
            return Err(A2aError::Config("exposed endpoint disabled"));
        }
        endpoints.insert(name.clone(), endpoint.id());
    }
    let peers = config
        .transports
        .a2a
        .peers
        .values()
        .map(|peer| {
            (
                peer.expected_peer_id.clone(),
                PeerAccess {
                    token: peer.token.clone(),
                    allowed_endpoints: peer.allowed_targets.iter().cloned().collect(),
                },
            )
        })
        .collect();
    let card = super::wire::AgentCard {
        name: "guigu-agent-bridge".into(),
        description: "trusted-LAN A2A bridge".into(),
        url: unix_path.map_or_else(|| format!("http://{bind}"), |_| "http+unix://local".into()),
        protocol_version: super::wire::PROTOCOL_VERSION.into(),
        capabilities: super::wire::Capabilities {
            streaming: false,
            push_notifications: false,
        },
        skills: config
            .transports
            .a2a
            .exposed_endpoints
            .iter()
            .map(|name| super::wire::Skill {
                id: name.clone(),
                name: name.clone(),
                description: "configured bridge endpoint".into(),
            })
            .collect(),
    };
    let ingress = Arc::new(A2aIngress::new(
        A2aStore::new(pool),
        repository,
        concrete,
        bus,
        cancellation,
        endpoints,
    ));
    let server_config = A2aServerConfig {
        bind,
        allow_private_plaintext: config.transports.a2a.allow_private_plaintext,
        max_body_bytes: config.transports.a2a.max_body_bytes,
        card,
        peers,
    };
    #[cfg(unix)]
    if let Some(path) = unix_path {
        return Ok(
            A2aServer::bind_unix(std::path::Path::new(path), server_config, ingress)?.start(),
        );
    }
    Ok(A2aServer::bind(server_config, ingress).await?.start())
}
