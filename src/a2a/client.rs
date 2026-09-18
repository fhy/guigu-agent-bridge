use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use reqwest::{Certificate, Client, Url};
use serde_json::{Value, json};

use crate::config::SecretString;

use super::wire::{
    AgentCard, JsonRpcRequest, JsonRpcResponse, SendParams, TaskQuery, TaskSnapshot,
};
use super::{A2aError, validate_peer_url};

#[derive(Debug, Clone)]
pub enum TrustPolicy {
    WebPki,
    PrivateCa(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ClientPeer {
    pub id: String,
    pub rpc_url: Url,
    pub token: SecretString,
    pub resolved: Vec<SocketAddr>,
    pub trust: TrustPolicy,
    pub danger_accept_invalid_certs: bool,
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

#[derive(Clone)]
pub struct A2aClient {
    peer: ClientPeer,
    http: Client,
}

impl A2aClient {
    pub fn new(peer: ClientPeer) -> Result<Self, A2aError> {
        validate_peer_url(&peer.rpc_url, &peer.resolved)?;
        let host = peer
            .rpc_url
            .host_str()
            .ok_or(A2aError::Config("peer URL missing host"))?;
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(peer.timeout)
            .danger_accept_invalid_certs(peer.danger_accept_invalid_certs);
        builder = builder.resolve_to_addrs(host, &peer.resolved);
        if let TrustPolicy::PrivateCa(pem) = &peer.trust {
            let certificate = Certificate::from_pem(pem)
                .map_err(|_| A2aError::Config("invalid private CA PEM"))?;
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(certificate);
        }
        let http = builder.build()?;
        Ok(Self { peer, http })
    }

    pub fn load_private_ca(path: &Path, max_bytes: u64) -> Result<Vec<u8>, A2aError> {
        let metadata =
            std::fs::metadata(path).map_err(|_| A2aError::Config("private CA unavailable"))?;
        if !metadata.is_file() || metadata.len() > max_bytes {
            return Err(A2aError::Config(
                "private CA must be a bounded regular file",
            ));
        }
        std::fs::read(path).map_err(|_| A2aError::Config("private CA unavailable"))
    }

    pub async fn agent_card(&self) -> Result<AgentCard, A2aError> {
        let url = self
            .peer
            .rpc_url
            .join("/.well-known/agent-card.json")
            .map_err(|_| A2aError::Config("invalid Agent Card URL"))?;
        let response = self.http.get(url).send().await?;
        if !response.status().is_success() {
            return Err(A2aError::Protocol("remote Agent Card HTTP status"));
        }
        let bytes = response.bytes().await?;
        if bytes.len() > self.peer.max_response_bytes {
            return Err(A2aError::TooLarge);
        }
        serde_json::from_slice(&bytes).map_err(|_| A2aError::Protocol("invalid Agent Card"))
    }

    pub async fn send(&self, params: SendParams) -> Result<TaskSnapshot, A2aError> {
        self.rpc(
            "message/send",
            serde_json::to_value(params).map_err(|_| A2aError::Protocol("request encoding"))?,
        )
        .await
    }

    pub async fn get(&self, task_id: &str) -> Result<TaskSnapshot, A2aError> {
        self.rpc(
            "tasks/get",
            serde_json::to_value(TaskQuery {
                protocol_version: super::wire::PROTOCOL_VERSION.into(),
                task_id: task_id.into(),
            })
            .map_err(|_| A2aError::Protocol("request encoding"))?,
        )
        .await
    }

    pub async fn cancel(&self, task_id: &str) -> Result<TaskSnapshot, A2aError> {
        self.rpc(
            "tasks/cancel",
            serde_json::to_value(TaskQuery {
                protocol_version: super::wire::PROTOCOL_VERSION.into(),
                task_id: task_id.into(),
            })
            .map_err(|_| A2aError::Protocol("request encoding"))?,
        )
        .await
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<TaskSnapshot, A2aError> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(uuid::Uuid::now_v7().to_string()),
            method: method.into(),
            params,
        };
        let response = self
            .http
            .post(self.peer.rpc_url.clone())
            .bearer_auth(self.peer.token.expose())
            .json(&request)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                A2aError::Unauthorized
            } else {
                A2aError::Protocol("remote HTTP status")
            });
        }
        let bytes = response.bytes().await?;
        if bytes.len() > self.peer.max_response_bytes {
            return Err(A2aError::TooLarge);
        }
        let envelope: JsonRpcResponse = serde_json::from_slice(&bytes)
            .map_err(|_| A2aError::Protocol("invalid JSON-RPC response"))?;
        if envelope.error.is_some() {
            return Err(A2aError::Protocol("remote JSON-RPC error"));
        }
        serde_json::from_value(
            envelope
                .result
                .ok_or(A2aError::Protocol("missing result"))?,
        )
        .map_err(|_| A2aError::Protocol("invalid task result"))
    }
}
