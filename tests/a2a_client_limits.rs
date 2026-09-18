use std::time::Duration;

use guigu_agent_bridge::a2a::{A2aClient, A2aError, ClientPeer, TrustPolicy};
use guigu_agent_bridge::config::SecretString;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server(response: &'static [u8]) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..count]);
        }
        stream.write_all(response).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    address
}

fn client(address: std::net::SocketAddr) -> A2aClient {
    A2aClient::new(ClientPeer {
        id: "bounded-peer".into(),
        rpc_url: format!("http://{address}/a2a/worker/rpc").parse().unwrap(),
        token: SecretString::new("test-token".into()),
        resolved: vec![address],
        trust: TrustPolicy::WebPki,
        danger_accept_invalid_certs: false,
        timeout: Duration::from_secs(2),
        max_response_bytes: 8,
    })
    .unwrap()
}

const CONTENT_LENGTH: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 9999\r\nConnection: close\r\n\r\n";
const CHUNKED: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\n0123456789abcdef\r\n0\r\n\r\n";
const UNTIL_EOF: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n0123456789abcdef";

#[tokio::test]
async fn agent_card_rejects_declared_chunked_and_eof_oversize_bodies() {
    for response in [CONTENT_LENGTH, CHUNKED, UNTIL_EOF] {
        let address = server(response).await;
        assert!(matches!(
            client(address).agent_card().await,
            Err(A2aError::TooLarge)
        ));
    }
}

#[tokio::test]
async fn rpc_rejects_declared_chunked_and_eof_oversize_bodies() {
    for response in [CONTENT_LENGTH, CHUNKED, UNTIL_EOF] {
        let address = server(response).await;
        assert!(matches!(
            client(address).get("remote-task").await,
            Err(A2aError::TooLarge)
        ));
    }
}
