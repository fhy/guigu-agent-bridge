use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use guigu_agent_bridge::a2a::{A2aClient, A2aDispatcher, A2aStore, ClientPeer, TrustPolicy};
use guigu_agent_bridge::bus::{
    Backoff, Clock, DispatcherRegistry, EndpointRegistry, LoopLimits, MpscEventSink, RetryPolicy,
    Worker, WorkerConfig,
};
use guigu_agent_bridge::config::{SecretString, load_from_str_with_env};
use guigu_agent_bridge::models::{
    AgentTask, ConversationId, EndpointId, Priority, TaskId, TaskStatus, TransportType,
};
use guigu_agent_bridge::storage::{connect, migrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

async fn consume_request(stream: &mut tokio::net::TcpStream) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    let header_end = loop {
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0, "request closed before headers");
        request.extend_from_slice(&buffer[..count]);
        if let Some(index) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&request[..header_end]).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length: ")
                .or_else(|| line.strip_prefix("Content-Length: "))
        })
        .unwrap()
        .parse::<usize>()
        .unwrap();
    while request.len() - header_end < length {
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0, "request closed before body");
        request.extend_from_slice(&buffer[..count]);
    }
}

async fn corrupting_server(
    counter: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let owner = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            consume_request(&mut stream).await;
            counter.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx")
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (address, owner)
}

async fn insert_task(pool: &sqlx::SqlitePool, task: &AgentTask) {
    sqlx::query("INSERT INTO conversations VALUES (?,NULL,NULL,NULL,'[]')")
        .bind(task.conversation_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks VALUES (?,?,NULL,?,?,?,NULL,?,5,0,0,NULL,0)")
        .bind(task.task_id.to_string())
        .bind(task.task_id.to_string())
        .bind(task.from_agent.to_string())
        .bind(task.to_agent.to_string())
        .bind(task.conversation_id.to_string())
        .bind(&task.text)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn consumed_post_with_corrupt_response_is_not_retried_by_worker() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (address, server) = corrupting_server(Arc::clone(&counter)).await;
    let config = load_from_str_with_env(
        &format!(
            r#"
[transports.a2a.peers.lan]
url = "http://{address}/a2a/worker/rpc"
expected_peer_id = "peer"
token = "{{env:A2A_TOKEN}}"

[agents.worker]
transport = "a2a"
peer = "lan"
enabled = true
"#
        ),
        &BTreeMap::from([
            ("HOME".into(), "/tmp".into()),
            ("A2A_TOKEN".into(), "secret".into()),
        ]),
    )
    .unwrap();
    let registry = Arc::new(EndpointRegistry::from_config(&config));
    let target = registry.resolve_agent_id("worker").unwrap();
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: EndpointId::generate(),
        to_agent: target,
        conversation_id: ConversationId::generate(),
        reply_to: None,
        text: "execute once".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let path = std::env::temp_dir().join(format!("a2a-dispatcher-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    insert_task(&pool, &task).await;
    let store = A2aStore::new(pool.clone());
    let client = A2aClient::new(ClientPeer {
        id: "peer".into(),
        rpc_url: format!("http://{address}/a2a/worker/rpc").parse().unwrap(),
        token: SecretString::new("secret".into()),
        resolved: vec![address],
        trust: TrustPolicy::WebPki,
        danger_accept_invalid_certs: false,
        timeout: Duration::from_secs(2),
        max_response_bytes: 4096,
    })
    .unwrap();
    let dispatcher = Arc::new(A2aDispatcher::new(
        Arc::new(client),
        store.clone(),
        "peer".into(),
        Duration::ZERO,
        1,
    ));
    let (task_tx, task_rx) = mpsc::channel(1);
    let (events, mut event_rx) = MpscEventSink::new(16);
    let worker = Worker::builder(
        Arc::clone(&registry),
        task_rx,
        Arc::new(events),
        Clock::system(),
        DispatcherRegistry::new().with(TransportType::A2a, dispatcher),
    )
    .config(WorkerConfig {
        limits: LoopLimits::default(),
        retry: RetryPolicy {
            max_attempts: 3,
            backoff: Backoff::None,
        },
        cancellation: None,
        timer: None,
    })
    .build();
    task_tx.send(task).await.unwrap();
    drop(task_tx);
    worker.run().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    let exchange = store
        .outbound_for_task("peer", &task_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exchange.state, "acceptance_unknown");
    assert!(exchange.external_task_id.is_none());
    assert_eq!(
        event_rx.recv().await.unwrap().status,
        TaskStatus::Dispatched
    );
    assert_eq!(event_rx.recv().await.unwrap().status, TaskStatus::Failed);
    server.abort();
    let _ = server.await;
    drop(store);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}
