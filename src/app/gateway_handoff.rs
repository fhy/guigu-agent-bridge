use std::sync::Arc;

use crate::{bus::BusError, gateway::GatewayStore, models::AgentTask};

use super::PersistingBus;

/// Gateway-private durable handoff used by both live ingress and startup recovery.
#[derive(Clone)]
pub struct GatewayHandoff {
    store: GatewayStore,
    bus: Arc<PersistingBus>,
    runtime: String,
}

impl GatewayHandoff {
    pub fn new(store: GatewayStore, bus: Arc<PersistingBus>, runtime: String) -> Self {
        Self {
            store,
            bus,
            runtime,
        }
    }

    pub async fn handoff(&self, task: AgentTask, revision: i64) -> Result<(), BusError> {
        let task_id = task.task_id.to_string();
        if !self
            .store
            .enqueue_ready(&task_id, &self.runtime, revision)
            .await
            .map_err(|_| BusError::TaskChannelClosed)?
        {
            return Err(BusError::TaskChannelClosed);
        }
        match self.bus.enqueue_persisted(task).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self
                    .store
                    .restore_ready(&task_id, &self.runtime, revision + 1)
                    .await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bus::{Clock, EndpointRegistry, MpscEventSink, derive_endpoint_id},
        config::{
            A2aTransportConfig, AgentEndpointConfig, BridgeConfig, Config, GatewayTransportConfig,
            MatrixRoutesConfig, MatrixTransportConfig, RuntimeConfig, SecretString,
            TransportsConfig,
        },
        models::{AgentTask, ConversationId, Priority, TaskId, TransportType},
        storage::SqliteRepository,
    };
    use std::{collections::BTreeMap, path::PathBuf};

    struct TestDir(PathBuf);
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn config(root: &std::path::Path) -> Config {
        let mut agents = BTreeMap::new();
        agents.insert(
            "recipient".into(),
            AgentEndpointConfig {
                transport: TransportType::Acp,
                command: Some("true".into()),
                args: vec![],
                enabled: true,
                workspace: Some(root.to_path_buf()),
                additional_workspaces: Vec::new(),
                peer: None,
            },
        );
        Config {
            bridge: BridgeConfig {
                database: root.join("db"),
                session_root: root.into(),
                max_task_depth: 8,
                max_task_hops: 16,
                default_timeout_seconds: 300,
                queue_capacity: 1,
                event_capacity: 4,
                shutdown_timeout_seconds: 5,
                health_bind: None,
            },
            runtime: RuntimeConfig {
                allow_nonterminal_end_turn: false,
                max_turns: 8,
                max_wall_seconds: 900,
                max_inactivity_seconds: 120,
                max_no_progress: 2,
                max_output_bytes: 1024,
                lease_ttl_seconds: 30,
            },
            transports: TransportsConfig {
                matrix: MatrixTransportConfig {
                    enabled: false,
                    homeserver: String::new(),
                    user_id: String::new(),
                    device_id: String::new(),
                    access_token: SecretString::new(String::new()),
                    monitor_room: String::new(),
                    sync_capacity: 4,
                    allowed_users: vec![],
                    routes: MatrixRoutesConfig::default(),
                    admin_users: vec![],
                    admin_rooms: vec![],
                    crypto_store_path: None,
                    device_trusted: false,
                },
                a2a: A2aTransportConfig {
                    enabled: false,
                    listen: String::new(),
                    allow_private_plaintext: false,
                    max_body_bytes: 1024,
                    terminal_content_ttl_seconds: 60,
                    retained_bytes_ceiling: 1024,
                    retained_bytes_low_watermark: 512,
                    cleanup_batch: 1,
                    exposed_endpoints: vec![],
                    peers: BTreeMap::new(),
                },
                gateway: GatewayTransportConfig {
                    enabled: false,
                    room_id: String::new(),
                    peer_id: String::new(),
                    local_endpoint_id: String::new(),
                    remote_endpoint_id: String::new(),
                    allowed_senders: vec![],
                    generation: 0,
                    max_payload_bytes: 1024,
                    deadline_seconds: 300,
                },
            },
            agents,
        }
    }

    #[tokio::test]
    async fn queue_full_restores_ready_then_recovery_enqueues_once() {
        let root = std::env::temp_dir().join(format!("guigu-handoff-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let _guard = TestDir(root.clone());
        let pool = crate::storage::connect(root.join("state.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let target = derive_endpoint_id("recipient");
        let from = derive_endpoint_id("sender");
        let conversation = ConversationId::generate();
        for (id, name) in [(from, "sender"), (target, "recipient")] {
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?,?,'acp',1,NULL,'[]')").bind(id.to_string()).bind(name).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,thread_ref,participants_json) VALUES (?,NULL,NULL,NULL,'[]')").bind(conversation.to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES ('runtime','x','x','active','x')").execute(&pool).await.unwrap();
        let task_id = TaskId::generate();
        let task = AgentTask {
            task_id,
            root_task_id: task_id,
            parent_task_id: None,
            from_agent: from,
            to_agent: target,
            conversation_id: conversation,
            reply_to: None,
            text: "work".into(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        };
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,deadline,version) VALUES (?,?,?,?,?,'work',5,0,0,NULL,0)").bind(task_id.to_string()).bind(task_id.to_string()).bind(from.to_string()).bind(target.to_string()).bind(conversation.to_string()).execute(&pool).await.unwrap();
        let queued = serde_json::to_string(&crate::models::TaskEventPayload::Queued).unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES (?, ?, 1, 'queued', '2026-01-01T00:00:00Z', json(?))")
            .bind(crate::models::EventId::generate().to_string())
            .bind(task_id.to_string())
            .bind(queued)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES (?,'ready',0,'runtime','x','x')").bind(task_id.to_string()).execute(&pool).await.unwrap();
        let registry = Arc::new(EndpointRegistry::from_config(&config(&root)));
        let (sink, _events) = MpscEventSink::new(4);
        let (bus, mut receiver) = PersistingBus::new(
            registry,
            1,
            Arc::new(sink),
            SqliteRepository::new(pool.clone()),
            Clock::system(),
        );
        bus.enqueue_persisted(task.clone()).await.unwrap();
        let handoff = GatewayHandoff::new(
            GatewayStore::new(pool.clone()),
            Arc::new(bus),
            "runtime".into(),
        );
        assert!(matches!(
            handoff.handoff(task.clone(), 0).await,
            Err(BusError::QueueFull)
        ));
        let state: (String, i64) =
            sqlx::query_as("SELECT state,revision FROM task_admissions WHERE task_id=?")
                .bind(task_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, ("ready".into(), 2));
        assert_eq!(receiver.recv().await.unwrap().task_id, task_id);
        handoff.handoff(task, 2).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().task_id, task_id);
        assert!(receiver.try_recv().is_err());
        let facts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM tasks WHERE task_id=?), \
                    (SELECT COUNT(*) FROM task_events WHERE task_id=? AND seq=1), \
                    (SELECT COUNT(*) FROM task_admissions WHERE task_id=?)",
        )
        .bind(task_id.to_string())
        .bind(task_id.to_string())
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(facts, (1, 1, 1));
        pool.close().await;
    }
}
