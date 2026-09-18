use chrono::SecondsFormat;
use uuid::Uuid;

use super::{OutboxDrain, PersistingBus};
use crate::{
    bus::{BusFuture, Clock},
    matrix::{InboundMatrixEvent, RetryAdmission, RetryReply},
    models::{AgentTask, EventId, TaskId},
    storage::{ReceiptOutcome, ReliabilityStore, RetryTaskInput},
};

const RETRY_TXN_NAMESPACE: Uuid = Uuid::from_u128(0xf47cb0fc_c0c4_55ac_b278_198b5de252eb);

pub struct DurableRetryAdmission {
    store: ReliabilityStore,
    clock: Clock,
    default_timeout: chrono::Duration,
    bus: PersistingBus,
    outbox: OutboxDrain,
    runtime_instance: String,
}

impl DurableRetryAdmission {
    pub fn new(
        store: ReliabilityStore,
        clock: Clock,
        default_timeout: chrono::Duration,
        bus: PersistingBus,
        outbox: OutboxDrain,
        runtime_instance: String,
    ) -> Self {
        Self {
            store,
            clock,
            default_timeout,
            bus,
            outbox,
            runtime_instance,
        }
    }
}

impl RetryAdmission for DurableRetryAdmission {
    fn admit<'a>(
        &'a self,
        event: &'a InboundMatrixEvent,
        source: &'a AgentTask,
    ) -> BusFuture<'a, Result<RetryReply, ()>> {
        Box::pin(async move {
            let now = self.clock.now();
            let deadline = now.checked_add_signed(self.default_timeout).ok_or(())?;
            let task_id = TaskId::generate();
            let event_id = EventId::generate();
            let body = format!("retry=admitted task_id={task_id}");
            let txn = Uuid::new_v5(
                &RETRY_TXN_NAMESPACE,
                format!("matrix\0{}\0{}", event.room_id, event.event_id).as_bytes(),
            )
            .to_string();
            let timestamp = now.to_rfc3339_opts(SecondsFormat::Nanos, true);
            let deadline = deadline.to_rfc3339_opts(SecondsFormat::Nanos, true);
            let input = RetryTaskInput {
                transport: "matrix",
                external_event_id: &event.event_id,
                room_id: &event.room_id,
                thread_root: event.thread_root.as_deref(),
                reply_event_id: &event.event_id,
                admin_actor: &event.sender,
                source_task_id: &source.task_id.to_string(),
                task_id: &task_id.to_string(),
                from_agent: &source.from_agent.to_string(),
                to_agent: &source.to_agent.to_string(),
                conversation_id: &source.conversation_id.to_string(),
                text: &source.text,
                priority: i64::from(source.priority.value()),
                deadline: Some(&deadline),
                timestamp: &timestamp,
                event_id: &event_id.to_string(),
                body: &body,
                stable_txn_id: &txn,
            };
            match self.store.admit_retry(input).await.map_err(|_| ())? {
                ReceiptOutcome::Inserted => {
                    let task = AgentTask {
                        task_id,
                        root_task_id: task_id,
                        parent_task_id: None,
                        from_agent: source.from_agent,
                        to_agent: source.to_agent,
                        conversation_id: source.conversation_id,
                        reply_to: None,
                        text: source.text.clone(),
                        priority: source.priority,
                        depth: 0,
                        hops: 0,
                        deadline: Some(deadline.parse().map_err(|_| ())?),
                        version: 0,
                    };
                    if self
                        .store
                        .claim_admission(
                            &task_id.to_string(),
                            0,
                            Some(&self.runtime_instance),
                            &timestamp,
                        )
                        .await
                        .map_err(|_| ())?
                        && self.bus.enqueue_persisted(task).await.is_err()
                    {
                        let _ = self
                            .store
                            .release_admission(&task_id.to_string(), 1, &timestamp)
                            .await;
                    }
                    let _ = self.outbox.drain_once().await;
                    Ok(RetryReply::OutboxOwned)
                }
                ReceiptOutcome::Replay {
                    task_id: Some(task_id),
                    result_code,
                } if result_code == "admitted" => {
                    let _ = task_id;
                    let _ = self.outbox.drain_once().await;
                    Ok(RetryReply::OutboxOwned)
                }
                ReceiptOutcome::Replay { result_code, .. } => {
                    Ok(RetryReply::Immediate(format!("retry={result_code}")))
                }
            }
        })
    }
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DurableRetryAdmission>();
};
