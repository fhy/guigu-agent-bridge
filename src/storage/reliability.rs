//! T018 durable idempotency facts.  This store is intentionally additive: the
//! event log remains the task-status authority while receipts are replay keys.

use sqlx::{Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    bus::AdmissionContext,
    models::{AgentTask, TaskEvent, WorkflowRole},
};

#[derive(Debug, Clone, Copy)]
pub struct WorkflowAdmission<'a> {
    pub transport: &'a str,
    pub external_event_id: &'a str,
    pub sender_endpoint_id: &'a str,
    pub target_endpoint_id: &'a str,
    pub task_id: &'a str,
    pub correlation_id: &'a str,
    pub idempotency_key: &'a str,
    pub kind: &'a str,
    pub body_hash: &'a str,
    pub now: &'a str,
    pub task: &'a AgentTask,
    pub delivery_id: &'a str,
    pub authorization: Option<WorkflowAuthorization<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkflowAuthorization<'a> {
    pub endpoint: &'a str,
    pub role: WorkflowRole,
    pub expected_revision: i64,
}

const OUTBOX_BODY_NAMESPACE: Uuid = Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a);

#[derive(Debug, Error)]
pub enum ReliabilityError {
    #[error("reliability storage failed")]
    Sql(#[from] sqlx::Error),
    #[error("reliability row is malformed")]
    Malformed,
    #[error("retry source is not terminal")]
    NotTerminal,
    #[error("retry input does not match the immutable source task")]
    SourceMismatch,
    #[error("workflow envelope conflicts with immutable winner")]
    WorkflowConflict,
    #[error("workflow authorization does not match durable task state")]
    WorkflowUnauthorized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptOutcome {
    Inserted,
    Replay {
        task_id: Option<String>,
        result_code: String,
    },
}

#[derive(Debug, Clone)]
pub struct RetryTaskInput<'a> {
    pub transport: &'a str,
    pub external_event_id: &'a str,
    pub room_id: &'a str,
    pub thread_root: Option<&'a str>,
    pub reply_event_id: &'a str,
    pub admin_actor: &'a str,
    pub source_task_id: &'a str,
    pub task_id: &'a str,
    pub from_agent: &'a str,
    pub to_agent: &'a str,
    pub conversation_id: &'a str,
    pub text: &'a str,
    pub priority: i64,
    pub deadline: Option<&'a str>,
    pub timestamp: &'a str,
    pub event_id: &'a str,
    pub body: &'a str,
    pub stable_txn_id: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingProjection {
    pub source_kind: String,
    pub source_id: String,
    pub projection: String,
    pub stable_txn_id: String,
    pub room_id: String,
    pub thread_root: Option<String>,
    pub reply_event_id: Option<String>,
    pub body: String,
    pub claim_owner: String,
    pub claim_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRecovery {
    pub eligible: Vec<(String, i64)>,
    pub covered: Vec<String>,
    pub blocked: bool,
}

#[derive(Clone)]
pub struct ReliabilityStore {
    pool: SqlitePool,
}

impl ReliabilityStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn workflow_revision(&self, task_id: &str) -> Result<Option<i64>, ReliabilityError> {
        let row = sqlx::query("SELECT revision FROM task_admissions WHERE task_id=?")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| row.try_get("revision"))
            .transpose()
            .map_err(Into::into)
    }

    /// Atomically reserves one workflow envelope. Identical keys replay; a
    /// conflicting key is rejected by SQLite uniqueness without mutation.
    pub async fn admit_workflow(
        &self,
        input: WorkflowAdmission<'_>,
    ) -> Result<ReceiptOutcome, ReliabilityError> {
        let mut tx = self.pool.begin().await?;
        if let Some(auth) = &input.authorization {
            let task_row = sqlx::query("SELECT from_agent,to_agent FROM tasks WHERE task_id=?")
                .bind(input.task_id)
                .fetch_optional(&mut *tx)
                .await?;
            let Some(task_row) = task_row else {
                return Err(ReliabilityError::WorkflowUnauthorized);
            };
            let from_agent: String = task_row.try_get("from_agent")?;
            let to_agent: String = task_row.try_get("to_agent")?;
            let owner_allowed = match auth.role {
                WorkflowRole::Coordinator => true,
                WorkflowRole::Developer => to_agent == auth.endpoint,
                WorkflowRole::Reviewer => {
                    to_agent == auth.endpoint && input.correlation_id.starts_with("review:")
                }
                WorkflowRole::Observer => false,
            };
            if from_agent.is_empty() || !owner_allowed {
                return Err(ReliabilityError::WorkflowUnauthorized);
            }
            let admission =
                sqlx::query("SELECT state,revision FROM task_admissions WHERE task_id=?")
                    .bind(input.task_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some(admission) = admission else {
                return Err(ReliabilityError::WorkflowUnauthorized);
            };
            let revision: i64 = admission.try_get("revision")?;
            let state: String = admission.try_get("state")?;
            if revision != auth.expected_revision {
                return Err(ReliabilityError::WorkflowConflict);
            }
            if state == "terminal" || state == "recovery_needed" {
                return Err(ReliabilityError::WorkflowUnauthorized);
            }
        }
        let inserted = sqlx::query("INSERT INTO workflow_envelopes(transport,external_event_id,sender_endpoint_id,target_endpoint_id,task_id,correlation_id,idempotency_key,schema,kind,state,outcome,body_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'workflow.v1',?,'reserved',NULL,?,?,?) ON CONFLICT(transport,external_event_id) DO NOTHING")
            .bind(input.transport).bind(input.external_event_id).bind(input.sender_endpoint_id)
            .bind(input.target_endpoint_id).bind(input.task_id).bind(input.correlation_id)
            .bind(input.idempotency_key).bind(input.kind).bind(input.body_hash)
            .bind(input.now).bind(input.now).execute(&mut *tx).await;
        let inserted = match inserted {
            Ok(result) => result,
            Err(error) => {
                let row = sqlx::query("SELECT task_id,outcome,body_hash,sender_endpoint_id,target_endpoint_id,correlation_id,idempotency_key,schema,kind FROM workflow_envelopes WHERE sender_endpoint_id=? AND idempotency_key=?")
                    .bind(input.sender_endpoint_id).bind(input.idempotency_key).fetch_optional(&mut *tx).await?;
                let Some(row) = row else {
                    return Err(ReliabilityError::Sql(error));
                };
                let same = row.try_get::<String, _>("schema")? == "workflow.v1"
                    && row.try_get::<String, _>("kind")? == input.kind
                    && row.try_get::<String, _>("task_id")? == input.task_id
                    && row.try_get::<String, _>("body_hash")? == input.body_hash
                    && row.try_get::<String, _>("target_endpoint_id")? == input.target_endpoint_id
                    && row.try_get::<String, _>("correlation_id")? == input.correlation_id;
                if !same {
                    return Err(ReliabilityError::WorkflowConflict);
                }
                return Ok(ReceiptOutcome::Replay {
                    task_id: Some(row.try_get("task_id")?),
                    result_code: row
                        .try_get::<Option<String>, _>("outcome")?
                        .unwrap_or_else(|| "reserved".into()),
                });
            }
        };
        if inserted.rows_affected() == 0 {
            let row = sqlx::query("SELECT task_id,outcome,body_hash,sender_endpoint_id,target_endpoint_id,correlation_id,idempotency_key,schema,kind FROM workflow_envelopes WHERE transport=? AND external_event_id=?")
                .bind(input.transport).bind(input.external_event_id).fetch_one(&mut *tx).await?;
            let same = row.try_get::<String, _>("schema")? == "workflow.v1"
                && row.try_get::<String, _>("kind")? == input.kind
                && row.try_get::<String, _>("task_id")? == input.task_id
                && row.try_get::<String, _>("body_hash")? == input.body_hash
                && row.try_get::<String, _>("sender_endpoint_id")? == input.sender_endpoint_id
                && row.try_get::<String, _>("target_endpoint_id")? == input.target_endpoint_id
                && row.try_get::<String, _>("correlation_id")? == input.correlation_id
                && row.try_get::<String, _>("idempotency_key")? == input.idempotency_key;
            if !same {
                return Err(ReliabilityError::WorkflowConflict);
            }
            return Ok(ReceiptOutcome::Replay {
                task_id: Some(row.try_get("task_id")?),
                result_code: row
                    .try_get::<Option<String>, _>("outcome")?
                    .unwrap_or_else(|| "reserved".into()),
            });
        }
        if input.authorization.is_none() {
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(input.task.task_id.to_string()).bind(input.task.root_task_id.to_string())
            .bind(input.task.parent_task_id.map(|value| value.to_string()))
            .bind(input.task.from_agent.to_string()).bind(input.task.to_agent.to_string())
            .bind(input.task.conversation_id.to_string()).bind(input.task.reply_to.map(|value| value.to_string()))
            .bind(&input.task.text).bind(i64::from(input.task.priority.value()))
            .bind(i64::from(input.task.depth)).bind(i64::from(input.task.hops))
            .bind(input.task.deadline.map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)))
            .bind(i64::try_from(input.task.version).map_err(|_| ReliabilityError::Malformed)?)
            .execute(&mut *tx).await?;
            sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'queued',?,?)")
            .bind(Uuid::now_v7().to_string()).bind(input.task.task_id.to_string()).bind(input.now).bind("\"queued\"")
            .execute(&mut *tx).await?;
            sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation,render_version,created_at,updated_at) VALUES(?,'enqueued',0,NULL,NULL,?,?,NULL,0,'v1',?,?)")
            .bind(input.task.task_id.to_string()).bind(input.external_event_id).bind(input.external_event_id)
            .bind(input.now).bind(input.now).execute(&mut *tx).await?;
        } else if let Some(auth) = &input.authorization {
            let result = sqlx::query("UPDATE task_admissions SET revision=revision+1,updated_at=? WHERE task_id=? AND revision=?")
                .bind(input.now).bind(input.task_id).bind(auth.expected_revision)
                .execute(&mut *tx).await?;
            if result.rows_affected() != 1 {
                return Err(ReliabilityError::WorkflowConflict);
            }
        }
        let attempt = input
            .authorization
            .map_or(1, |auth| auth.expected_revision + 2);
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,?,?,?,NULL)")
            .bind(input.delivery_id).bind(input.task.task_id.to_string()).bind(attempt).bind(input.target_endpoint_id).bind(input.now)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES(?,?,?,'prepared')")
            .bind(input.delivery_id).bind(input.task.task_id.to_string()).bind(attempt).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ReceiptOutcome::Inserted)
    }

    /// Persist queue admission before making the reserved in-memory slot visible.
    pub async fn admit_task(
        &self,
        task: &AgentTask,
        event: &TaskEvent,
        runtime_instance: &str,
        context: Option<&AdmissionContext>,
    ) -> Result<ReceiptOutcome, ReliabilityError> {
        let mut tx = self.pool.begin().await?;
        if let Some(context) = context {
            let inserted = sqlx::query("INSERT INTO transport_receipts(transport,external_event_id,room_id,thread_ref,reply_event_id,conversation_id,selected_endpoint_id,task_id,receipt_kind,route_kind,state,result_code,created_at,updated_at) VALUES(?,?,?,?,?,?,?,NULL,'ordinary','message','reserved','reserved',?,?) ON CONFLICT(transport,external_event_id) DO NOTHING")
                .bind(&context.transport).bind(&context.external_event_id).bind(&context.room_id)
                .bind(&context.thread_root).bind(&context.reply_event_id)
                .bind(task.conversation_id.to_string()).bind(task.to_agent.to_string())
                .bind(event.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
                .bind(event.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
                .execute(&mut *tx).await?;
            if inserted.rows_affected() == 0 {
                let row = sqlx::query("SELECT task_id,result_code,receipt_kind FROM transport_receipts WHERE transport=? AND external_event_id=?")
                    .bind(&context.transport).bind(&context.external_event_id)
                    .fetch_one(&mut *tx).await?;
                if row.try_get::<String, _>("receipt_kind")? != "ordinary" {
                    return Err(ReliabilityError::Malformed);
                }
                return Ok(ReceiptOutcome::Replay {
                    task_id: row.try_get("task_id")?,
                    result_code: row.try_get("result_code")?,
                });
            }
        }
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(task.task_id.to_string()).bind(task.root_task_id.to_string())
            .bind(task.parent_task_id.map(|value| value.to_string()))
            .bind(task.from_agent.to_string()).bind(task.to_agent.to_string())
            .bind(task.conversation_id.to_string()).bind(task.reply_to.map(|value| value.to_string()))
            .bind(&task.text).bind(i64::from(task.priority.value())).bind(i64::from(task.depth))
            .bind(i64::from(task.hops))
            .bind(task.deadline.map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)))
            .bind(i64::try_from(task.version).map_err(|_| ReliabilityError::Malformed)?)
            .execute(&mut *tx).await?;
        let payload =
            serde_json::to_string(&event.payload).map_err(|_| ReliabilityError::Malformed)?;
        let at = event
            .timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'queued',?,?)")
            .bind(event.id.to_string()).bind(task.task_id.to_string()).bind(&at).bind(payload)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation,render_version,created_at,updated_at) VALUES(?,'enqueued',0,?,?,?,?,?,?,'v1',?,?)")
            .bind(task.task_id.to_string()).bind(runtime_instance)
            .bind(context.map(|value| value.room_id.as_str()))
            .bind(context.and_then(|value| value.thread_root.as_deref()))
            .bind(context.map(|value| value.reply_event_id.as_str()))
            .bind(context.and_then(|value| value.monitor_room.as_deref()))
            .bind(context.map(|value| i64::try_from(value.monitor_generation).unwrap_or(i64::MAX)))
            .bind(&at).bind(&at)
            .execute(&mut *tx).await?;
        if let Some(context) = context {
            let receipt = sqlx::query("UPDATE transport_receipts SET task_id=?,state='admitted',result_code='admitted',updated_at=? WHERE transport=? AND external_event_id=? AND state='reserved' AND task_id IS NULL")
                .bind(task.task_id.to_string()).bind(&at).bind(&context.transport)
                .bind(&context.external_event_id).execute(&mut *tx).await?;
            if receipt.rows_affected() != 1 {
                return Err(ReliabilityError::Malformed);
            }
        }
        tx.commit().await?;
        Ok(ReceiptOutcome::Inserted)
    }

    pub async fn ordinary_receipt_task(
        &self,
        transport: &str,
        external_event_id: &str,
    ) -> Result<Option<String>, ReliabilityError> {
        let row = sqlx::query("SELECT task_id,receipt_kind,state FROM transport_receipts WHERE transport=? AND external_event_id=?")
            .bind(transport).bind(external_event_id).fetch_optional(&self.pool).await?;
        match row {
            None => Ok(None),
            Some(row)
                if row.try_get::<String, _>("receipt_kind")? == "ordinary"
                    && row.try_get::<String, _>("state")? == "admitted" =>
            {
                Ok(row.try_get("task_id")?)
            }
            Some(_) => Err(ReliabilityError::Malformed),
        }
    }

    pub async fn prepare_delivery(
        &self,
        delivery_id: &str,
        task_id: &str,
        attempt: u32,
        target_endpoint: &str,
        now: &str,
    ) -> Result<(), ReliabilityError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,?,?,?,NULL)")
            .bind(delivery_id).bind(task_id).bind(i64::from(attempt)).bind(target_endpoint).bind(now)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES(?,?,?,'prepared')")
            .bind(delivery_id).bind(task_id).bind(i64::from(attempt)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn acknowledge_delivery(
        &self,
        delivery_id: &str,
        session_id: &str,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let mut tx = self.pool.begin().await?;
        let delivery = sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=? AND acknowledged_at IS NULL")
            .bind(now).bind(delivery_id).execute(&mut *tx).await?;
        let disposition = sqlx::query("UPDATE delivery_dispositions SET state='acknowledged',session_id=? WHERE delivery_id=? AND state='prepared'")
            .bind(session_id).bind(delivery_id).execute(&mut *tx).await?;
        if delivery.rows_affected() != disposition.rows_affected() {
            return Err(ReliabilityError::Malformed);
        }
        if delivery.rows_affected() == 0 {
            let acknowledged: Option<i64> = sqlx::query_scalar("SELECT 1 FROM delivery_dispositions WHERE delivery_id=? AND state='acknowledged' AND session_id=?")
                .bind(delivery_id).bind(session_id).fetch_optional(&mut *tx).await?;
            return Ok(acknowledged.is_some());
        }
        tx.commit().await?;
        Ok(true)
    }

    /// Reserve an external event exactly once. A duplicate returns the
    /// immutable winner and never changes routing or admission facts.
    pub async fn reserve_receipt(
        &self,
        transport: &str,
        external_event_id: &str,
        room_id: &str,
        receipt_kind: &str,
        route_kind: &str,
        now: &str,
    ) -> Result<ReceiptOutcome, ReliabilityError> {
        let result = sqlx::query(
            "INSERT INTO transport_receipts
             (transport,external_event_id,room_id,receipt_kind,route_kind,state,result_code,created_at,updated_at)
             VALUES (?,?,?,? ,?,'reserved','reserved',?,?)
             ON CONFLICT(transport,external_event_id) DO NOTHING",
        )
        .bind(transport)
        .bind(external_event_id)
        .bind(room_id)
        .bind(receipt_kind)
        .bind(route_kind)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(ReceiptOutcome::Inserted);
        }
        let row = sqlx::query(
            "SELECT task_id,result_code FROM transport_receipts
             WHERE transport=? AND external_event_id=?",
        )
        .bind(transport)
        .bind(external_event_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(ReceiptOutcome::Replay {
            task_id: row.try_get("task_id")?,
            result_code: row.try_get("result_code")?,
        })
    }

    pub async fn update_receipt(
        &self,
        transport: &str,
        external_event_id: &str,
        state: &str,
        result_code: &str,
        task_id: Option<&str>,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let result = sqlx::query(
            "UPDATE transport_receipts SET state=?,result_code=?,task_id=?,updated_at=?
             WHERE transport=? AND external_event_id=? AND state='reserved'",
        )
        .bind(state)
        .bind(result_code)
        .bind(task_id)
        .bind(now)
        .bind(transport)
        .bind(external_event_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Atomically admits one authorized terminal-task retry and its command
    /// reply projection. Callers must supply fields copied from the immutable
    /// source task; this method never parses or accepts free-form command text.
    pub async fn admit_retry(
        &self,
        input: RetryTaskInput<'_>,
    ) -> Result<ReceiptOutcome, ReliabilityError> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query("SELECT task_id,result_code FROM transport_receipts WHERE transport=? AND external_event_id=?")
            .bind(input.transport).bind(input.external_event_id).fetch_optional(&mut *tx).await?;
        if let Some(row) = existing {
            return Ok(ReceiptOutcome::Replay {
                task_id: row.try_get("task_id")?,
                result_code: row.try_get("result_code")?,
            });
        }
        let source = sqlx::query(
            "SELECT t.from_agent,t.to_agent,t.conversation_id,t.text,t.priority,
                    (SELECT status FROM task_events WHERE task_id=t.task_id ORDER BY seq DESC LIMIT 1) AS status
             FROM tasks t WHERE t.task_id=?",
        )
        .bind(input.source_task_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ReliabilityError::NotTerminal)?;
        let status: Option<String> = source.try_get("status")?;
        if !matches!(
            status.as_deref(),
            Some("completed" | "failed" | "timed_out" | "cancelled")
        ) {
            return Err(ReliabilityError::NotTerminal);
        }
        let matches = source.try_get::<String, _>("from_agent")? == input.from_agent
            && source.try_get::<String, _>("to_agent")? == input.to_agent
            && source.try_get::<String, _>("conversation_id")? == input.conversation_id
            && source.try_get::<String, _>("text")? == input.text
            && source.try_get::<i64, _>("priority")? == input.priority;
        if !matches {
            return Err(ReliabilityError::SourceMismatch);
        }
        sqlx::query("INSERT INTO transport_receipts (transport,external_event_id,room_id,thread_ref,reply_event_id,conversation_id,task_id,source_task_id,retry_task_id,receipt_kind,route_kind,state,result_code,admin_actor,command_reply_body,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(input.transport).bind(input.external_event_id).bind(input.room_id)
            .bind(input.thread_root).bind(input.reply_event_id).bind(input.conversation_id)
            .bind(None::<String>).bind(input.source_task_id).bind(None::<String>)
            .bind("admin").bind("retry").bind("admitted")
            .bind("admitted").bind(input.admin_actor).bind(input.body)
            .bind(input.timestamp).bind(input.timestamp).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO tasks (task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES (?,?,NULL,?,?,?,NULL,?,?,0,0,?,0)")
            .bind(input.task_id).bind(input.task_id).bind(input.from_agent).bind(input.to_agent)
            .bind(input.conversation_id).bind(input.text).bind(input.priority).bind(input.deadline)
            .execute(&mut *tx).await?;
        sqlx::query("UPDATE transport_receipts SET task_id=?,retry_task_id=? WHERE transport=? AND external_event_id=?")
            .bind(input.task_id).bind(input.task_id).bind(input.transport).bind(input.external_event_id)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_events (event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,1,'queued',?,?)")
            .bind(input.event_id).bind(input.task_id).bind(input.timestamp).bind("{\"kind\":\"queued\"}").execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_admissions (task_id,state,revision,created_at,updated_at) VALUES (?,'ready',0,?,?)")
            .bind(input.task_id).bind(input.timestamp).bind(input.timestamp).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO projection_outbox (source_kind,source_id,projection,transport,stable_txn_id,room_id,thread_root,reply_event_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES ('transport_receipt',?,?,?,?,?,?,?,'v1',?,?,'pending',0,?,?)")
            .bind(format!("{}:{}", input.transport, input.external_event_id)).bind("command_reply")
            .bind(input.transport).bind(input.stable_txn_id).bind(input.room_id)
            .bind(input.thread_root).bind(input.reply_event_id)
            .bind(input.body)
            .bind(Uuid::new_v5(&OUTBOX_BODY_NAMESPACE, input.body.as_bytes()).to_string())
            .bind(input.timestamp).bind(input.timestamp).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ReceiptOutcome::Inserted)
    }

    pub async fn claim_projections(
        &self,
        owner: &str,
        now: &str,
        stale_before: &str,
        limit: u32,
    ) -> Result<Vec<PendingProjection>, ReliabilityError> {
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let row=sqlx::query("UPDATE projection_outbox SET state='sending',claim_owner=?,claim_revision=COALESCE(claim_revision,0)+1,updated_at=? WHERE rowid=(SELECT rowid FROM projection_outbox WHERE state='pending' OR (state='sending' AND updated_at<=?) ORDER BY created_at,source_kind,source_id,projection LIMIT 1) RETURNING source_kind,source_id,projection,stable_txn_id,room_id,thread_root,reply_event_id,body,claim_revision")
                .bind(owner).bind(now).bind(stale_before).fetch_optional(&self.pool).await?;
            let Some(row) = row else { break };
            claimed.push(PendingProjection {
                source_kind: row.try_get("source_kind")?,
                source_id: row.try_get("source_id")?,
                projection: row.try_get("projection")?,
                stable_txn_id: row.try_get("stable_txn_id")?,
                room_id: row.try_get("room_id")?,
                thread_root: row.try_get("thread_root")?,
                reply_event_id: row.try_get("reply_event_id")?,
                body: row.try_get("body")?,
                claim_owner: owner.into(),
                claim_revision: row.try_get("claim_revision")?,
            });
        }
        Ok(claimed)
    }

    pub async fn mark_projection_sent(
        &self,
        row: &PendingProjection,
    ) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE projection_outbox SET state='sent',attempt_count=attempt_count+1 WHERE source_kind=? AND source_id=? AND projection=? AND state='sending' AND claim_owner=? AND claim_revision=?")
            .bind(&row.source_kind).bind(&row.source_id).bind(&row.projection).bind(&row.claim_owner).bind(row.claim_revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn claim_admission(
        &self,
        task_id: &str,
        revision: i64,
        runtime_instance: Option<&str>,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE task_admissions SET state='enqueued',revision=revision+1,runtime_instance=?,updated_at=? WHERE task_id=? AND state='ready' AND revision=?")
            .bind(runtime_instance).bind(now).bind(task_id).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn reclaim_stopped_admission(
        &self,
        task_id: &str,
        revision: i64,
        runtime_instance: &str,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE task_admissions SET revision=revision+1,runtime_instance=?,updated_at=? WHERE task_id=? AND state='enqueued' AND revision=? AND runtime_instance IN (SELECT instance_token FROM runtime_instances WHERE state='stopped')")
            .bind(runtime_instance).bind(now).bind(task_id).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn release_admission(
        &self,
        task_id: &str,
        revision: i64,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE task_admissions SET state='ready',revision=revision+1,updated_at=? WHERE task_id=? AND state='enqueued' AND revision=?")
            .bind(now).bind(task_id).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn ready_admissions(
        &self,
        limit: u32,
    ) -> Result<Vec<(String, i64)>, ReliabilityError> {
        let rows = sqlx::query(
            "SELECT task_id,revision FROM task_admissions WHERE state='ready' ORDER BY created_at LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| Ok((row.try_get("task_id")?, row.try_get("revision")?)))
            .collect()
    }

    pub async fn begin_runtime(
        &self,
        token: &str,
        fingerprint: &str,
        now: &str,
    ) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES (?,?,?,'active',?) ON CONFLICT DO NOTHING")
            .bind(token).bind(now).bind(now).bind(fingerprint).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn stop_runtime(&self, token: &str, now: &str) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state IN ('active','stopping')")
            .bind(now).bind(token).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn stopping_runtime(&self, token: &str, now: &str) -> Result<bool, ReliabilityError> {
        let result=sqlx::query("UPDATE runtime_instances SET state='stopping',heartbeat_at=? WHERE instance_token=? AND state='active'")
            .bind(now).bind(token).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn classify_admissions(
        &self,
        _limit: u32,
    ) -> Result<AdmissionRecovery, ReliabilityError> {
        let rows=sqlx::query("SELECT a.task_id,a.revision,a.state,r.state AS owner_state FROM task_admissions a LEFT JOIN runtime_instances r ON r.instance_token=a.runtime_instance WHERE a.state NOT IN ('terminal','recovery_needed') ORDER BY a.created_at")
            .fetch_all(&self.pool).await?;
        let mut result = AdmissionRecovery {
            eligible: Vec::new(),
            covered: Vec::new(),
            blocked: false,
        };
        for row in rows {
            let task: String = row.try_get("task_id")?;
            let revision: i64 = row.try_get("revision")?;
            let state: String = row.try_get("state")?;
            let owner: Option<String> = row.try_get("owner_state")?;
            result.covered.push(task.clone());
            if state == "ready" || (state == "enqueued" && owner.as_deref() == Some("stopped")) {
                result.eligible.push((task, revision));
            } else {
                result.blocked = true;
            }
        }
        Ok(result)
    }
}
