use std::collections::BTreeSet;

use sqlx::SqliteConnection;

use super::{D1ChainTuple, Failure, Plan, records};
use crate::{
    offline_production_migration::reconciliation_records::RowImage,
    storage::recovery_eligibility::inventory::{self, RowIdentity},
};

struct FrozenRow {
    identity: RowIdentity,
    before: RowImage,
    after: RowImage,
    step: usize,
}

type CellChanges = Vec<(&'static str, String)>;

pub(super) struct Guard {
    rows: Vec<FrozenRow>,
    audits: Vec<(String, String)>,
    pub(super) admission_state: &'static str,
}

fn text(value: &str) -> String {
    format!(
        "text:{}",
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    )
}

fn key(value: &str) -> Result<String, Failure> {
    serde_json::to_string(&[text(value)]).map_err(|_| Failure::ManualRequired)
}

fn cell<'image>(image: &'image RowImage, name: &str) -> Result<&'image str, Failure> {
    let index = image
        .names
        .iter()
        .position(|column| column == name)
        .ok_or(Failure::ManualRequired)?;
    image
        .values
        .get(index)
        .map(String::as_str)
        .ok_or(Failure::ManualRequired)
}

fn changed(before: &RowImage, changes: &[(&str, String)]) -> Result<RowImage, Failure> {
    let mut after = before.clone();
    for (name, value) in changes {
        let index = after
            .names
            .iter()
            .position(|column| column == name)
            .ok_or(Failure::ManualRequired)?;
        *after.values.get_mut(index).ok_or(Failure::ManualRequired)? = value.clone();
    }
    Ok(after)
}

fn delta(
    tuple: &D1ChainTuple,
    plan: &Plan,
    identity: &RowIdentity,
) -> Result<(usize, CellChanges), Failure> {
    let changes = match identity.table.as_str() {
        "execution_leases" if identity.key == key(&tuple.resource)? => (
            1,
            vec![
                ("state", text("released")),
                ("heartbeat_at", text(&plan.abandonment_at)),
            ],
        ),
        "task_continuations" if identity.key == key(&tuple.task)? => {
            let revision = tuple
                .continuation_revision
                .checked_add(1)
                .ok_or(Failure::ManualRequired)?;
            (
                2,
                vec![
                    ("state", text("terminal")),
                    (
                        "revision",
                        format!(
                            "integer:{}",
                            revision
                                .to_string()
                                .as_bytes()
                                .iter()
                                .map(|byte| format!("{byte:02X}"))
                                .collect::<String>()
                        ),
                    ),
                    ("heartbeat_at", text(&plan.abandonment_at)),
                ],
            )
        }
        "delivery_dispositions" if identity.key == key(&tuple.delivery)? => (
            3,
            vec![
                ("state", text("terminal")),
                ("reason_code", text("historical-unknown-abandoned")),
            ],
        ),
        _ => (usize::MAX, vec![]),
    };
    Ok(changes)
}

impl Guard {
    pub(super) async fn capture(
        connection: &mut SqliteConnection,
        plan: &Plan,
        tuple: &D1ChainTuple,
    ) -> Result<Self, Failure> {
        if records::encoded_digest(&plan.actions)? != plan.action_digest {
            return Err(Failure::ManualRequired);
        }
        let mut rows = Vec::new();
        for action in &plan.actions {
            if action.action_type == "insert" {
                continue;
            }
            let identity = RowIdentity {
                table: action.table_name.clone(),
                key: action.canonical_pk.clone(),
            };
            let before = action.before.clone().ok_or(Failure::ManualRequired)?;
            if records::encoded_digest(&(&before.names, &before.values))? != action.original_digest
            {
                return Err(Failure::ManualRequired);
            }
            let (step, changes) = delta(tuple, plan, &identity)?;
            let after = changed(&before, &changes)?;
            if step != usize::MAX
                && (action.action_type != "update" || action.after.as_ref() != Some(&after))
            {
                return Err(Failure::ManualRequired);
            }
            if matches!(
                identity.table.as_str(),
                "tasks"
                    | "task_admissions"
                    | "runtime_instances"
                    | "runtime_owner_evidence"
                    | "sessions"
                    | "deliveries"
            ) && (action.action_type != "preserve" || action.after.as_ref() != Some(&before))
            {
                return Err(Failure::ManualRequired);
            }
            rows.push(FrozenRow {
                identity,
                before,
                after,
                step,
            });
        }
        let mut audits = audit_rows(connection).await?;
        if audits.iter().any(|row| row.0 == plan.native_operation_id) {
            return Err(Failure::ManualRequired);
        }
        audits.push((
            plan.native_operation_id.clone(),
            super::expected_chain_audit(plan)?.ok_or(Failure::ManualRequired)?,
        ));
        audits.sort();
        let mut guard = Self {
            rows,
            audits,
            admission_state: "terminal",
        };
        guard.require_chain(tuple)?;
        guard.verify(connection, 0).await?;
        Ok(guard)
    }

    fn row(&self, table: &str, key: &str) -> Result<&RowImage, Failure> {
        let mut rows = self
            .rows
            .iter()
            .filter(|row| row.identity.table == table && row.identity.key == key);
        let row = rows.next().ok_or(Failure::ManualRequired)?;
        if rows.next().is_some() {
            return Err(Failure::ManualRequired);
        }
        Ok(&row.before)
    }

    fn require_chain(&mut self, tuple: &D1ChainTuple) -> Result<(), Failure> {
        let task_key = key(&tuple.task)?;
        self.row("tasks", &task_key)?;
        self.row("deliveries", &key(&tuple.delivery)?)?;
        self.row("execution_leases", &key(&tuple.resource)?)?;
        self.row("task_continuations", &task_key)?;
        self.row("delivery_dispositions", &key(&tuple.delivery)?)?;
        let admission = self.row("task_admissions", &task_key)?;
        let state = match cell(admission, "state")? {
            value if value == text("terminal") => "terminal",
            value if value == text("recovery_needed") => "recovery_needed",
            _ => return Err(Failure::ManualRequired),
        };
        let runtime_key = serde_json::to_string(&[cell(admission, "runtime_instance")?])
            .map_err(|_| Failure::ManualRequired)?;
        self.row("runtime_instances", &runtime_key)?;
        self.admission_state = state;
        Ok(())
    }

    pub(super) async fn verify(
        &self,
        connection: &mut SqliteConnection,
        step: usize,
    ) -> Result<(), Failure> {
        let actual = inventory::identities(connection)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        let keys = actual
            .iter()
            .map(|identity| (&identity.table, &identity.key))
            .collect::<BTreeSet<_>>();
        let expected = self
            .rows
            .iter()
            .map(|row| (&row.identity.table, &row.identity.key))
            .collect::<BTreeSet<_>>();
        if keys != expected || expected.len() != self.rows.len() {
            return Err(Failure::ManualRequired);
        }
        for row in &self.rows {
            let (names, values) = inventory::values(connection, &row.identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            let expected = if step >= row.step {
                &row.after
            } else {
                &row.before
            };
            if (RowImage { names, values }) != *expected {
                return Err(Failure::ManualRequired);
            }
        }
        if step > 0 {
            self.verify_audit(connection).await?;
        }
        Ok(())
    }

    pub(super) async fn verify_audit(
        &self,
        connection: &mut SqliteConnection,
    ) -> Result<(), Failure> {
        if audit_rows(connection).await? != self.audits {
            return Err(Failure::ManualRequired);
        }
        Ok(())
    }
}

async fn audit_rows(connection: &mut SqliteConnection) -> Result<Vec<(String, String)>, Failure> {
    sqlx::query_as("SELECT decision_id,json_array(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind,termination_proven) FROM historical_unknown_abandonments ORDER BY decision_id")
        .fetch_all(connection).await.map_err(|_| Failure::ManualRequired)
}
