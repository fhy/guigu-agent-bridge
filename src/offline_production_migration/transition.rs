use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use sqlx::Connection;

use super::{Failure, TransitionRequest, arguments::Mode, evidence, records, verification};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub protocol: u8,
    pub plan_digest: String,
    pub root_digest: String,
    pub schema_complete_digest: String,
    pub identity_digest: String,
    pub classification_digest: String,
    pub native_operation_id: String,
    pub classifications: Vec<Classification>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Classification {
    pub table_name: String,
    pub canonical_pk: String,
    pub origin_generation: String,
    pub original_revision: String,
    pub original_digest: String,
    pub parent_relations: serde_json::Value,
    pub disposition: String,
    pub evidence_digest: String,
    pub matrix_operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TransitionIntent {
    protocol: u8,
    operation_id: String,
    plan_digest: String,
    identity_digest: String,
    classification_digest: String,
    native_operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TransitionComplete {
    intent_digest: String,
    plan_digest: String,
    classification_digest: String,
    native_operation_id: String,
}

impl Plan {
    fn load(path: &Path) -> Result<Self, Failure> {
        evidence::canonical(path).map_err(|_| Failure::ManualRequired)?;
        let bytes = fs::read(path).map_err(|_| Failure::ManualRequired)?;
        let plan: Self = serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)?;
        if serde_json::to_vec(&plan).map_err(|_| Failure::Preflight)? != bytes
            || plan.protocol != 1
            || plan.plan_digest.is_empty()
            || plan.root_digest.is_empty()
            || plan.schema_complete_digest.is_empty()
            || plan.identity_digest.is_empty()
            || plan.classification_digest.is_empty()
            || plan.native_operation_id.is_empty()
        {
            return Err(Failure::ManualRequired);
        }
        Ok(plan)
    }
}

pub(super) fn plan_digest(plan: &Plan) -> Result<String, Failure> {
    #[derive(Serialize)]
    struct Payload<'a> {
        protocol: u8,
        root_digest: &'a str,
        schema_complete_digest: &'a str,
        identity_digest: &'a str,
        classification_digest: &'a str,
        native_operation_id: &'a str,
        classifications: &'a [Classification],
    }
    records::encoded_digest(&Payload {
        protocol: plan.protocol,
        root_digest: &plan.root_digest,
        schema_complete_digest: &plan.schema_complete_digest,
        identity_digest: &plan.identity_digest,
        classification_digest: &plan.classification_digest,
        native_operation_id: &plan.native_operation_id,
        classifications: &plan.classifications,
    })
}

pub(super) async fn run(req: TransitionRequest) -> Result<String, Failure> {
    for path in [&req.config, &req.release_provenance, &req.executable] {
        evidence::canonical(path).map_err(|_| Failure::Preflight)?;
    }
    if !req.manifest_dir.is_dir() {
        return Err(Failure::Preflight);
    }
    let plan = Plan::load(&req.transition_plan)?;
    if plan_digest(&plan)? != plan.plan_digest {
        return Err(Failure::ManualRequired);
    }
    if req.mode == Mode::Preflight {
        return Ok("offline-transition-matrix-compatibility result=preflight".into());
    }
    let intent = TransitionIntent {
        protocol: 1,
        operation_id: req.operation_id.clone(),
        plan_digest: plan.plan_digest.clone(),
        identity_digest: plan.identity_digest.clone(),
        classification_digest: plan.classification_digest.clone(),
        native_operation_id: plan.native_operation_id.clone(),
    };
    let intent_path = req.manifest_dir.join("transition-intent-v1.json");
    if intent_path.exists() {
        if records::load::<TransitionIntent>(&intent_path)? != intent {
            return Err(Failure::ManualRequired);
        }
    } else if req.mode == Mode::Apply {
        records::publish(&intent_path, &intent)?;
    }
    super::flow::fault("transition-after-intent");
    let mut connection = verification::open(&req.database, false).await?;
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| Failure::Migration)?;
    let row = sqlx::query("SELECT root_digest,admin_operation_id,schema_complete_digest,identity_digest,phase,plan_digest,classification_digest,native_operation_id FROM offline_recovery_binding WHERE singleton=1")
        .fetch_one(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    use sqlx::Row;
    if row.get::<String, _>(0) != plan.root_digest
        || row.get::<String, _>(1) != req.operation_id
        || row.get::<String, _>(2) != plan.schema_complete_digest
        || row.get::<String, _>(3) != plan.identity_digest
    {
        return Err(Failure::ManualRequired);
    }
    let mut phase: String = row.get(4);
    if phase == "identity_ready" && (req.mode == Mode::Apply || req.mode == Mode::Resume) {
        let intent_digest = records::encoded_digest(&intent)?;
        let classification_digest = records::encoded_digest(&plan.classifications)?;
        if classification_digest != plan.classification_digest {
            return Err(Failure::ManualRequired);
        }
        for classification in &plan.classifications {
            sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,admin_operation_id,plan_digest,matrix_operation_id) VALUES(?,?,?,?,?,?,?,?, 'admin',?,?,?)")
                .bind(&classification.table_name)
                .bind(&classification.canonical_pk)
                .bind(&classification.origin_generation)
                .bind(&classification.original_revision)
                .bind(&classification.original_digest)
                .bind(serde_json::to_string(&classification.parent_relations).map_err(|_| Failure::Preflight)?)
                .bind(&classification.disposition)
                .bind(&classification.evidence_digest)
                .bind(&req.operation_id)
                .bind(&plan.plan_digest)
                .bind(&classification.matrix_operation_id)
                .execute(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
        }
        let changed = sqlx::query("UPDATE offline_recovery_binding SET plan_digest=?,classification_digest=?,transition_intent_digest=?,native_operation_id=?,phase='transition_pending',revision=revision+1 WHERE singleton=1 AND phase='identity_ready' AND root_digest=? AND admin_operation_id=? AND identity_digest=? AND schema_complete_digest=?")
            .bind(&plan.plan_digest)
            .bind(&plan.classification_digest)
            .bind(&intent_digest)
            .bind(&plan.native_operation_id)
            .bind(&plan.root_digest)
            .bind(&req.operation_id)
            .bind(&plan.identity_digest)
            .bind(&plan.schema_complete_digest)
            .execute(&mut *tx).await.map_err(|_| Failure::Migration)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
        phase = "transition_pending".into();
    } else if phase != "transition_pending" && phase != "baseline_ready" && phase != "active"
        || row.get::<Option<String>, _>(5).as_deref() != Some(&plan.plan_digest)
        || row.get::<Option<String>, _>(6).as_deref() != Some(&plan.classification_digest)
        || row.get::<Option<String>, _>(7).as_deref() != Some(&plan.native_operation_id)
    {
        return Err(Failure::ManualRequired);
    }
    tx.commit().await.map_err(|_| Failure::Migration)?;
    connection.close().await.map_err(|_| Failure::Migration)?;
    super::flow::fault("transition-after-sql");
    if phase == "transition_pending" {
        let complete = TransitionComplete {
            intent_digest: records::encoded_digest(&intent)?,
            plan_digest: plan.plan_digest.clone(),
            classification_digest: plan.classification_digest.clone(),
            native_operation_id: plan.native_operation_id.clone(),
        };
        let complete_path = req.manifest_dir.join("transition-complete-v1.json");
        if complete_path.exists() {
            if records::load::<TransitionComplete>(&complete_path)? != complete {
                return Err(Failure::ManualRequired);
            }
        } else {
            records::publish(&complete_path, &complete)?;
        }
        super::flow::fault("transition-after-complete");
        let mut connection = verification::open(&req.database, false).await?;
        let complete_digest = records::encoded_digest(&complete)?;
        let intent_digest = records::encoded_digest(&intent)?;
        let changed = sqlx::query("UPDATE offline_recovery_binding SET transition_complete_digest=?,phase='baseline_ready',revision=revision+1 WHERE singleton=1 AND phase='transition_pending' AND plan_digest=? AND classification_digest=? AND transition_intent_digest=? AND native_operation_id=?")
            .bind(&complete_digest)
            .bind(&plan.plan_digest)
            .bind(&plan.classification_digest)
            .bind(&intent_digest)
            .bind(&plan.native_operation_id)
            .execute(&mut connection).await.map_err(|_| Failure::Migration)?;
        if changed.rows_affected() == 1 {
            phase = "baseline_ready".into();
        } else {
            let existing: Option<String> = sqlx::query_scalar("SELECT transition_complete_digest FROM offline_recovery_binding WHERE singleton=1 AND phase='baseline_ready'")
                .fetch_optional(&mut connection).await.map_err(|_| Failure::ManualRequired)?;
            if existing.as_deref() != Some(&complete_digest) {
                return Err(Failure::ManualRequired);
            }
        }
        connection.close().await.map_err(|_| Failure::Migration)?;
        super::flow::fault("transition-after-cas");
    }
    Ok(format!(
        "offline-transition-matrix-compatibility result=phase-{phase}"
    ))
}
