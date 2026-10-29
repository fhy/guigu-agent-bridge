use sqlx::{SqliteConnection, SqlitePool};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, Ordering},
};
use tokio::sync::Mutex;

use super::{
    EffectDomain, EligibilityContext, LiveFence, ParentIdentity, ParentRole, RowIdentity,
    authority, inventory, metadata, refused, require_row,
};

mod effect_scope;
mod mutation;
mod native_control;
mod native_lease;
mod transaction;
mod workspace;
pub(crate) use mutation::{
    Mutation, complete_mutation, complete_refused_receipt, prepare_mutation,
};
pub(crate) use native_control::{complete_control, prepare_control};
pub(crate) use native_lease::{complete_expired_lease, complete_lease, prepare_lease};
pub(crate) use transaction::ScopedTransaction;
pub(crate) use workspace::delete_workspace;

#[derive(Debug, Clone)]
pub struct RecoveryScope {
    context: EligibilityContext,
    domain: EffectDomain,
    fence: Option<LiveFence>,
    anchors: Vec<RowIdentity>,
    revision: Arc<AtomicI64>,
    serial: Arc<Mutex<()>>,
    matrix_fence: Arc<OnceLock<LiveFence>>,
}

impl RecoveryScope {
    pub(crate) fn component_fence(&self) -> Option<&LiveFence> {
        self.fence.as_ref()
    }

    pub(crate) fn for_component(&self, fence: LiveFence) -> Self {
        let mut scope = self.clone();
        scope.domain = EffectDomain::Matrix;
        scope.fence = Some(fence);
        scope
    }
    pub(crate) fn require_matrix_component(&self, component: &str) -> Result<(), sqlx::Error> {
        if self.domain != EffectDomain::Matrix
            || self
                .fence
                .as_ref()
                .is_none_or(|fence| fence.component != component)
        {
            return Err(refused());
        }
        Ok(())
    }

    pub fn new(
        context: EligibilityContext,
        domain: EffectDomain,
        fence: Option<LiveFence>,
    ) -> Self {
        let revision = match &context {
            EligibilityContext::NonAdmin { revision, .. }
            | EligibilityContext::Admin { revision, .. } => *revision,
        };
        Self {
            context,
            domain,
            fence,
            anchors: Vec::new(),
            revision: Arc::new(AtomicI64::new(revision)),
            serial: Arc::new(Mutex::new(())),
            matrix_fence: Arc::new(OnceLock::new()),
        }
    }

    fn context(&self) -> EligibilityContext {
        let mut context = self.context.clone();
        match &mut context {
            EligibilityContext::NonAdmin { revision, .. }
            | EligibilityContext::Admin { revision, .. } => {
                *revision = self.revision.load(Ordering::Acquire);
            }
        }
        context
    }

    pub(crate) fn install_matrix_fence(&self, fence: LiveFence) -> Result<(), sqlx::Error> {
        let installed = self.matrix_fence.get_or_init(|| fence.clone());
        if installed.component != fence.component
            || installed.owner != fence.owner
            || installed.epoch != fence.epoch
        {
            return Err(refused());
        }
        Ok(())
    }

    pub fn with_metadata_anchor(mut self, identity: RowIdentity) -> Self {
        self.anchors.push(identity);
        self
    }
}

pub(crate) async fn begin<'pool>(
    pool: &'pool SqlitePool,
    scope: Option<&RecoveryScope>,
) -> Result<ScopedTransaction<'pool>, sqlx::Error> {
    let guard = match scope {
        Some(scope) => Some(scope.serial.clone().lock_owned().await),
        None => None,
    };
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(scope) = scope {
        authority::validate(
            &mut transaction,
            &scope.context(),
            scope.domain,
            scope.fence.as_ref(),
        )
        .await?;
    } else {
        let audit: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM offline_recovery_binding)+(SELECT count(*) FROM runtime_eligibility_context)")
            .fetch_one(&mut *transaction).await?;
        if audit != 0 {
            return Err(refused());
        }
    }
    Ok(ScopedTransaction::new(
        transaction,
        scope.map(|scope| scope.revision.clone()),
        guard,
    ))
}

pub(crate) fn text_identity(table: &str, key: &str) -> Result<RowIdentity, sqlx::Error> {
    let encoded = key
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    Ok(RowIdentity {
        table: table.into(),
        key: serde_json::to_string(&[format!("text:{encoded}")]).map_err(|_| refused())?,
    })
}

pub(crate) fn composite_text_identity(
    table: &str,
    keys: &[&str],
) -> Result<RowIdentity, sqlx::Error> {
    let encoded = keys
        .iter()
        .map(|key| {
            format!(
                "text:{}",
                key.as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            )
        })
        .collect::<Vec<_>>();
    Ok(RowIdentity {
        table: table.into(),
        key: serde_json::to_string(&encoded).map_err(|_| refused())?,
    })
}

pub(crate) async fn inserted(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    identity: &RowIdentity,
) -> Result<(), sqlx::Error> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let routed = effect_scope::resolve(connection, scope, identity).await?;
    let scope = &routed;
    authority::validate(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
    )
    .await?;
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?",
    )
    .bind(&identity.table)
    .bind(&identity.key)
    .fetch_one(&mut *connection)
    .await?;
    if exists != 0 {
        return require_row(
            connection,
            &scope.context(),
            scope.domain,
            scope.fence.as_ref(),
            identity,
        )
        .await;
    }
    let snapshot = inventory::snapshot(connection, identity).await?;
    let parents = parents(connection, scope, identity, &snapshot).await?;
    let inherited_matrix = parents
        .iter()
        .any(|parent| parent.role == ParentRole::MetadataAnchor);
    let matrix =
        snapshot.matrix || inherited_matrix || matrix_parents(connection, &parents).await?;
    if matrix && scope.domain != EffectDomain::Matrix {
        return Err(refused());
    }
    let encoded = serde_json::to_string(&parents).map_err(|_| refused())?;
    insert_classification(connection, scope, identity, &snapshot, &encoded, matrix).await?;
    require_row(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
        identity,
    )
    .await
}

async fn parents(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    identity: &RowIdentity,
    snapshot: &inventory::Snapshot,
) -> Result<Vec<ParentIdentity>, sqlx::Error> {
    let mut parents = Vec::new();
    for parent in &snapshot.parents {
        let current = inventory::snapshot(connection, parent).await?;
        let role = if scope.anchors.contains(parent) {
            ParentRole::MetadataAnchor
        } else {
            ParentRole::Effectful
        };
        let reference = ParentIdentity {
            identity: parent.clone(),
            revision: current.revision,
            digest: current.digest,
            role,
        };
        if role == ParentRole::MetadataAnchor {
            metadata::validate(connection, &scope.context(), scope.domain, &reference).await?;
        } else if parent != identity {
            require_row(
                connection,
                &scope.context(),
                scope.domain,
                scope.fence.as_ref(),
                parent,
            )
            .await?;
        }
        parents.push(reference);
    }
    Ok(parents)
}

async fn matrix_parents(
    connection: &mut SqliteConnection,
    parents: &[ParentIdentity],
) -> Result<bool, sqlx::Error> {
    for parent in parents {
        let matrix: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND matrix_operation_id IS NOT NULL")
            .bind(&parent.identity.table).bind(&parent.identity.key).fetch_one(&mut *connection).await?;
        if matrix != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn insert_classification(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    identity: &RowIdentity,
    snapshot: &inventory::Snapshot,
    parents: &str,
    matrix: bool,
) -> Result<(), sqlx::Error> {
    let (kind, generation, admin, plan, origin) = match &scope.context {
        EligibilityContext::NonAdmin { generation, .. } => (
            "nonadmin",
            Some(generation.as_str()),
            None,
            None,
            generation.as_str(),
        ),
        EligibilityContext::Admin {
            operation,
            plan,
            matrix_operation,
            ..
        } => (
            "admin",
            None,
            Some(operation.as_str()),
            Some(plan.as_str()),
            matrix_operation.as_str(),
        ),
    };
    let operation = if matrix {
        Some(scope.context.matrix_operation().ok_or_else(refused)?)
    } else {
        None
    };
    sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id) VALUES(?,?,?,?,?,?,'eligible_new',?,?,?,?,?,?)")
        .bind(&identity.table).bind(&identity.key).bind(origin).bind(&snapshot.revision).bind(&snapshot.digest)
        .bind(parents).bind(&snapshot.digest).bind(kind).bind(generation).bind(admin).bind(plan).bind(operation)
        .execute(connection).await?;
    Ok(())
}

pub(crate) async fn require(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    identity: &RowIdentity,
) -> Result<(), sqlx::Error> {
    if let Some(scope) = scope {
        let routed = effect_scope::resolve(connection, scope, identity).await?;
        let scope = &routed;
        require_row(
            connection,
            &scope.context(),
            scope.domain,
            scope.fence.as_ref(),
            identity,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn route(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    identity: &RowIdentity,
) -> Result<(), sqlx::Error> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let routed = effect_scope::resolve(connection, scope, identity).await?;
    let scope = &routed;
    if scope.anchors.contains(identity) {
        authority::validate(
            connection,
            &scope.context(),
            scope.domain,
            scope.fence.as_ref(),
        )
        .await?;
        let snapshot = inventory::snapshot(connection, identity).await?;
        metadata::validate(
            connection,
            &scope.context(),
            scope.domain,
            &ParentIdentity {
                identity: identity.clone(),
                revision: snapshot.revision,
                digest: snapshot.digest,
                role: ParentRole::MetadataAnchor,
            },
        )
        .await
    } else {
        require_row(
            connection,
            &scope.context(),
            scope.domain,
            scope.fence.as_ref(),
            identity,
        )
        .await
    }
}
