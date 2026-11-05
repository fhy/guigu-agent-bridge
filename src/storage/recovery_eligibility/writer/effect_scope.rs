use sqlx::SqliteConnection;

use super::{EffectDomain, RecoveryScope, RowIdentity, inventory};

pub(super) async fn resolve(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    identity: &RowIdentity,
) -> Result<RecoveryScope, sqlx::Error> {
    let mut routed = scope.clone();
    if !matches!(scope.domain, EffectDomain::Local | EffectDomain::LocalHold)
        || scope.matrix_fence.get().is_none()
    {
        return Ok(routed);
    }
    let snapshot = inventory::snapshot(connection, identity).await?;
    let mut matrix = snapshot.matrix;
    for candidate in std::iter::once(identity).chain(snapshot.parents.iter()) {
        let classified: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND matrix_operation_id IS NOT NULL")
            .bind(&candidate.table).bind(&candidate.key).fetch_one(&mut *connection).await?;
        matrix |= classified != 0;
    }
    if matrix {
        routed.domain = EffectDomain::Matrix;
        routed.fence = scope.matrix_fence.get().cloned();
    }
    Ok(routed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::recovery_eligibility::{LiveFence, bootstrap_nonadmin, writer};
    use crate::storage::{CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore};

    #[tokio::test]
    async fn matrix_provenance_requires_installed_live_fence_without_restricting_local_rows() {
        let root = tempfile::Builder::new()
            .prefix("r092-effect-scope-")
            .tempdir_in("/tmp")
            .unwrap();
        let pool = crate::storage::connect(root.path().join("state.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap()
        else {
            panic!("native required")
        };
        let context = bootstrap_nonadmin(&pool, true).await.unwrap();
        let scope = RecoveryScope::new(context, EffectDomain::Local, None);
        let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
        store
            .activate_cutover(&operation, "B", "activation")
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let epoch = store
            .acquire_component_lease(
                "MatrixSync",
                "owner",
                &now.to_rfc3339(),
                &(now + chrono::Duration::minutes(1)).to_rfc3339(),
            )
            .await
            .unwrap();
        let matrix = writer::text_identity("conversations", "matrix").unwrap();
        let absent = RecoveryScope::new(scope.context(), EffectDomain::Local, None);
        let mut transaction = writer::begin(&pool, Some(&absent)).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,participants_json) VALUES('matrix','matrix','room','[]')").execute(&mut *transaction).await.unwrap();
        assert!(
            writer::inserted(&mut transaction, Some(&absent), &matrix)
                .await
                .is_err()
        );
        transaction.rollback().await.unwrap();
        scope
            .install_matrix_fence(LiveFence {
                component: "MatrixSync".into(),
                owner: "owner".into(),
                epoch,
            })
            .unwrap();
        assert!(
            scope
                .install_matrix_fence(LiveFence {
                    component: "MatrixSync".into(),
                    owner: "other".into(),
                    epoch
                })
                .is_err()
        );
        let mut transaction = writer::begin(&pool, Some(&scope)).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,participants_json) VALUES('matrix','matrix','room','[]')").execute(&mut *transaction).await.unwrap();
        writer::inserted(&mut transaction, Some(&scope), &matrix)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z'")
            .execute(&pool)
            .await
            .unwrap();
        let mut transaction = writer::begin(&pool, Some(&scope)).await.unwrap();
        assert!(
            writer::require(&mut transaction, Some(&scope), &matrix)
                .await
                .is_err()
        );
        sqlx::query(
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('local','[]')",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        let local = writer::text_identity("conversations", "local").unwrap();
        writer::inserted(&mut transaction, Some(&scope), &local)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let matrix_operation: Option<String> = sqlx::query_scalar("SELECT matrix_operation_id FROM offline_recovery_rows WHERE table_name='conversations' AND canonical_pk=?").bind(&local.key).fetch_one(&pool).await.unwrap();
        assert_eq!(matrix_operation, None);
    }
}
