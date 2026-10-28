use sqlx::SqlitePool;

use super::{
    EffectDomain, EligibilityContext, ParentIdentity, RowIdentity, inventory, require_row,
};

async fn database() -> (SqlitePool, tempfile::TempDir) {
    let directory = tempfile::Builder::new()
        .prefix("r092-eligibility-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(directory.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    (pool, directory)
}

fn context() -> EligibilityContext {
    EligibilityContext::NonAdmin {
        generation: "generation".into(),
        revision: 0,
        matrix_enabled: false,
        matrix_operation: None,
    }
}

async fn seed(pool: &SqlitePool, transport: Option<&str>) -> RowIdentity {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO runtime_eligibility_context(singleton,generation,bootstrap_nonce,provenance_kind,matrix_phase,state,revision,created_at,bootstrap_inventory_digest) SELECT 1,'generation',bootstrap_nonce,'pristine_local','absent','ready',0,'2026-10-28T00:00:00Z','inventory' FROM runtime_eligibility_bootstrap WHERE state='pending'")
        .execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,participants_json) VALUES('conversation',?,?, '[]')")
        .bind(transport).bind(transport.map(|_| "external")).execute(&mut *tx).await.unwrap();
    let identity = RowIdentity {
        table: "conversations".into(),
        key: "[\"text:636F6E766572736174696F6E\"]".into(),
    };
    classify(&mut tx, &identity, &[]).await;
    sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='complete',generation='generation',inventory_digest='inventory' WHERE state='pending'")
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    identity
}

async fn classify(
    connection: &mut sqlx::SqliteConnection,
    identity: &RowIdentity,
    parents: &[ParentIdentity],
) {
    let snapshot = inventory::snapshot(connection, identity).await.unwrap();
    sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation) VALUES(?,?,'generation',?,?,?,'eligible_new','inventory','nonadmin','generation')")
        .bind(&identity.table).bind(&identity.key).bind(snapshot.revision).bind(snapshot.digest)
        .bind(serde_json::to_string(parents).unwrap()).execute(connection).await.unwrap();
}

#[tokio::test]
async fn local_eligibility_requires_transaction_exact_digest_and_current_revision() {
    let (pool, _directory) = database().await;
    let identity = seed(&pool, None).await;
    let mut connection = pool.acquire().await.unwrap();
    assert!(
        require_row(
            &mut connection,
            &context(),
            EffectDomain::Local,
            None,
            &identity
        )
        .await
        .is_err()
    );
    drop(connection);
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET participants_json='[\"changed\"]'")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE runtime_eligibility_context SET revision=1 WHERE revision=0")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn matrix_derived_rows_cannot_be_claimed_as_local_or_disabled_matrix() {
    let (pool, _directory) = database().await;
    let identity = seed(&pool, Some("matrix")).await;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    for domain in [EffectDomain::Local, EffectDomain::Matrix] {
        assert!(
            require_row(&mut tx, &context(), domain, None, &identity)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn embedded_nul_tail_cannot_hide_a_changed_parent_digest() {
    let (pool, _directory) = database().await;
    let identity = seed(&pool, Some("http")).await;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET external_id=?")
        .bind("external\0hidden")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn actual_foreign_key_parent_cannot_be_omitted_from_classification() {
    let (pool, _directory) = database().await;
    let parent = seed(&pool, None).await;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','from','to','conversation','work',1,0,0,0)")
        .execute(&mut *tx).await.unwrap();
    let identity = RowIdentity {
        table: "tasks".into(),
        key: "[\"text:7461736B\"]".into(),
    };
    classify(&mut tx, &identity, &[]).await;
    assert!(
        require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
    let parent_state = inventory::snapshot(&mut tx, &parent).await.unwrap();
    let own_state = inventory::snapshot(&mut tx, &identity).await.unwrap();
    let parents = vec![
        ParentIdentity {
            identity: parent,
            revision: parent_state.revision,
            digest: parent_state.digest,
        },
        ParentIdentity {
            identity: identity.clone(),
            revision: own_state.revision,
            digest: own_state.digest,
        },
    ];
    sqlx::query("UPDATE offline_recovery_rows SET parent_relations=? WHERE table_name='tasks'")
        .bind(serde_json::to_string(&parents).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
        .await
        .unwrap();
    sqlx::query("UPDATE conversations SET participants_json='[\"stale-parent\"]'")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        require_row(&mut tx, &context(), EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
}
