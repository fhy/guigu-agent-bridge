use sqlx::{SqliteConnection, SqlitePool};

use super::{
    EffectDomain, EligibilityContext, LiveFence, ParentIdentity, ParentRole, RowIdentity,
    contract_tests::{classify, key, seed_admin, verify_canonical_identity},
    inventory, require_row,
};

struct Fixture {
    pool: SqlitePool,
    _directory: tempfile::TempDir,
    parent: ParentIdentity,
    context: EligibilityContext,
    fence: LiveFence,
}

impl Fixture {
    async fn create() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("r092-metadata-anchor-")
            .tempdir_in("/tmp")
            .unwrap();
        let pool = crate::storage::connect(directory.path().join("state.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let conversation = verify_canonical_identity(&pool).await;
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        seed_admin(&mut tx).await;
        let identity = RowIdentity {
            table: "conversations".into(),
            key: key(&conversation.id.to_string()),
        };
        classify(&mut tx, &identity, "terminal_evidenced", &[]).await;
        let snapshot = inventory::snapshot(&mut tx, &identity).await.unwrap();
        sqlx::query(
            "UPDATE offline_recovery_binding SET phase='transition_pending',revision=revision+1",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("INSERT INTO offline_recovery_metadata_anchors SELECT table_name,canonical_pk,'metadata_anchor','generation','root','admin','plan',original_revision,original_digest FROM offline_recovery_rows WHERE table_name='conversations'")
            .execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE offline_recovery_binding SET phase='active',revision=revision+1")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        Self {
            pool,
            _directory: directory,
            parent: ParentIdentity {
                identity,
                revision: snapshot.revision,
                digest: snapshot.digest,
                role: ParentRole::MetadataAnchor,
            },
            context: EligibilityContext::Admin {
                generation: "generation".into(),
                root: "root".into(),
                operation: "admin".into(),
                plan: "plan".into(),
                revision: 2,
                matrix_enabled: true,
                matrix_operation: "native".into(),
            },
            fence: LiveFence {
                component: "MatrixSync".into(),
                owner: "owner".into(),
                epoch: 1,
            },
        }
    }

    async fn message(&self, connection: &mut SqliteConnection, name: &str) -> RowIdentity {
        let conversation: String = sqlx::query_scalar("SELECT conversation_id FROM conversations")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages(message_id,conversation_id,sender,recipient,body,metadata_json) VALUES(?,?,'synthetic','synthetic','new-only','{}')")
            .bind(name).bind(conversation).execute(&mut *connection).await.unwrap();
        let identity = RowIdentity {
            table: "messages".into(),
            key: key(name),
        };
        classify(
            connection,
            &identity,
            "eligible_new",
            std::slice::from_ref(&self.parent),
        )
        .await;
        identity
    }

    async fn check(
        &self,
        connection: &mut SqliteConnection,
        row: &RowIdentity,
    ) -> Result<(), sqlx::Error> {
        require_row(
            connection,
            &self.context,
            EffectDomain::Matrix,
            Some(&self.fence),
            row,
        )
        .await
    }
}

#[tokio::test]
async fn new_work_uses_exact_terminal_route_anchor_but_terminal_root_remains_denied() {
    let fixture = Fixture::create().await;
    let mut tx = fixture.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let message = fixture.message(&mut tx, "fresh-idempotency-key").await;
    fixture.check(&mut tx, &message).await.unwrap();
    assert!(
        fixture
            .check(&mut tx, &fixture.parent.identity)
            .await
            .is_err()
    );
    assert!(sqlx::query("UPDATE offline_recovery_rows SET disposition='eligible_new' WHERE table_name='conversations'")
        .execute(&mut *tx).await.is_err());
    tx.commit().await.unwrap();
    fixture.pool.close().await;
    let restarted = crate::storage::connect(fixture._directory.path().join("state.db"))
        .await
        .unwrap();
    let mut tx = restarted.begin_with("BEGIN IMMEDIATE").await.unwrap();
    fixture.check(&mut tx, &message).await.unwrap();
    assert!(
        sqlx::query(
            "INSERT INTO messages SELECT * FROM messages WHERE message_id='fresh-idempotency-key'"
        )
        .execute(&mut *tx)
        .await
        .is_err()
    );
    let disposition: String = sqlx::query_scalar(
        "SELECT disposition FROM offline_recovery_rows WHERE table_name='conversations'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(disposition, "terminal_evidenced");
}

#[tokio::test]
async fn anchor_mutation_replacement_and_direct_role_forgery_refuse() {
    let fixture = Fixture::create().await;
    let mut tx = fixture.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let message = fixture.message(&mut tx, "new-message").await;
    for statement in [
        "UPDATE conversations SET participants_json='[\"changed\"]'",
        "DELETE FROM conversations",
        "INSERT OR REPLACE INTO conversations SELECT * FROM conversations",
        "UPDATE offline_recovery_metadata_anchors SET original_digest='forged'",
        "DELETE FROM offline_recovery_metadata_anchors",
        "INSERT OR REPLACE INTO offline_recovery_metadata_anchors SELECT * FROM offline_recovery_metadata_anchors",
        "INSERT INTO offline_recovery_metadata_anchors SELECT 'messages',canonical_pk,'metadata_anchor','generation','root','admin','plan',original_revision,original_digest FROM offline_recovery_rows WHERE table_name='messages'",
    ] {
        assert!(
            sqlx::query(statement).execute(&mut *tx).await.is_err(),
            "{statement}"
        );
    }
    let mut forged = fixture.parent.clone();
    forged.digest = "replacement".into();
    assert!(
        sqlx::query(
            "UPDATE offline_recovery_rows SET parent_relations=? WHERE table_name='messages'"
        )
        .bind(serde_json::to_string(std::slice::from_ref(&forged)).unwrap())
        .execute(&mut *tx)
        .await
        .is_err()
    );
    assert!(
        super::metadata::validate(&mut tx, &fixture.context, EffectDomain::Matrix, &forged)
            .await
            .is_err()
    );
    fixture.check(&mut tx, &message).await.unwrap();
}

#[tokio::test]
async fn disabled_wrong_root_plan_epoch_and_missing_b_deny_metadata_anchors() {
    let fixture = Fixture::create().await;
    let mut tx = fixture.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let message = fixture.message(&mut tx, "new-message").await;
    for changed in ["root", "plan", "disabled", "epoch"] {
        let mut context = fixture.context.clone();
        let mut fence = fixture.fence.clone();
        if let EligibilityContext::Admin {
            root,
            plan,
            matrix_enabled,
            ..
        } = &mut context
        {
            match changed {
                "root" => *root = "other-root".into(),
                "plan" => *plan = "other-plan".into(),
                "disabled" => *matrix_enabled = false,
                _ => fence.epoch += 1,
            }
        }
        assert!(
            require_row(
                &mut tx,
                &context,
                EffectDomain::Matrix,
                Some(&fence),
                &message
            )
            .await
            .is_err()
        );
    }
    assert!(
        require_row(
            &mut tx,
            &fixture.context,
            EffectDomain::Local,
            Some(&fixture.fence),
            &message
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE matrix_recovery_control SET phase='baseline',watermark=NULL")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(fixture.check(&mut tx, &message).await.is_err());
}

#[tokio::test]
async fn held_context_and_missing_effectful_parent_cannot_use_anchor_escape() {
    let fixture = Fixture::create().await;
    let mut tx = fixture.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let historical = fixture.message(&mut tx, "old-message").await;
    sqlx::query("UPDATE offline_recovery_rows SET disposition='terminal_evidenced' WHERE table_name='messages'")
        .execute(&mut *tx).await.unwrap();
    let message = fixture.message(&mut tx, "new-message").await;
    sqlx::query("UPDATE messages SET reply_to='old-message' WHERE message_id='new-message'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let snapshot = inventory::snapshot(&mut tx, &message).await.unwrap();
    sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name='messages' AND canonical_pk=?")
        .bind(snapshot.revision).bind(snapshot.digest).bind(&message.key)
        .execute(&mut *tx).await.unwrap();
    assert!(fixture.check(&mut tx, &message).await.is_err());
    let old = inventory::snapshot(&mut tx, &historical).await.unwrap();
    let forged = ParentIdentity {
        identity: historical,
        revision: old.revision,
        digest: old.digest,
        role: ParentRole::MetadataAnchor,
    };
    assert!(sqlx::query("UPDATE offline_recovery_rows SET parent_relations=? WHERE table_name='messages' AND canonical_pk=?")
        .bind(serde_json::to_string(&[fixture.parent.clone(), forged]).unwrap()).bind(&message.key)
        .execute(&mut *tx).await.is_err());
    assert!(fixture.check(&mut tx, &message).await.is_err());
    sqlx::query("UPDATE offline_recovery_rows SET disposition='uncertain_hold' WHERE table_name='conversations'")
        .execute(&mut *tx).await.unwrap();
    assert!(fixture.check(&mut tx, &message).await.is_err());
}
