use crate::{
    models::{Conversation, ConversationId, ExternalRef, TransportType},
    storage::{Repository, SqliteRepository},
};

use super::{EffectDomain, EligibilityContext, LiveFence, ParentIdentity, RowIdentity, inventory};

fn key(value: &str) -> String {
    let hexadecimal = value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    serde_json::to_string(&vec![format!("text:{hexadecimal}")]).unwrap()
}

async fn classify(
    connection: &mut sqlx::SqliteConnection,
    identity: &RowIdentity,
    disposition: &str,
    parents: &[ParentIdentity],
) {
    let snapshot = inventory::snapshot(connection, identity).await.unwrap();
    sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,admin_operation_id,plan_digest,matrix_operation_id) VALUES(?,?,'generation',?,?,?,?,'explicit-synthetic-plan','admin','admin','plan','native')")
        .bind(&identity.table).bind(&identity.key).bind(snapshot.revision).bind(snapshot.digest)
        .bind(serde_json::to_string(parents).unwrap()).bind(disposition)
        .execute(connection).await.unwrap();
}

#[tokio::test]
async fn admin_terminal_conversation_has_no_fresh_identity_or_eligible_parent_escape() {
    let root = tempfile::Builder::new()
        .prefix("r092-admin-parent-contract-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let repository = SqliteRepository::new(pool.clone());
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: Vec::new(),
        external_ref: Some(ExternalRef {
            transport: TransportType::Matrix,
            external_id: "!synthetic:example.test".into(),
            thread_ref: None,
        }),
    };
    repository.insert_conversation(&conversation).await.unwrap();
    let resolved =
        crate::matrix::resolve_conversation(&repository, "!synthetic:example.test", None)
            .await
            .unwrap();
    assert_eq!(resolved.id, conversation.id);
    let replacement = Conversation {
        id: ConversationId::generate(),
        ..conversation.clone()
    };
    assert!(matches!(
        repository.insert_conversation(&replacement).await,
        Err(crate::storage::StorageError::Duplicate { .. })
    ));
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    seed_admin(&mut tx).await;
    let parent = RowIdentity {
        table: "conversations".into(),
        key: key(&conversation.id.to_string()),
    };
    classify(&mut tx, &parent, "terminal_evidenced", &[]).await;
    let snapshot = inventory::snapshot(&mut tx, &parent).await.unwrap();
    sqlx::query("INSERT INTO messages(message_id,conversation_id,sender,recipient,body,metadata_json) VALUES('new-message',?,'synthetic-sender','synthetic-recipient','new-only','{}')")
        .bind(conversation.id.to_string()).execute(&mut *tx).await.unwrap();
    let child = RowIdentity {
        table: "messages".into(),
        key: key("new-message"),
    };
    let parents = [ParentIdentity {
        identity: parent,
        revision: snapshot.revision,
        digest: snapshot.digest,
    }];
    classify(&mut tx, &child, "eligible_new", &parents).await;
    let context = EligibilityContext::Admin {
        generation: "generation".into(),
        operation: "admin".into(),
        plan: "plan".into(),
        revision: 0,
        matrix_enabled: true,
        matrix_operation: "native".into(),
    };
    let fence = LiveFence {
        component: "MatrixSync".into(),
        owner: "owner".into(),
        epoch: 1,
    };
    super::authority::validate(&mut tx, &context, EffectDomain::Matrix, Some(&fence))
        .await
        .unwrap();
    super::validate_node(&mut tx, &context, EffectDomain::Matrix, &child, None)
        .await
        .unwrap();
    assert!(
        super::require_row(
            &mut tx,
            &context,
            EffectDomain::Matrix,
            Some(&fence),
            &child
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

async fn seed_admin(connection: &mut sqlx::SqliteConnection) {
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,schema_complete_digest,plan_digest,identity_digest,classification_digest,transition_intent_digest,transition_complete_digest,native_operation_id,phase,revision) VALUES(1,2,'generation','database','root','admin','schema-intent','schema-complete','plan','identity','classification','transition-intent','transition-complete','native','active',0)")
        .execute(&mut *connection).await.unwrap();
    sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='admin_reserved'")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,'native','active','opaque-B','2026-10-28T00:00:00Z')")
        .execute(&mut *connection).await.unwrap();
    sqlx::query("INSERT INTO matrix_recovery_compatibility(singleton,decision,source_schema_version,source_state_digest,native_operation_id,reason_code,created_at) VALUES(1,'native',25,'explicit-synthetic-plan','native','synthetic-contract-fixture','2026-10-28T00:00:00Z')")
        .execute(&mut *connection).await.unwrap();
    sqlx::query("INSERT INTO matrix_component_leases(component,owner_token,epoch,state,expires_at,updated_at) VALUES('MatrixSync','owner',1,'active','2099-01-01T00:00:00Z','2026-10-28T00:00:00Z')")
        .execute(connection).await.unwrap();
}
