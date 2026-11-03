use super::*;
use crate::offline_production_migration::reconciliation_records::{Binding, Completion};
use sqlx::{Connection, Row, SqliteConnection};

#[tokio::test]
async fn schema_inspection_rejects_constraint_and_object_drift() {
    let variants = [
        MARKER_DDL.replace("protocol INTEGER NOT NULL", "protocol INTEGER"),
        MARKER_DDL.replace("CHECK (protocol = 2)", "CHECK (protocol >= 1)"),
        MARKER_DDL.replace(
            "operation_id TEXT NOT NULL UNIQUE",
            "operation_id TEXT NOT NULL",
        ),
        MARKER_DDL.replace(
            "DEFERRABLE INITIALLY DEFERRED",
            "NOT DEFERRABLE INITIALLY IMMEDIATE",
        ),
        MARKER_DDL.replace(
            "REFERENCES offline_recovery_binding(singleton)",
            "REFERENCES offline_recovery_binding(protocol)",
        ),
        MARKER_DDL.replace("STRICT;", ";"),
        format!("{MARKER_DDL} CREATE INDEX r093_extra ON r093_reconciliation_audit(action_id);"),
        format!(
            "{MARKER_DDL} CREATE TRIGGER foreign_trigger AFTER INSERT ON r093_reconciliation_marker BEGIN SELECT 1; END;"
        ),
        format!(
            "{MARKER_DDL} CREATE VIEW foreign_view AS SELECT * FROM r093_reconciliation_marker;"
        ),
    ];
    for ddl in variants {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.clone()))
            .execute(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            schema_state(&mut connection).await.unwrap(),
            SchemaState::Partial,
            "{ddl}"
        );
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn schema_inspection_distinguishes_absent_partial_and_complete() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    assert_eq!(
        schema_state(&mut connection).await.unwrap(),
        SchemaState::None
    );
    create_schema(&mut connection).await.unwrap();
    assert_eq!(
        schema_state(&mut connection).await.unwrap(),
        SchemaState::Complete
    );
    assert!(matches!(
        create_schema(&mut connection).await,
        Err(Failure::ManualRequired)
    ));
    sqlx::query("DROP TABLE r093_reconciliation_audit")
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        schema_state(&mut connection).await.unwrap(),
        SchemaState::Partial
    );
    connection.close().await.unwrap();
}

fn plan() -> Plan {
    Plan {
        protocol: 2,
        plan_digest: "plan".into(),
        operation_id: "operation".into(),
        root_digest: "root".into(),
        original_provenance_digest: "provenance".into(),
        original_executable_digest: "original-executable".into(),
        archive_digest: "archive".into(),
        schema_complete_digest: "schema".into(),
        identity_digest: "identity".into(),
        grant_digest: "grant".into(),
        successor_executable_digest: "successor".into(),
        successor_package_digest: "package".into(),
        successor_source: "source".into(),
        successor_review: "review".into(),
        native_operation_id: "native".into(),
        source_state_digest: "source-state".into(),
        original_binding: Binding {
            db_generation: "generation".into(),
            canonical_db_digest: "database".into(),
            root_digest: "root".into(),
            admin_operation_id: "operation".into(),
            schema_intent_digest: "schema-intent".into(),
            schema_complete_digest: None,
            plan_digest: None,
            identity_digest: None,
            classification_digest: None,
            transition_intent_digest: None,
            transition_complete_digest: None,
            native_operation_id: None,
            phase: "identity_ready".into(),
            revision: 4,
        },
        classifications: Vec::new(),
        inserted_classifications: Vec::new(),
        actions: Vec::new(),
        d1_unknown_chain: None,
        d1_quarantined: None,
        d2_owner: Vec::new(),
        anchors: Vec::new(),
        workspaces: Vec::new(),
        native_at: "2026-01-01T00:00:00Z".into(),
        abandonment_at: "2026-01-01T00:00:00Z".into(),
        action_digest: "action".into(),
        classification_digest: "classification".into(),
    }
}

#[tokio::test]
async fn marker_finalization_preserves_completion_digest() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "CREATE TABLE offline_recovery_binding (
            singleton INTEGER PRIMARY KEY,
            protocol INTEGER NOT NULL,
            phase TEXT NOT NULL
        )",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,phase) VALUES(1,2,'transition_pending')")
        .execute(&mut connection)
        .await
        .unwrap();
    create_schema(&mut connection).await.unwrap();

    let plan = plan();
    insert_marker(
        &mut connection,
        &plan,
        "intent",
        "committed-graph",
        "commit",
        5,
    )
    .await
    .unwrap();
    let completion = Completion {
        protocol: 2,
        operation_id: plan.operation_id.clone(),
        intent_digest: "intent".into(),
        plan_digest: plan.plan_digest.clone(),
        action_digest: plan.action_digest.clone(),
        classification_digest: plan.classification_digest.clone(),
        native_operation_id: plan.native_operation_id.clone(),
        committed_graph_digest: "committed-graph".into(),
        committed_binding_revision: 5,
    };
    finalize_marker(
        &mut connection,
        &plan,
        &completion,
        "completion",
        "final-graph",
        "final-cas",
        "commit",
    )
    .await
    .unwrap();
    let row = sqlx::query(
        "SELECT state,completion_digest,final_graph_digest,final_cas_digest
         FROM r093_reconciliation_marker WHERE singleton=1",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "finalized");
    assert_eq!(row.get::<String, _>("completion_digest"), "completion");
    assert_eq!(row.get::<String, _>("final_graph_digest"), "final-graph");
    assert_eq!(row.get::<String, _>("final_cas_digest"), "final-cas");
}

#[tokio::test]
async fn final_binding_requires_the_complete_unchanged_tuple() {
    let mutations = [
        "UPDATE offline_recovery_binding SET db_generation='changed'",
        "UPDATE offline_recovery_binding SET canonical_db_digest='changed'",
        "UPDATE offline_recovery_binding SET schema_intent_digest='changed'",
        "UPDATE offline_recovery_binding SET schema_complete_digest='changed'",
        "UPDATE offline_recovery_binding SET identity_digest='changed'",
        "UPDATE offline_recovery_binding SET transition_intent_digest='changed'",
        "UPDATE offline_recovery_binding SET revision=6",
        "UPDATE offline_recovery_binding SET transition_complete_digest='changed'",
    ];
    for mutation in std::iter::once(None).chain(mutations.into_iter().map(Some)) {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE offline_recovery_binding(
                singleton INTEGER PRIMARY KEY,protocol INTEGER,phase TEXT,
                db_generation TEXT,canonical_db_digest TEXT,root_digest TEXT,
                admin_operation_id TEXT,schema_intent_digest TEXT,
                schema_complete_digest TEXT,identity_digest TEXT,plan_digest TEXT,
                classification_digest TEXT,transition_intent_digest TEXT,
                native_operation_id TEXT,revision INTEGER,transition_complete_digest TEXT);
             INSERT INTO offline_recovery_binding VALUES(
                1,2,'transition_pending','generation','database','root','operation',
                'schema-intent','schema','identity','plan','classification','intent',
                'native',5,NULL);",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        if let Some(statement) = mutation {
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .unwrap();
        }
        let result = final_binding(&mut connection, &plan(), "intent", "completion").await;
        if mutation.is_none() {
            result.unwrap();
            let actual: (String, i64, String) = sqlx::query_as(
                "SELECT phase,revision,transition_complete_digest FROM offline_recovery_binding",
            )
            .fetch_one(&mut connection)
            .await
            .unwrap();
            assert_eq!(actual, ("baseline_ready".into(), 6, "completion".into()));
        } else {
            assert!(
                matches!(result, Err(Failure::ManualRequired)),
                "{mutation:?}"
            );
            let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
                .fetch_one(&mut connection)
                .await
                .unwrap();
            assert_eq!(phase, "transition_pending");
        }
        connection.close().await.unwrap();
    }
}
