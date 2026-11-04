use sqlx::{AssertSqlSafe, Connection, Row};

use super::super::reconciliation_d1_fixture as fixture;
use super::{Failure, apply_chain, d1_tuple_tests::fixture_plan};

async fn refused(sql: &str, refresh: bool) {
    let (_root, mut connection) = fixture::database().await;
    fixture::seed(&mut connection, ["terminal", "recovery_needed"]).await;
    let mut plan = fixture_plan();
    fixture::populate(&mut connection, &mut plan, "a").await;
    sqlx::query(AssertSqlSafe(sql))
        .execute(&mut connection)
        .await
        .unwrap();
    if refresh {
        plan.actions.clear();
        fixture::populate(&mut connection, &mut plan, "a").await;
    }
    let before = fixture::snapshot(&mut connection).await;
    let mut transaction = connection.begin().await.unwrap();
    assert!(
        matches!(
            apply_chain(&mut transaction, &plan).await,
            Err(Failure::ManualRequired)
        ),
        "{sql}"
    );
    transaction.rollback().await.unwrap();
    assert_eq!(before, fixture::snapshot(&mut connection).await, "{sql}");
    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(audit, 0, "{sql}");
}

#[tokio::test]
async fn terminal_chain_refuses_stale_full_images_and_parent_identity() {
    for sql in [
        "UPDATE tasks SET version=version+1 WHERE task_id='task-a'",
        "UPDATE task_admissions SET revision=revision+1 WHERE task_id='task-a'",
        "UPDATE task_continuations SET revision=revision+1 WHERE task_id='task-a'",
        "UPDATE execution_leases SET fence=fence+1 WHERE task_id='task-a'",
        "UPDATE execution_leases SET owner_token='other' WHERE task_id='task-a'",
        "UPDATE task_continuations SET runtime_generation='other' WHERE task_id='task-a'",
        "UPDATE task_continuations SET delivery_id='delivery-b' WHERE task_id='task-a'",
        "UPDATE task_continuations SET resource_key='resource-b' WHERE task_id='task-a'",
        "UPDATE delivery_dispositions SET attempt=3 WHERE task_id='task-a'",
        "UPDATE delivery_dispositions SET session_id='other' WHERE task_id='task-a'",
        "UPDATE task_admissions SET state='recovery_needed' WHERE task_id='task-a'",
        "UPDATE task_admissions SET reply_room='drift-without-revision' WHERE task_id='task-a'",
        "UPDATE runtime_instances SET process_fingerprint='changed' WHERE instance_token='runtime-a'",
        "UPDATE sessions SET updated_at='changed' WHERE session_id='session-a'",
        "DELETE FROM delivery_dispositions WHERE task_id='task-a'",
        "INSERT INTO runtime_instances VALUES('extra','t','t','stopped','extra')",
    ] {
        refused(sql, false).await;
    }
}

#[tokio::test]
async fn terminal_chain_refuses_supported_state_without_parent_or_exact_delivery() {
    for sql in [
        "UPDATE runtime_instances SET state='active' WHERE instance_token='runtime-a'",
        "UPDATE runtime_instances SET state='stopping' WHERE instance_token='runtime-a'",
        "UPDATE runtime_instances SET process_fingerprint='' WHERE instance_token='runtime-a'",
        "UPDATE runtime_instances SET process_fingerprint='  ' WHERE instance_token='runtime-a'",
        "UPDATE task_admissions SET runtime_instance=NULL WHERE task_id='task-a'",
        "UPDATE deliveries SET attempt=3 WHERE delivery_id='delivery-a'",
        "UPDATE execution_leases SET state='released' WHERE task_id='task-a'",
        "UPDATE task_continuations SET state='terminal' WHERE task_id='task-a'",
        "UPDATE delivery_dispositions SET state='acknowledged' WHERE task_id='task-a'",
        "UPDATE delivery_dispositions SET session_id=NULL WHERE task_id='task-a'",
        "INSERT INTO execution_leases VALUES('extra','task-a','owner-a',7,'recovery_needed','t','t','later')",
        "UPDATE delivery_dispositions SET task_id='task-a',attempt=3 WHERE delivery_id='delivery-b'",
    ] {
        refused(sql, true).await;
    }
    for state in ["ready", "enqueued", "dispatching", "running"] {
        refused(
            &format!("UPDATE task_admissions SET state='{state}' WHERE task_id='task-a'"),
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn terminal_chain_binds_all_eleven_tuple_fields() {
    for index in 0..11 {
        let (_root, mut connection) = fixture::database().await;
        fixture::seed(&mut connection, ["terminal", "recovery_needed"]).await;
        let mut plan = fixture_plan();
        fixture::populate(&mut connection, &mut plan, "a").await;
        plan.d1_unknown_chain.as_mut().unwrap().references[index] =
            if [4, 5, 6, 7, 10].contains(&index) {
                "999".into()
            } else {
                "other".into()
            };
        let before = fixture::snapshot(&mut connection).await;
        let mut transaction = connection.begin().await.unwrap();
        assert!(
            matches!(
                apply_chain(&mut transaction, &plan).await,
                Err(Failure::ManualRequired)
            ),
            "tuple field {index}"
        );
        transaction.rollback().await.unwrap();
        assert_eq!(before, fixture::snapshot(&mut connection).await);
    }
}

#[tokio::test]
async fn terminal_chain_audit_first_and_each_cas_failure_roll_back_every_row() {
    for target in [
        "historical_unknown_abandonments",
        "execution_leases",
        "task_continuations",
        "delivery_dispositions",
    ] {
        for effect in [
            "SELECT RAISE(IGNORE);",
            "SELECT RAISE(ABORT,'injected');",
            "UPDATE task_admissions SET reply_room='trigger-drift' WHERE task_id='task-a';",
        ] {
            let (_root, mut connection) = fixture::database().await;
            fixture::seed(&mut connection, ["terminal", "recovery_needed"]).await;
            let mut plan = fixture_plan();
            fixture::populate(&mut connection, &mut plan, "a").await;
            let event = if target == "historical_unknown_abandonments" {
                "INSERT"
            } else {
                "UPDATE"
            };
            sqlx::query(AssertSqlSafe(format!(
                "CREATE TEMP TRIGGER injected BEFORE {event} ON {target} BEGIN {effect} END"
            )))
            .execute(&mut connection)
            .await
            .unwrap();
            let before = fixture::snapshot(&mut connection).await;
            let mut transaction = connection.begin().await.unwrap();
            super::super::reconciliation_sql::create_schema(&mut transaction)
                .await
                .unwrap();
            super::super::reconciliation_sql::insert_audit(&mut transaction, &plan)
                .await
                .unwrap();
            assert!(
                matches!(
                    apply_chain(&mut transaction, &plan).await,
                    Err(Failure::ManualRequired)
                ),
                "{target} {effect}"
            );
            transaction.rollback().await.unwrap();
            assert_eq!(before, fixture::snapshot(&mut connection).await);
            let audit: i64 =
                sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
                    .fetch_one(&mut connection)
                    .await
                    .unwrap();
            assert_eq!(audit, 0, "{target} {effect}");
            let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name IN ('r093_reconciliation_marker','r093_reconciliation_audit')").fetch_one(&mut connection).await.unwrap();
            assert_eq!(
                tables, 0,
                "reconciliation schema/audit rollback: {target} {effect}"
            );
            let native: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control")
                .fetch_one(&mut connection)
                .await
                .unwrap();
            let classified: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows")
                .fetch_one(&mut connection)
                .await
                .unwrap();
            assert_eq!((native, classified), (0, 0));
            assert!(
                sqlx::query("PRAGMA foreign_key_check")
                    .fetch_all(&mut connection)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
async fn terminal_chain_audit_precedes_exactly_once_ordered_mutations() {
    let (_root, mut connection) = fixture::database().await;
    fixture::seed(&mut connection, ["terminal", "recovery_needed"]).await;
    sqlx::query("CREATE TEMP TABLE write_order(stage TEXT NOT NULL)")
        .execute(&mut connection)
        .await
        .unwrap();
    for (table, event, stage) in [
        ("historical_unknown_abandonments", "INSERT", "audit"),
        ("execution_leases", "UPDATE", "lease"),
        ("task_continuations", "UPDATE", "continuation"),
        ("delivery_dispositions", "UPDATE", "disposition"),
    ] {
        sqlx::query(AssertSqlSafe(format!("CREATE TEMP TRIGGER trace_{stage} AFTER {event} ON {table} BEGIN INSERT INTO write_order VALUES('{stage}'); END"))).execute(&mut connection).await.unwrap();
    }
    let mut plan = fixture_plan();
    fixture::populate(&mut connection, &mut plan, "a").await;
    let mut transaction = connection.begin().await.unwrap();
    apply_chain(&mut transaction, &plan).await.unwrap();
    transaction.commit().await.unwrap();
    let order = sqlx::query("SELECT stage FROM write_order ORDER BY rowid")
        .fetch_all(&mut connection)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>();
    assert_eq!(order, ["audit", "lease", "continuation", "disposition"]);
}
