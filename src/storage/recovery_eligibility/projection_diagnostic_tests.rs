use super::*;
use tracing::instrument::WithSubscriber;

#[tokio::test]
async fn completion_diagnostics_distinguish_cas_authority_and_transaction_stages() {
    for (fault, step, class) in [
        ("cas", "claim-cas", "cas-mismatch"),
        ("fence", "begin", "authority-or-classification"),
        ("prepare", "prepare", "authority-or-classification"),
        ("update", "update", "database"),
        ("classification", "classification", "database"),
        ("commit", "commit", "database"),
    ] {
        let (f, store) = seeded().await;
        let mut rows = store
            .claim_projections("claim", "now", "unused", 1)
            .await
            .unwrap();
        inject(&f, fault).await;
        if fault == "cas" {
            rows[0].claim_revision += 1;
        }
        let before: Vec<(String, String, String)> = sqlx::query_as("SELECT canonical_pk,original_digest,parent_relations FROM offline_recovery_rows ORDER BY table_name,canonical_pk").fetch_all(&f.pool).await.unwrap();
        let capture = crate::test_diagnostics::Capture::default();
        let result = store
            .mark_projection_sent(&rows[0])
            .with_subscriber(capture.subscriber())
            .await;
        if fault == "cas" {
            assert!(!result.unwrap());
        } else {
            assert!(result.is_err());
        }
        let log = capture.output();
        assert!(log.contains(&format!("step=\"{step}\"")), "{log}");
        assert!(log.contains(&format!("class=\"{class}\"")), "{log}");
        assert!(!log.contains("PRIVATE-SQL-VALUE"));
        assert_eq!(state(&f).await, ("sending".into(), 0, 1));
        let after: Vec<(String, String, String)> = sqlx::query_as("SELECT canonical_pk,original_digest,parent_relations FROM offline_recovery_rows ORDER BY table_name,canonical_pk").fetch_all(&f.pool).await.unwrap();
        assert_eq!(before, after, "rollback {step}");
        println!(
            "completion fault={fault} step={step} class={class} sending-retained=true rollback=true"
        );
    }
}

async fn inject(f: &Fixture, fault: &str) {
    let statements: &[&str] = match fault {
        "cas" => &[],
        "fence" => &[
            "UPDATE matrix_component_leases SET owner_token='other' WHERE component='OutboxDrain'",
        ],
        "prepare" => &["UPDATE projection_outbox SET body_hash='tampered-fixture'"],
        "update" => &[
            "CREATE TRIGGER fail_terminal BEFORE UPDATE ON projection_outbox WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'PRIVATE-SQL-VALUE'); END",
        ],
        "classification" => &[
            "CREATE TRIGGER fail_classification BEFORE UPDATE ON offline_recovery_rows WHEN NEW.table_name='projection_outbox' BEGIN SELECT RAISE(ABORT,'PRIVATE-SQL-VALUE'); END",
        ],
        "commit" => &[
            "CREATE TABLE deferred_parent(id INTEGER PRIMARY KEY)",
            "CREATE TABLE deferred_child(id INTEGER REFERENCES deferred_parent(id) DEFERRABLE INITIALLY DEFERRED)",
            "CREATE TRIGGER fail_commit AFTER UPDATE ON projection_outbox WHEN NEW.state='sent' BEGIN INSERT INTO deferred_child VALUES(99); END",
        ],
        _ => unreachable!(),
    };
    for statement in statements {
        sqlx::query(sqlx::AssertSqlSafe(*statement))
            .execute(&f.pool)
            .await
            .unwrap();
    }
}
