use super::{bootstrap_nonadmin, diagnostics};
use crate::test_diagnostics::Capture;
use tracing::instrument::WithSubscriber;

#[test]
fn bootstrap_error_diagnostics_never_log_driver_or_protocol_payloads() {
    let captured = Capture::default();
    tracing::subscriber::with_default(captured.subscriber(), || {
        let _ = diagnostics::stage(
            "inventory",
            sqlx::Error::Protocol("secret-token SQL body primary-key crypto-material".into()),
        );
        let _ = diagnostics::stage(
            "snapshot",
            sqlx::Error::ColumnNotFound("secret-token SQL body primary-key crypto-material".into()),
        );
        let _ = diagnostics::refusal("inventory", "classification-cardinality");
    });
    let output = captured.output();
    for required in [
        "inventory",
        "snapshot",
        "predicate",
        "classification-cardinality",
    ] {
        assert!(output.contains(required));
    }
    for secret in ["secret-token", "SQL body", "primary-key", "crypto-material"] {
        assert!(!output.contains(secret));
    }
}

#[tokio::test]
async fn bootstrap_missing_classification_reports_counts_and_rolls_back() {
    let (pool, _root) = super::bootstrap_tests::database().await;
    bootstrap_nonadmin(&pool, false).await.unwrap();
    // Deliberately unexplained synthetic mutation, never a retained-data repair.
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES('secret-row-key','[]')")
        .execute(&pool).await.unwrap();
    let captured = Capture::default();
    assert!(
        bootstrap_nonadmin(&pool, false)
            .with_subscriber(captured.subscriber())
            .await
            .is_err()
    );
    let output = captured.output();
    assert!(output.contains("classification-cardinality"));
    assert!(output.contains("live=1"));
    assert!(output.contains("classified=0"));
    assert!(!output.contains("secret-row-key"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "explicit private disposable paired-copy diagnostic; no network or Agent calls"]
async fn retained_copy_reports_actual_bootstrap_first_failure() {
    let path = std::env::var_os("R115_COPY_DATABASE").expect("explicit disposable copy required");
    let pool = crate::storage::connect(std::path::PathBuf::from(path))
        .await
        .unwrap();
    let captured = Capture::default();
    let result = bootstrap_nonadmin(&pool, true)
        .with_subscriber(captured.subscriber())
        .await;
    println!("{}", captured.output());
    assert!(
        result.is_err(),
        "refusal diagnostic is not a recovery acceptance"
    );
    println!(
        "bootstrap_outcome=refused error_class={}",
        diagnostics::class(&result.unwrap_err())
    );
    pool.close().await;
}
