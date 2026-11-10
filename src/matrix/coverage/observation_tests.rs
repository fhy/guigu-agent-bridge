//! Explicitly gated one-sync diagnostic. Never invokes coverage recovery or paging.
use super::*;
use std::{path::PathBuf, time::Duration};

#[tokio::test]
#[ignore = "requires explicitly authorized disposable pair; performs one new initial sync"]
async fn one_new_target_observation() {
    let root = PathBuf::from(std::env::var_os("R112_OBSERVATION_ROOT").expect("copy root"));
    let mut config = crate::config::load(root.join("config.toml")).expect("copy config");
    config.bridge.database = root.join("pair/state/state.db");
    config.transports.matrix.crypto_store_path = Some(root.join("pair/state/crypto"));
    let pool = crate::storage::connect(&config.bridge.database)
        .await
        .unwrap();
    let document: String =
        sqlx::query_scalar("SELECT document FROM matrix_recovery_coverage WHERE coverage_id=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    let row: Coverage = serde_json::from_str(&document).unwrap();
    assert_eq!(row.status, "refused");
    assert!(row.target.is_none());
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("one-sync-spent"))
        .expect("one observation only");
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let client = crate::matrix::MatrixClient::restore(&config.transports.matrix)
            .await
            .map_err(|_| "restore")?;
        tokio::time::timeout(
            Duration::from_secs(30),
            sdk::target(&client, &pool, &row.state),
        )
        .await
        .map_err(|_| "sync-timeout")?
        .map_err(|_| "target-construction")
    })
    .await;
    let (target, reason) = match result {
        Ok(Ok(target)) => (Some(target), "new-observation"),
        Ok(Err(reason)) => (None, reason),
        Err(_) => (None, "total-timeout"),
    };
    if let Some(target) = &target {
        std::fs::write(
            root.join("NEW-target.json"),
            serde_json::to_vec(target).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT document FROM matrix_recovery_coverage WHERE coverage_id=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        document
    );
    let result = serde_json::json!({"kind":"NEW, not live-R5 input","result":reason,
        "seconds":started.elapsed().as_secs_f64(),"has_target":target.is_some(),
        "pages":0,"business_sends":0,"coverage_document_unchanged":true});
    std::fs::write(root.join("observation.json"), result.to_string()).unwrap();
    println!("{result}");
    pool.close().await;
}
