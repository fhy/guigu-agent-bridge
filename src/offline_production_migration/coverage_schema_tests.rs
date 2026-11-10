use crate::storage;
#[tokio::test]
async fn coverage_schema_additive_new_discovery_and_old_binary_refusal() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("state.db");
    let pool = storage::connect(&db).await.unwrap();
    let old = super::verification::embedded_through(25);
    old.run(&pool).await.unwrap();
    assert!(super::check_runtime_discovery(&db).is_ok());
    let before:Vec<(String,String)>=sqlx::query_as("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetch_all(&pool).await.unwrap();
    storage::migrate(&pool).await.unwrap();
    assert!(super::check_runtime_discovery(&db).is_ok());
    let after:Vec<(String,String)>=sqlx::query_as("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='matrix_recovery_coverage' ORDER BY name").fetch_all(&pool).await.unwrap();
    assert_eq!(before, after);
    assert!(
        old.run(&pool).await.is_err(),
        "schema25 binary cannot open migrated ledger"
    );
    sqlx::query("UPDATE _sqlx_migrations SET checksum=x'00' WHERE version=26")
        .execute(&pool)
        .await
        .unwrap();
    assert!(super::check_runtime_discovery(&db).is_err());
    pool.close().await;
}
