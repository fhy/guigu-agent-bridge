use guigu_agent_bridge::storage::{
    CompatibilityGate, CutoverStore, MatrixCompatibilityStore, connect, migrate,
};
use sqlx::{Row, SqlitePool};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

async fn database() -> (tempfile::TempDir, SqlitePool) {
    let root = tempfile::tempdir_in(std::env::var_os("HOME").expect("HOME")).unwrap();
    std::fs::set_permissions(
        root.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let pool = connect(root.path().join("bridge.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    (root, pool)
}

#[tokio::test]
async fn installed_and_public_upgrade_reopen_preserve_rows_checksums_and_interrupted_progress() {
    for version in [20, 21] {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bridge.db");
        let pool = connect(&file).await.unwrap();
        MIGRATOR.run_to(version, &pool).await.unwrap();
        sqlx::query(
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('retained','[]')",
        )
        .execute(&pool)
        .await
        .unwrap();
        if version == 21 {
            CutoverStore::prepare(pool.clone(), "old", &"a".repeat(64), &"b".repeat(64))
                .await
                .unwrap();
        }
        let old: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .unwrap();
        MIGRATOR.run_to(22, &pool).await.unwrap();
        pool.close().await;
        let pool = connect(&file).await.unwrap();
        migrate(&pool).await.unwrap();
        migrate(&pool).await.unwrap();
        let retained: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT version,checksum FROM _sqlx_migrations WHERE version<=? ORDER BY version",
        )
        .bind(version)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(retained, old);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM conversations WHERE conversation_id='retained'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
                .fetch_one(&pool)
                .await
                .unwrap(),
            24
        );
        if version == 21 {
            assert!(matches!(
                MatrixCompatibilityStore::new(pool.clone())
                    .gate(true, None)
                    .await
                    .unwrap(),
                CompatibilityGate::Held(_)
            ));
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT operation_id FROM matrix_cutover")
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                "old"
            );
        }
        pool.close().await;
    }
}

#[tokio::test]
async fn failed_compatibility_migration_rolls_back_without_ledger_rewrite() {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(root.path().join("bridge.db")).await.unwrap();
    MIGRATOR.run_to(23, &pool).await.unwrap();
    sqlx::query("CREATE TABLE matrix_recovery_compatibility(blocker TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(migrate(&pool).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        23
    );
    sqlx::query("DROP TABLE matrix_recovery_compatibility")
        .execute(&pool)
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
}

#[tokio::test]
async fn native_decision_and_control_are_atomic_and_bound_across_reopen() {
    let (root, pool) = database().await;
    sqlx::query("CREATE TRIGGER fail_control BEFORE INSERT ON matrix_recovery_control BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&pool).await.unwrap();
    let store = MatrixCompatibilityStore::new(pool.clone());
    assert!(
        store
            .gate(true, Some(&root.path().join("absent")))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM matrix_recovery_compatibility")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_control")
        .execute(&pool)
        .await
        .unwrap();
    let CompatibilityGate::Native(operation) = store
        .gate(true, Some(&root.path().join("absent")))
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let row = sqlx::query("SELECT c.native_operation_id,r.cutover_operation_id,r.watermark FROM matrix_recovery_compatibility c JOIN matrix_recovery_control r ON r.control_id=c.singleton").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>(0), operation);
    assert_eq!(row.get::<String, _>(1), operation);
    assert!(row.get::<Option<String>, _>(2).is_none());
    pool.close().await;
    let pool = connect(root.path().join("bridge.db")).await.unwrap();
    assert_eq!(
        MatrixCompatibilityStore::new(pool)
            .gate(false, None)
            .await
            .unwrap(),
        CompatibilityGate::Native(operation)
    );
}

#[tokio::test]
async fn malformed_native_identity_phase_or_b_never_regenerates_or_rebaselines() {
    for mutation in [
        "DELETE FROM matrix_recovery_control",
        "UPDATE matrix_recovery_control SET cutover_operation_id='different'",
        "UPDATE matrix_recovery_control SET cutover_operation_id=''",
        "UPDATE matrix_recovery_control SET watermark='illegal-baseline-B'",
        "UPDATE matrix_recovery_control SET phase='active'",
        "UPDATE matrix_recovery_control SET phase='active',watermark=''",
        "UPDATE matrix_recovery_control SET phase='refused'",
        "UPDATE matrix_recovery_control SET restart_hold_state='unknown'",
    ] {
        let (_root, pool) = database().await;
        let store = MatrixCompatibilityStore::new(pool.clone());
        let CompatibilityGate::Native(operation) = store.gate(true, None).await.unwrap() else {
            panic!("native")
        };
        sqlx::query(sqlx::AssertSqlSafe(mutation))
            .execute(&pool)
            .await
            .unwrap();
        for enabled in [false, true] {
            assert!(matches!(
                store.gate(enabled, None).await.unwrap(),
                CompatibilityGate::Held(_)
            ));
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT native_operation_id FROM matrix_recovery_compatibility"
                )
                .fetch_one(&pool)
                .await
                .unwrap(),
                operation
            );
        }
    }
}

#[tokio::test]
async fn native_progress_does_not_change_legacy_digest_and_legacy_changes_are_refused() {
    let (_root, pool) = database().await;
    let store = MatrixCompatibilityStore::new(pool.clone());
    let CompatibilityGate::Native(operation) = store.gate(true, None).await.unwrap() else {
        panic!("native")
    };
    let digest: String =
        sqlx::query_scalar("SELECT source_state_digest FROM matrix_recovery_compatibility")
            .fetch_one(&pool)
            .await
            .unwrap();
    for token in ["C", "A", "opaque-not-ordered"] {
        sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark=?")
            .bind(token)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            store.gate(false, None).await.unwrap(),
            CompatibilityGate::Native(operation.clone())
        );
    }
    sqlx::query(
        "INSERT INTO matrix_cutover_required(singleton,operation_id) VALUES(1,'unexpected')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store.gate(false, None).await.unwrap(),
        CompatibilityGate::Held("legacy-source-digest-mismatch")
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_state_digest FROM matrix_recovery_compatibility"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        digest
    );
}
