use super::{history_fixture as fixture, process::Exclusion, state};
use crate::storage::recovery_eligibility::writer::succession::{confirmed, publish};

#[tokio::test]
async fn mixed_history_recovery_preserves_twelve_selectors_and_exact_retry() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.db");
    let pool = fixture::seed(&path).await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE is_current=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 12); // R113's unclassified global selector refused here.
    let mut c = pool.acquire().await.unwrap();
    let s = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    assert_eq!(s.owner.token, "unknown");
    assert_eq!(s.owner.revision, 38);
    assert_eq!(s.history.len(), 11);
    let before = fixture::notices(&pool).await;
    let proof = Exclusion::modeled(s.owner.clone());
    publish(&pool, &s, &proof, &[], || Ok(())).await.unwrap();
    assert!(confirmed(&path, &s, &proof).await.unwrap());
    let mut c = pool.acquire().await.unwrap();
    assert!(state::read(&mut c).await.unwrap().is_none());
    drop(c);
    assert_eq!(fixture::notices(&pool).await, before);
    assert!(publish(&pool, &s, &proof, &[], || Ok(())).await.is_err());
    assert!(confirmed(&path, &s, &proof).await.unwrap());
    fixture::scope(&pool).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime_owner_evidence WHERE is_current=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        12
    );
    pool.close().await;
}

#[tokio::test]
async fn history_corruption_and_competitors_refuse_without_writes() {
    let template = tempfile::tempdir().unwrap();
    let template_path = template.path().join("state.db");
    fixture::seed(&template_path).await.close().await;
    for change in [
        "UPDATE runtime_instances SET state='active' WHERE instance_token='controlled-0'",
        "UPDATE runtime_owner_evidence SET child_snapshot=json_set(child_snapshot,'$.epoch',0) WHERE instance_token='controlled-0' AND is_current=1",
        "UPDATE runtime_owner_evidence SET child_snapshot=json_set(child_snapshot,'$.source.operation','foreign') WHERE instance_token='succession-0' AND is_current=1",
        "UPDATE runtime_owner_evidence SET pid=pid+100000 WHERE instance_token='succession-0' AND is_current=1",
        "UPDATE offline_recovery_rows SET original_digest='drift' WHERE table_name='runtime_owner_evidence'",
        "UPDATE matrix_component_leases SET owner_token='controlled-0' WHERE component='OutboxDrain'",
        "UPDATE task_admissions SET runtime_instance='controlled-0',state='running' WHERE task_id='task-0'",
        "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task-0','invocation',1,'active','t','t','t')",
        "UPDATE runtime_owner_evidence SET barrier_state='closed_joined' WHERE instance_token='unknown' AND is_current=1",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("state.db");
        std::fs::copy(&template_path, &path).unwrap();
        let pool = crate::storage::connect(&path).await.unwrap();
        sqlx::query(change).execute(&pool).await.unwrap();
        let before = fixture::snapshot(&pool).await;
        let mut c = pool.acquire().await.unwrap();
        assert!(state::read(&mut c).await.is_err(), "{change}");
        drop(c);
        assert_eq!(fixture::snapshot(&pool).await, before);
        pool.close().await;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.db");
    std::fs::copy(&template_path, &path).unwrap();
    let pool = crate::storage::connect(&path).await.unwrap();
    fixture::start(&pool, "live-competitor").await;
    let before = fixture::snapshot(&pool).await;
    let mut c = pool.acquire().await.unwrap();
    let error = state::read(&mut c).await.unwrap_err();
    assert!(error.to_string().contains("competing-owner"), "{error}");
    drop(c);
    assert_eq!(fixture::snapshot(&pool).await, before);
    pool.close().await;
}

#[tokio::test]
async fn mixed_history_cas_and_late_failure_roll_back_all_publication() {
    let root = tempfile::tempdir().unwrap();
    let pool = fixture::seed(&root.path().join("state.db")).await;
    super::history_boot_tests::previous_boot(&pool).await;
    let mut c = pool.acquire().await.unwrap();
    let s = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = super::history_boot_tests::modeled_proof(s.owner.clone());
    let before = fixture::snapshot(&pool).await;
    let cancel = tokio::sync::Notify::new();
    let cancelled = tokio::select! {
        biased;
        _ = cancel.notified() => true,
        result = publish(&pool, &s, &proof, &[], || {
            cancel.notify_one();
            Ok(())
        }) => panic!("publication completed before cancellation: {result:?}"),
    };
    assert!(cancelled);
    assert_eq!(fixture::snapshot(&pool).await, before);
    let calls = std::cell::Cell::new(0);
    assert!(
        publish(&pool, &s, &proof, &[], || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(state::refuse("injected-precommit"))
            } else {
                Ok(())
            }
        })
        .await
        .is_err()
    );
    assert_eq!(fixture::snapshot(&pool).await, before);
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=json_set(child_snapshot,'$.fixture_race',true) WHERE instance_token='controlled-0' AND is_current=1").execute(&pool).await.unwrap();
    let before = fixture::snapshot(&pool).await;
    assert!(publish(&pool, &s, &proof, &[], || Ok(())).await.is_err());
    assert_eq!(fixture::snapshot(&pool).await, before);
    pool.close().await;
}
