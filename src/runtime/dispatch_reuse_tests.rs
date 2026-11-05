use super::*;
use crate::runtime::ReleaseDisposition;

#[tokio::test]
async fn scoped_released_lease_can_be_reused_by_a_different_task() {
    let seed = Seed::new().await;
    let store = SqliteRuntimeStore::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let resource = ExecutionResourceKey::new(
        seed.endpoint,
        WorkspaceId::from_canonical_path(seed.root.path()).unwrap(),
    );
    let now = Utc::now();
    let AcquireOutcome::Acquired(first) = store
        .acquire(resource, seed.task, now, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("first lease refused")
    };
    store
        .release(&first, ReleaseDisposition::Released, now)
        .await
        .unwrap();
    let before: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=?")
        .bind(writer::text_identity("tasks", &seed.task.to_string()).unwrap().key).fetch_one(&seed.pool).await.unwrap();
    let next_task = TaskId::generate();
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,'local','synthetic',5,0,0,0)")
        .bind(next_task.to_string()).bind(next_task.to_string()).bind(seed.endpoint.to_string()).bind(seed.endpoint.to_string()).execute(&mut *tx).await.unwrap();
    writer::inserted(
        &mut tx,
        Some(&seed.scope),
        &writer::text_identity("tasks", &next_task.to_string()).unwrap(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let AcquireOutcome::Acquired(second) = store
        .acquire(resource, next_task, now, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("second lease refused")
    };
    assert_eq!(second.fence, first.fence + 1);
    assert_eq!(second.task_id, next_task);
    assert_ne!(second.owner, first.owner);
    let after: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=?")
        .bind(writer::text_identity("tasks", &seed.task.to_string()).unwrap().key).fetch_one(&seed.pool).await.unwrap();
    assert_eq!(before, after);
    assert!(
        store
            .renew(&first, now, Duration::from_secs(30))
            .await
            .is_err()
    );
}
