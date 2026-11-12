use super::*;
use std::sync::Arc;

struct CrashSender(std::path::PathBuf);
impl crate::matrix::MatrixOutboxSender for CrashSender {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            // Durable external-effect witness in the disposable fixture. Exit before
            // returning the result or running the completion transaction/destructors.
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.0)
                .unwrap();
            std::io::Write::write_all(&mut file, b"one simulated external effect\n").unwrap();
            file.sync_all().unwrap();
            std::process::exit(86);
        })
    }
}

#[tokio::test]
#[ignore = "child only; launched by crash test with isolated synthetic fixture"]
async fn completion_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("R112_COMPLETION_CRASH_FIXTURE").unwrap());
    let context =
        serde_json::from_slice(&std::fs::read(root.join("context.json")).unwrap()).unwrap();
    let pool = crate::storage::connect(root.join("state.db"))
        .await
        .unwrap();
    let scope = RecoveryScope::new(
        context,
        EffectDomain::Matrix,
        Some(crate::storage::recovery_eligibility::LiveFence {
            component: "OutboxDrain".into(),
            owner: "outbox-owner".into(),
            epoch: 1,
        }),
    );
    let drain = crate::app::OutboxDrain::new(
        ReliabilityStore::with_recovery(pool, scope),
        Arc::new(CrashSender(root.join("effect"))),
        1,
    );
    drain.drain_once().await.unwrap();
    panic!("must exit before completion");
}

struct NeverSend;
impl crate::matrix::MatrixOutboxSender for NeverSend {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async { panic!("crashed sending claim must not be resent") })
    }
}

#[tokio::test]
async fn process_exit_before_completion_reopens_sending_without_resend() {
    let (f, store) = seeded().await;
    std::fs::write(
        f._directory.path().join("context.json"),
        serde_json::to_vec(&f.context).unwrap(),
    )
    .unwrap();
    f.pool.close().await;
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "completion_crash_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("R112_COMPLETION_CRASH_FIXTURE", f._directory.path())
        .kill_on_drop(true);
    let status = tokio::time::timeout(std::time::Duration::from_secs(30), child.status())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    let witness = std::fs::read(f._directory.path().join("effect")).unwrap();
    let pool = crate::storage::connect(f._directory.path().join("state.db"))
        .await
        .unwrap();
    let scoped =
        ReliabilityStore::with_recovery(pool.clone(), store.test_recovery_scope().unwrap());
    let drain = crate::app::OutboxDrain::new(scoped, Arc::new(NeverSend), 1);
    assert!(drain.clone().drain_once().await.is_err());
    assert!(drain.drain_once().await.is_err());
    let row: (String, i64, i64) =
        sqlx::query_as("SELECT state,attempt_count,claim_revision FROM projection_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row, ("sending".into(), 0, 1));
    assert_eq!(
        std::fs::read(f._directory.path().join("effect")).unwrap(),
        witness
    );
    pool.close().await;
    println!("child-exit=86 sending=1 simulated-effect=1 reopen-send=0");
}
