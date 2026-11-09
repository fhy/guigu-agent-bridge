use super::{history_fixture as fixture, process::Exclusion, state};
use crate::storage::recovery_eligibility::writer::succession::{confirmed, publish};
use std::path::Path;

#[test]
fn mixed_history_crash_before_and_after_commit() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Some(path) = std::env::var_os("R114_CRASH_ROOT") {
        runtime.block_on(crash_child(Path::new(&path)));
        return;
    }
    runtime.block_on(async {
        let template = tempfile::tempdir().unwrap();
        let database = template.path().join("state.db");
        let pool = fixture::seed(&database).await;
        super::history_boot_tests::previous_boot(&pool).await;
        let before = fixture::snapshot(&pool).await;
        let notices = fixture::notices(&pool).await;
        let mut c = pool.acquire().await.unwrap();
        let state = state::read(&mut c).await.unwrap().unwrap();
        drop(c);
        pool.close().await;
        for stage in ["before", "after"] {
            let root = tempfile::tempdir().unwrap();
            let copy = root.path().join("state.db");
            std::fs::copy(&database, &copy).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "native_succession::history_atomic_tests::mixed_history_crash_before_and_after_commit", "--nocapture"])
                .env("R114_CRASH_ROOT", root.path()).env("R114_CRASH_STAGE", stage)
                .output().unwrap();
            assert_eq!(output.status.code(), Some(if stage == "before" {87} else {88}),
                "{}", String::from_utf8_lossy(&output.stderr));
            let proof: Exclusion = serde_json::from_slice(
                &std::fs::read(root.path().join("proof.json")).unwrap()).unwrap();
            assert_eq!(confirmed(&copy, &state, &proof).await.unwrap(), stage == "after");
            let pool = crate::storage::connect(&copy).await.unwrap();
            assert_eq!(fixture::notices(&pool).await, notices);
            if stage == "before" {
                assert_eq!(fixture::snapshot(&pool).await, before);
                publish(&pool, &state, &proof, &[], || Ok(())).await.unwrap();
                assert!(confirmed(&copy, &state, &proof).await.unwrap());
            }
            let mut c = pool.acquire().await.unwrap();
            assert!(state::read(&mut c).await.unwrap().is_none());
            drop(c);
            assert!(publish(&pool, &state, &proof, &[], || Ok(())).await.is_err());
            assert!(confirmed(&copy, &state, &proof).await.unwrap());
            assert_eq!(fixture::notices(&pool).await, notices);
            pool.close().await;
        }
    });
}

async fn crash_child(root: &Path) {
    let pool = crate::storage::connect(root.join("state.db"))
        .await
        .unwrap();
    let mut c = pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = super::history_boot_tests::modeled_proof(state.owner.clone());
    std::fs::write(root.join("proof.json"), serde_json::to_vec(&proof).unwrap()).unwrap();
    let calls = std::cell::Cell::new(0);
    publish(&pool, &state, &proof, &[], || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 && std::env::var("R114_CRASH_STAGE").unwrap() == "before" {
            std::process::exit(87);
        }
        Ok(())
    })
    .await
    .unwrap();
    std::process::exit(88);
}
