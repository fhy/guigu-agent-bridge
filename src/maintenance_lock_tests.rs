use super::*;
use std::os::unix::fs::MetadataExt;

#[tokio::test]
async fn exclusion_and_cancel_release_preserve_lock_inode() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    std::fs::write(&database, b"synthetic").unwrap();
    let lock = MaintenanceLock::acquire(&database).unwrap();
    let lock_path = root.path().join(".state.db.offline-migration.lock");
    let before = std::fs::metadata(&lock_path).unwrap();
    assert!(MaintenanceLock::acquire(&database).is_err());
    drop(lock);
    let (ready, received) = tokio::sync::oneshot::channel();
    let owned_database = database.clone();
    let task = tokio::spawn(async move {
        let _lock = MaintenanceLock::acquire(&owned_database).unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    received.await.unwrap();
    assert!(MaintenanceLock::acquire(&database).is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let _reopened = MaintenanceLock::acquire(&database).unwrap();
    let after = std::fs::metadata(&lock_path).unwrap();
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
}

#[test]
fn lock_child() {
    let Some(database) = std::env::var_os("R092_LOCK_CHILD_DATABASE") else {
        return;
    };
    let _lock = MaintenanceLock::acquire(Path::new(&database)).unwrap();
    std::fs::write(std::env::var_os("R092_LOCK_CHILD_READY").unwrap(), b"ready").unwrap();
    if std::env::var("R092_LOCK_CHILD_EXIT").as_deref() == Ok("exit") {
        std::process::exit(86);
    }
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn process_exit_and_kill_release_without_unlink() {
    for mode in ["exit", "kill"] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let ready = root.path().join("ready");
        std::fs::write(&database, b"synthetic").unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "maintenance_lock::tests::lock_child"])
            .env("R092_LOCK_CHILD_DATABASE", &database)
            .env("R092_LOCK_CHILD_READY", &ready)
            .env("R092_LOCK_CHILD_EXIT", mode)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let lock_path = root.path().join(".state.db.offline-migration.lock");
        let before = std::fs::metadata(&lock_path).unwrap();
        if mode == "kill" {
            assert!(MaintenanceLock::acquire(&database).is_err());
            child.kill().await.unwrap();
        }
        let status = child.wait().await.unwrap();
        if mode == "exit" {
            assert_eq!(status.code(), Some(86));
        }
        let _reopened = MaintenanceLock::acquire(&database).unwrap();
        let after = std::fs::metadata(&lock_path).unwrap();
        assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    }
}
