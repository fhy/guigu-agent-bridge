use super::tests::fixture;
use super::*;
use crate::owner_binding_remediation::close_fixture::Registration;

async fn delayed_checkpoint(point: Boundary) {
    let (root, database, retained, args) = fixture().await;
    let registration = Registration::new(&database);
    let gate = registration.gate.clone();
    let release = async {
        gate.wait_entered().await;
        // Longer than the unchanged 1s busy budget; no product timeout changes.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        gate.release();
    };
    let work = async {
        let mut boundary = |actual| {
            if actual == point {
                registration.gate.arm();
            }
            Ok(())
        };
        remediate_paths_with_boundaries(&database, &retained, &args, false, false, &mut boundary)
            .await
    };
    let (result, ()) = tokio::join!(work, release);
    if result != Ok(Outcome::Remediated) {
        let path = root.keep();
        panic!("checkpoint result {result:?}; retained fixture {path:?}");
    }
    assert_eq!(
        remediate_paths(&database, &retained, &args, false).await,
        Ok(Outcome::AlreadyRemediated)
    );
}

#[tokio::test]
async fn publication_close_checkpoint_finishes_before_fresh_reread() {
    delayed_checkpoint(Boundary::BeforePublicationCommit).await;
}

#[tokio::test]
async fn business_close_checkpoint_finishes_before_fresh_reread() {
    delayed_checkpoint(Boundary::BeforeBusinessCommit).await;
}

#[tokio::test]
async fn failed_connection_close_remains_ambiguous() {
    let (_root, database, _retained, _args) = fixture().await;
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(database)
            .optimize_on_close(true, None),
    )
    .await
    .unwrap();
    c.lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, || false);
    assert_eq!(
        close_before_reread(c, "test-close").await,
        Err(Failure::new("unknown-commit", "ambiguous"))
    );
}
