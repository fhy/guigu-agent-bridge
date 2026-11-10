use super::*;
use crate::bus::{AdmissionContext, BusError};
use crate::matrix::{MatrixAdmissionFuture, ReplyFuture};
use crate::models::AgentTask;
use std::sync::atomic::{AtomicUsize, Ordering};

struct NeverSend;
impl MatrixSender for NeverSend {
    fn send_reply<'a>(&'a self, _: &'a ReplyContext, _: &'a str) -> ReplyFuture<'a> {
        Box::pin(async { panic!("fixture must not send") })
    }
}
struct PanicAdmission;
impl DurableMatrixAdmission for PanicAdmission {
    fn winner<'a>(
        &'a self,
        _: &'a str,
    ) -> MatrixAdmissionFuture<'a, Result<Option<AgentTask>, BusError>> {
        Box::pin(async { panic!("synthetic ingress persistence panic") })
    }
    fn admit<'a>(
        &'a self,
        _: AgentTask,
        _: AdmissionContext,
    ) -> MatrixAdmissionFuture<'a, Result<AgentTask, BusError>> {
        Box::pin(async { panic!("synthetic ingress admission panic") })
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    MatrixIngress,
    mpsc::Sender<InboundMatrixEvent>,
) {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let config = crate::config::load_from_str("").unwrap();
    let registry = Arc::new(EndpointRegistry::from_config(&config));
    let reload = Arc::new(
        ReloadController::new(
            config,
            Arc::new(crate::app::FileConfigSource::new(
                root.path().join("config.toml"),
            )),
        )
        .unwrap(),
    );
    let (tx, rx) = mpsc::channel(1);
    let ingress = MatrixIngress::new(
        rx,
        Arc::new(crate::storage::SqliteRepository::new(pool)),
        registry,
        Arc::new(PanicAdmission),
        Arc::new(NeverSend),
        reload,
        Cancellation::new(),
        Arc::new(CommandLedger::new(4)),
        Arc::new(ReplyRegistry::default()),
        4,
    );
    (root, ingress, tx)
}

#[tokio::test]
async fn actual_ingress_unpolled_abort_and_channel_exit_publish_liveness_once() {
    for abort in [true, false] {
        let (_root, ingress, tx) = fixture().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let handle = ingress.start_supervised(move |class| {
            assert_eq!(class, "unexpected-exit");
            observed.fetch_add(1, Ordering::SeqCst);
        });
        if abort {
            handle.join.abort();
        } else {
            drop(tx);
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), handle.join)
            .await
            .unwrap();
        assert_eq!(result.is_err(), abort);
        assert!(!handle.alive.load(Ordering::Acquire));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
