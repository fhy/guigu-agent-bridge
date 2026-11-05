use super::*;
use crate::matrix::{MemorySyncTokenStore, SyncTokenStore};
use tracing::instrument::WithSubscriber;

#[tokio::test]
async fn persisted_checkpoint_gap_does_not_advance_watermark_or_deliver_events() {
    let tokens = Arc::new(MemorySyncTokenStore::default());
    tokens.save("durable-before").await.unwrap();
    let sync = MatrixSync::new(MatrixClient::for_test().await, tokens.clone(), 2).unwrap();
    let mut response = SyncResponse {
        next_batch: "untrusted-after".into(),
        ..Default::default()
    };
    let mut room = matrix_sdk::sync::JoinedRoomUpdate::default();
    room.timeline.limited = true;
    response
        .rooms
        .joined
        .insert("!room:example.test".parse().unwrap(), room);
    let (sender, mut receiver) = mpsc::channel(2);
    let captured = crate::test_diagnostics::Capture::default();
    assert!(matches!(
        sync.process_batch(response, Some("durable-before"), &sender, &mut None)
            .with_subscriber(captured.subscriber())
            .await
            .unwrap_err(),
        MatrixError::CutoverRefused
    ));
    assert_eq!(
        tokens.load().await.unwrap().as_deref(),
        Some("durable-before")
    );
    assert!(receiver.try_recv().is_err());
    let output = captured.output();
    assert!(output.contains("ERROR"));
    assert!(output.contains("checkpoint-gap"));
    assert!(output.contains("checkpoint_present=true"));
    assert!(!output.contains("durable-before"));
    assert!(!output.contains("untrusted-after"));
}
