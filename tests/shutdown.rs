use std::future;
use std::io;

use guigu_agent_bridge::Error;

#[tokio::test]
async fn entry_returns_ok_on_immediate_shutdown() {
    let result = guigu_agent_bridge::run_with_shutdown(future::ready(Ok(()))).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn entry_propagates_shutdown_error() {
    let inner = io::Error::new(io::ErrorKind::Other, "signal failure");
    let result =
        guigu_agent_bridge::run_with_shutdown(future::ready(Err::<(), io::Error>(inner))).await;

    match result {
        Err(Error::Shutdown(source)) => assert_eq!(source.to_string(), "signal failure"),
        other => panic!("expected Err(Error::Shutdown), got {other:?}"),
    }
}
