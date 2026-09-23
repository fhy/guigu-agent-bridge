//! T014 integration tests: real child processes over real pipes.
//!
//! Every case spawns `examples/acp-mock-backend` and drives the adapter through the
//! public API, so the real process, the real stdio pipes and the real JSONL codec
//! are all exercised. No `sleep` is used to make progress: waits are ended by the
//! adapter's own deadlines, and a mock that hangs is a mock whose deadline must
//! fire.
//!
//! Each test is self-contained, and every child is shut down (or reaped) before the
//! test ends, so no process survives a test.

use std::path::{Path, PathBuf};
use std::time::Duration;

use guigu_agent_bridge::acp::TurnUpdate;
use guigu_agent_bridge::acp::error::{AcpError, Phase};
use guigu_agent_bridge::acp::framing::FrameError;
use guigu_agent_bridge::acp::{AcpClient, AcpLimits, AcpTransport, TransportLimits};
use guigu_agent_bridge::models::{EndpointAddress, TransportType};

const AGENT_ID: &str = "worker";

/// The path of the mock backend, rebuilt once per test process so it is current.
///
/// `cargo test` builds `examples/`, but a filtered run (`cargo test --test ...`)
/// does not, which would silently spawn a stale fixture. Building once up front is
/// cheap when everything is up to date and keeps the suite honest either way.
fn mock_backend() -> PathBuf {
    static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let cargo = option_env!("CARGO").unwrap_or("cargo");
            let status = std::process::Command::new(cargo)
                .args(["build", "--example", "acp-mock-backend"])
                .status()
                .expect("run cargo build --example");
            assert!(status.success(), "building the mock backend failed");

            let current = std::env::current_exe().expect("current test binary");
            let mut path = current
                .parent()
                .expect("the test binary has a directory")
                .to_path_buf();
            path.pop();
            path.push("examples");
            path.push(format!("acp-mock-backend{}", std::env::consts::EXE_SUFFIX));
            assert!(
                path.exists(),
                "the mock backend is missing at {}",
                path.display()
            );
            path
        })
        .clone()
}

/// Wait, without sleeping, until a transport has noticed its child is gone.
async fn wait_until_dead(client: &AcpClient) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_alive() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the transport must notice the child's exit");
}

fn address(scenario: &str) -> EndpointAddress {
    EndpointAddress::Acp {
        command: mock_backend().to_string_lossy().into_owned(),
        args: vec![scenario.to_owned()],
    }
}

fn limits() -> AcpLimits {
    AcpLimits {
        initialize_deadline: Duration::from_secs(5),
        request_deadline: Duration::from_secs(5),
        transport: TransportLimits::default(),
    }
}

/// Limits with a very short deadline, so a hanging mock is bounded quickly.
fn impatient() -> AcpLimits {
    AcpLimits {
        initialize_deadline: Duration::from_millis(150),
        request_deadline: Duration::from_millis(150),
        transport: TransportLimits::default(),
    }
}

async fn connect(scenario: &str) -> AcpClient {
    AcpClient::connect(&address(scenario), AGENT_ID, None, limits())
        .await
        .expect("the mock backend should connect")
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_happy_backend_negotiates_and_reports_its_identity() {
    let client = connect("happy").await;

    assert_eq!(
        client.backend_id(),
        "worker/acp-mock-backend@0.1.0",
        "ADR-001: the compatibility identity carries the backend's own version"
    );
    assert!(client.is_alive());

    let session = client.new_session("/tmp").await.expect("session");
    assert_eq!(session, "session-1");

    let turn = client.prompt(&session, "do the thing").await.expect("turn");
    assert!(turn.stop_reason.is_end_turn());
    assert_eq!(
        turn.text, "Hello, world",
        "the baseline collects streamed chunks into the turn's output"
    );

    assert_eq!(client.shutdown().await, Some(0));
    assert_eq!(
        client.shutdown().await,
        None,
        "shutting down twice is harmless"
    );
    assert!(!client.is_alive());
}

#[tokio::test]
async fn prompt_stream_exposes_ordered_bounded_chunks_before_the_terminal_result() {
    let client = connect("happy").await;
    let session = client.new_session("/tmp").await.expect("session");
    let mut turn = client
        .prompt_stream(&session, "do it", 3)
        .await
        .expect("stream");
    assert_eq!(
        turn.recv().await,
        Some(TurnUpdate::AgentMessageChunk("Hello".into()))
    );
    assert_eq!(
        turn.recv().await,
        Some(TurnUpdate::AgentMessageChunk(", ".into()))
    );
    assert_eq!(
        turn.recv().await,
        Some(TurnUpdate::AgentMessageChunk("world".into()))
    );
    let completed = turn.finish().await.expect("terminal result");
    assert_eq!(completed.streamed_text, "Hello, world");
    client.shutdown().await;
}

#[tokio::test]
async fn protocol_cancel_reaps_the_live_backend() {
    let client = connect("happy").await;
    let session = client.new_session("/tmp").await.expect("session");
    client.cancel_and_shutdown(&session).await.expect("cancel");
    assert!(!client.is_alive());
}

#[tokio::test]
async fn an_undrained_turn_stream_reports_backpressure_without_dropping_terminal_delivery() {
    let client = connect("happy").await;
    let session = client.new_session("/tmp").await.expect("session");
    let turn = client
        .prompt_stream(&session, "do it", 1)
        .await
        .expect("stream");
    assert!(matches!(
        turn.finish().await,
        Err(AcpError::StreamBackpressure)
    ));
    assert!(client.is_alive());
    client.shutdown().await;
}

#[tokio::test]
async fn a_backend_on_another_protocol_version_is_refused() {
    let error = AcpClient::connect(&address("bad-protocol-version"), AGENT_ID, None, limits())
        .await
        .expect_err("the version must be refused");

    match error {
        AcpError::UnsupportedProtocol { got, supported } => {
            assert_eq!(got, 99);
            assert_eq!(supported, 1);
        }
        other => panic!("expected an unsupported-version failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_backend_that_requires_api_key_authenticates_before_session() {
    let client = AcpClient::connect(&address("need-auth"), AGENT_ID, None, limits())
        .await
        .expect("the API-key method should be selected and authenticated");
    let session = client
        .new_session("/tmp")
        .await
        .expect("session after auth");
    let turn = client
        .prompt(&session, "one prompt")
        .await
        .expect("prompt after auth");
    assert!(turn.stop_reason.is_end_turn());
    client.shutdown().await;
}

#[tokio::test]
async fn a_spawn_failure_never_renders_the_command_line() {
    let secret = "--token=SECRET-DO-NOT-LEAK";
    let address = EndpointAddress::Acp {
        command: "definitely-not-a-real-acp-backend".to_owned(),
        args: vec![secret.to_owned()],
    };

    let error = AcpTransport::spawn(&address, None, TransportLimits::default())
        .expect_err("spawning must fail");
    let rendered = error.to_string();

    assert!(matches!(error, AcpError::Spawn { .. }));
    assert!(
        !rendered.contains(secret),
        "the arguments must never be rendered: {rendered}"
    );
    assert!(
        !rendered.contains("definitely-not-a-real-acp-backend"),
        "the program must never be rendered: {rendered}"
    );
}

#[tokio::test]
async fn a_non_acp_address_is_rejected() {
    let address = EndpointAddress::Matrix {
        user_id: "@someone:example.org".to_owned(),
    };
    assert!(matches!(
        AcpTransport::spawn(&address, None, TransportLimits::default()),
        Err(AcpError::UnsupportedAddress { .. })
    ));
    let _ = TransportType::Matrix;
}

#[tokio::test]
async fn the_initialize_deadline_bounds_a_silent_backend() {
    let error = AcpClient::connect(&address("hang"), AGENT_ID, None, impatient())
        .await
        .expect_err("the handshake must not wait forever");

    match error {
        AcpError::Timeout { phase } => assert_eq!(phase, Phase::Initialize),
        other => panic!("expected an initialize timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn the_prompt_deadline_bounds_a_silent_turn() {
    let client = AcpClient::connect(&address("hang-prompt"), AGENT_ID, None, impatient())
        .await
        .expect("the handshake itself answers");
    let session = client.new_session("/tmp").await.expect("session");

    let error = client
        .prompt(&session, "do the thing")
        .await
        .expect_err("the turn must not wait forever");
    match error {
        AcpError::Timeout { phase } => assert_eq!(phase, Phase::Prompt),
        other => panic!("expected a prompt timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn repeated_prompt_timeouts_abandon_pending_requests_on_a_live_transport() {
    let client = AcpClient::connect(&address("hang-prompt"), AGENT_ID, None, impatient())
        .await
        .expect("the handshake itself answers");
    let session = client.new_session("/tmp").await.expect("session");

    for _ in 0..10 {
        assert!(matches!(
            client.prompt(&session, "bounded").await,
            Err(AcpError::Timeout {
                phase: Phase::Prompt
            })
        ));
    }

    assert!(client.is_alive(), "timeouts must not stop the transport");
    client.shutdown().await;
}

// ---------------------------------------------------------------------------
// Process exit and framing failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_exit_before_the_handshake_is_reported_as_an_exit() {
    // The child dies before answering. Which side of the race is observed first
    // (the end of stdout, or the exit status itself) is not fixed, so this asserts
    // the classification and the phase; the status is asserted deterministically in
    // the reaping test below.
    let error = AcpClient::connect(&address("exit-early"), AGENT_ID, None, limits())
        .await
        .expect_err("the backend died");

    match error {
        AcpError::Exited { phase, detail } => {
            assert_eq!(phase, Phase::Initialize);
            assert!(!detail.is_empty());
        }
        AcpError::Frame {
            source: FrameError::Io(_),
        } => {}
        other => panic!("expected an exit or broken-pipe failure, got {other:?}"),
    }
}

#[tokio::test]
async fn an_exit_after_the_handshake_is_reaped_with_its_status() {
    let client = connect("exit-after-init").await;
    wait_until_dead(&client).await;

    // A request on a transport whose loop has ended is refused without waiting.
    assert!(matches!(
        client.new_session("/tmp").await,
        Err(AcpError::TransportClosed)
    ));
    assert!(
        !client.is_alive(),
        "a dead transport reports itself as dead"
    );
    assert_eq!(
        client.shutdown().await,
        Some(3),
        "the child's exit code is propagated when it is reaped"
    );
}

#[tokio::test]
async fn a_frame_cut_in_half_by_exit_is_reported_as_truncated() {
    let client = connect("truncated-eof").await;

    let error = client
        .new_session("/tmp")
        .await
        .expect_err("the frame was cut in half");
    assert!(
        matches!(
            error,
            AcpError::Frame {
                source: FrameError::Truncated
            }
        ),
        "expected a truncated frame, got {error:?}"
    );
}

#[tokio::test]
async fn an_oversized_frame_is_reported_as_too_large() {
    let limits = AcpLimits {
        initialize_deadline: Duration::from_secs(5),
        request_deadline: Duration::from_secs(5),
        transport: TransportLimits {
            max_frame_bytes: 512,
            ..TransportLimits::default()
        },
    };
    let client = AcpClient::connect(&address("oversize-frame"), AGENT_ID, None, limits)
        .await
        .expect("the handshake fits in the limit");

    let error = client
        .new_session("/tmp")
        .await
        .expect_err("the answer exceeds the limit");
    assert!(
        matches!(
            error,
            AcpError::Frame {
                source: FrameError::TooLarge { limit: 512 }
            }
        ),
        "expected a too-large frame, got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Codec shapes over a real pipe
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coalesced_frames_in_one_write_are_all_handled() {
    let client = connect("coalesced").await;
    let session = client.new_session("/tmp").await.expect("session");

    let turn = client
        .prompt(&session, "do the thing")
        .await
        .expect("the turn must complete even when every frame arrives in one write");
    assert!(turn.stop_reason.is_end_turn());
    assert_eq!(turn.text, "Hello, world");
}

#[tokio::test]
async fn blank_lines_between_frames_are_tolerated() {
    let client = connect("blank-lines").await;
    let session = client.new_session("/tmp").await.expect("session");

    let turn = client.prompt(&session, "do the thing").await.expect("turn");
    assert_eq!(turn.text, "Hello, world");
}

#[tokio::test]
async fn a_flooding_stderr_pipe_does_not_stall_the_turn() {
    // If the transport did not drain stderr the child would block writing to it,
    // and this turn would never finish (the deadline would fire instead).
    let client = connect("noisy-stderr").await;
    let session = client.new_session("/tmp").await.expect("session");

    let turn = client
        .prompt(&session, "do the thing")
        .await
        .expect("the turn must complete while stderr floods");
    assert!(turn.stop_reason.is_end_turn());
    assert_eq!(turn.text, "Hello, world");
}

// ---------------------------------------------------------------------------
// Agent-initiated requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_permission_request_is_denied_so_the_turn_is_refused() {
    // The mock offers `allow_once` and `reject_once` and ends the turn with
    // `end_turn` only when the allow option was selected. A refusal therefore proves
    // the adapter denied the request instead of granting it.
    let client = connect("permission").await;
    let session = client.new_session("/tmp").await.expect("session");

    let turn = client.prompt(&session, "do the thing").await.expect("turn");
    assert_eq!(
        turn.stop_reason,
        guigu_agent_bridge::acp::schema::StopReason::Refusal,
        "the adapter must deny permissions until T011 decides policy"
    );
}

/// A backend that answers the handshake and then a prompt: used to prove that the
/// prompt path itself is what a later stage extends.
#[tokio::test]
async fn a_session_can_be_resumed_with_the_same_identity() {
    let client = connect("resume").await;
    let session = client.new_session("/tmp").await.expect("session");
    let resumed = client
        .resume_session(&session, "/tmp")
        .await
        .expect("resume");
    assert_eq!(resumed, session, "resume must not move the session");
}

/// Nothing about a path used by a test may leak into an error.
#[tokio::test]
async fn a_failure_reason_never_carries_the_working_directory() {
    let secret = "/tmp/SECRET-WORKDIR-DO-NOT-LEAK";
    let client = connect("truncated-eof").await;
    let error = client
        .new_session(secret)
        .await
        .expect_err("the frame is cut in half");
    assert!(
        !error.to_string().contains("SECRET-WORKDIR"),
        "got: {error}"
    );
}

#[test]
fn the_mock_backend_binary_exists() {
    // Keeps the fixture honest: a missing example would otherwise show up only as a
    // confusing failure in the async tests.
    assert!(mock_backend().exists());
    let _ = Path::new(".");
}
