//! The ACP transport: one child process, its pipes, and request correlation.
//!
//! # Single owner
//!
//! Starting a transport spawns exactly **one** task, which owns the child and all
//! three pipes for its whole life:
//!
//! ```text
//! loop {
//!     biased select {
//!         bytes from stdout           -> buffer them, yield complete frames, route each
//!         an outgoing request         -> write it to stdin
//!         a chunk from stderr         -> drain it (a full stderr pipe would deadlock us)
//!         the child exits             -> fail every pending request and stop
//!     }
//! }
//! ```
//!
//! Callers hold an [`AcpTransport`] handle and only ever send a request and await
//! their own reply channel. That is why the transport contains **no lock at all**
//! and why frames cannot interleave: stdin is owned by one task, so whole frames
//! are written in order by construction.
//!
//! `biased` puts **inbound bytes ahead of process exit**, so a reply that has
//! already arrived is never lost to a simultaneous exit — the same preference the
//! worker's race gives an already-available stage result.
//!
//! # Cancel safety
//!
//! `select!` drops the branches it did not take, so a read that owned a partial
//! frame would lose it. The partial line therefore lives in a [`FrameReader`]
//! owned by the loop, and the select only ever polls `fill_buf` (which consumes
//! nothing until the loop itself calls `consume`). A cancelled poll is harmless.
//!
//! # Deadlines and abandoned requests
//!
//! A caller applies `tokio::time::timeout` to its own reply channel. A per-request
//! guard sends an abandonment message when the caller times out or is cancelled,
//! so the loop removes the entry immediately. Request ids increase monotonically
//! and are never reused, so a late reply matches no pending entry and is dropped.
//!
//! # Exit
//!
//! A child that dies closes the stream, and the loop fails every pending request
//! before returning, so a caller never waits for a process that is already gone.
//! [`AcpTransport::shutdown`] closes stdin (the "no more requests" signal), waits
//! the configured deadline, then kills and reaps. Dropping the handle instead
//! closes the outgoing channel, which runs the same cleanup, and `kill_on_drop`
//! covers the case where the loop itself is gone.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::acp::error::{AcpError, Phase};
use crate::acp::framing::FrameReader;
use crate::acp::schema::{self, SessionNotification, SessionUpdate};
use crate::acp::{MAX_FRAME_BYTES, MAX_SESSION_ID_BYTES, bounded, framing, jsonrpc};
use crate::models::EndpointAddress;

/// How many outgoing requests may queue before a sender waits.
///
/// The adapter dispatches one task at a time, so this is a comfort margin rather
/// than a throughput knob; a bounded channel keeps a stalled writer visible
/// instead of growing memory.
const OUTGOING_CAPACITY: usize = 8;

/// How much stderr is drained per read.
const STDERR_CHUNK: usize = 4096;

/// What replaces the tail of over-long turn output.
const TRUNCATION_MARKER: &str = "…[truncated]";

/// The default ceiling on text accumulated for one prompt turn.
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// The default bound on a graceful shutdown.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

/// Bounds the transport enforces while it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportLimits {
    /// Largest accepted frame.
    pub max_frame_bytes: usize,
    /// Largest amount of streamed text retained for one turn.
    pub max_output_bytes: usize,
    /// How long a graceful shutdown waits before killing the child.
    pub shutdown_deadline: Duration,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            shutdown_deadline: DEFAULT_SHUTDOWN_DEADLINE,
        }
    }
}

/// A completed request: its JSON result, plus any text the turn streamed.
///
/// `streamed_text` is only ever non-empty for `session/prompt`, which is where the
/// baseline collects a turn's answer from `session/update` notifications.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletedRequest {
    /// The JSON-RPC `result` value.
    pub result: Value,
    /// Text accumulated for this request (empty for non-prompt requests).
    pub streamed_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnUpdate {
    AgentMessageChunk(String),
}

pub struct AcpTurnHandle {
    updates: mpsc::Receiver<TurnUpdate>,
    reply: oneshot::Receiver<Result<CompletedRequest, AcpError>>,
    guard: RequestGuard,
    deadline: Duration,
    phase: Phase,
}

impl AcpTurnHandle {
    pub async fn recv(&mut self) -> Option<TurnUpdate> {
        self.updates.recv().await
    }
    pub async fn finish(self) -> Result<CompletedRequest, AcpError> {
        let Self {
            updates: _,
            reply,
            guard,
            deadline,
            phase,
        } = self;
        match tokio::time::timeout(deadline, reply).await {
            Ok(Ok(outcome)) => {
                guard.disarm();
                outcome
            }
            Ok(Err(_)) => {
                guard.disarm();
                Err(AcpError::TransportClosed)
            }
            Err(_) => Err(AcpError::Timeout { phase }),
        }
    }
}

/// A live ACP child process speaking JSONL over stdio.
pub struct AcpTransport {
    outgoing: mpsc::Sender<Outgoing>,
    next_id: AtomicI64,
    alive: Arc<AtomicBool>,
    /// Taken by [`AcpTransport::shutdown`] so the handle is awaited exactly once.
    /// The lock is held only to take it out, never across an await.
    loop_handle: Mutex<Option<JoinHandle<Option<i32>>>>,
}

/// A message from a caller to the transport loop.
enum Outgoing {
    Request {
        id: i64,
        phase: Phase,
        method: String,
        params: Value,
        responder: oneshot::Sender<Result<CompletedRequest, AcpError>>,
        updates: Option<mpsc::Sender<TurnUpdate>>,
    },
    Shutdown,
    Notification {
        method: String,
        params: Value,
    },
    Cancel(i64),
}

struct RequestGuard {
    outgoing: mpsc::Sender<Outgoing>,
    id: i64,
    armed: bool,
}

impl RequestGuard {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.outgoing.try_send(Outgoing::Cancel(self.id));
        }
    }
}

/// State kept for one in-flight request.
struct Pending {
    responder: oneshot::Sender<Result<CompletedRequest, AcpError>>,
    /// The caller's phase, so a failure names the step the caller was in.
    phase: Phase,
    /// `Some(session_id)` for a prompt: the session whose chunks accumulate here.
    session: Option<String>,
    output: String,
    output_truncated: bool,
    updates: Option<mpsc::Sender<TurnUpdate>>,
}

impl AcpTransport {
    /// Spawn the backend described by `address`.
    ///
    /// `cwd` becomes the child's working directory; the ACP `session/new` request
    /// carries it too, and the caller passes the same value to both.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime: the transport loop is a
    /// `tokio::spawn`ed task (mirrors [`Worker::spawn`](crate::bus::Worker::spawn)).
    ///
    /// # Errors
    ///
    /// [`AcpError::UnsupportedAddress`] when the endpoint is not an ACP address, or
    /// [`AcpError::Spawn`] when the process cannot be started. The spawn error
    /// carries the OS failure only, never the program or its arguments.
    pub fn spawn(
        address: &EndpointAddress,
        cwd: Option<&Path>,
        limits: TransportLimits,
    ) -> Result<Self, AcpError> {
        let EndpointAddress::Acp { command, args } = address else {
            return Err(AcpError::UnsupportedAddress {
                phase: Phase::Spawn,
            });
        };
        let mut builder = Command::new(command);
        builder
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Even a leaked handle must not leave the child behind.
            .kill_on_drop(true);
        if let Some(cwd) = cwd {
            builder.current_dir(cwd);
        }
        let mut child = builder.spawn().map_err(|source| AcpError::Spawn {
            phase: Phase::Spawn,
            source,
        })?;

        let stdin = child.stdin.take().ok_or_else(|| missing_pipe("stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| missing_pipe("stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| missing_pipe("stderr"))?;

        let alive = Arc::new(AtomicBool::new(true));
        let (outgoing, incoming) = mpsc::channel(OUTGOING_CAPACITY);
        let loop_handle = tokio::spawn(run_loop(
            child,
            stdin,
            stdout,
            stderr,
            incoming,
            limits,
            Arc::clone(&alive),
        ));

        Ok(Self {
            outgoing,
            next_id: AtomicI64::new(0),
            alive,
            loop_handle: Mutex::new(Some(loop_handle)),
        })
    }

    /// Whether the child and its loop are still running.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    /// Send a request and await its reply, bounded by `deadline`.
    ///
    /// # Errors
    ///
    /// [`AcpError::Timeout`] when the deadline elapses (the request is abandoned;
    /// a late reply is dropped harmlessly), [`AcpError::TransportClosed`] when the
    /// transport is gone, or whatever the reply itself carries.
    pub async fn request(
        &self,
        phase: Phase,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<CompletedRequest, AcpError> {
        if !self.is_alive() {
            return Err(AcpError::TransportClosed);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (responder, reply) = oneshot::channel();
        self.outgoing
            .send(Outgoing::Request {
                id,
                phase,
                method: method.to_owned(),
                params,
                responder,
                updates: None,
            })
            .await
            .map_err(|_| AcpError::TransportClosed)?;

        let guard = RequestGuard {
            outgoing: self.outgoing.clone(),
            id,
            armed: true,
        };

        match tokio::time::timeout(deadline, reply).await {
            Ok(Ok(outcome)) => {
                guard.disarm();
                outcome
            }
            // The loop dropped the responder: the transport died meanwhile.
            Ok(Err(_)) => {
                guard.disarm();
                Err(AcpError::TransportClosed)
            }
            Err(_) => Err(AcpError::Timeout { phase }),
        }
    }

    pub async fn request_stream(
        &self,
        phase: Phase,
        method: &str,
        params: Value,
        deadline: Duration,
        capacity: usize,
    ) -> Result<AcpTurnHandle, AcpError> {
        if !self.is_alive() {
            return Err(AcpError::TransportClosed);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (responder, reply) = oneshot::channel();
        let (updates, receiver) = mpsc::channel(capacity);
        self.outgoing
            .send(Outgoing::Request {
                id,
                phase,
                method: method.to_owned(),
                params,
                responder,
                updates: Some(updates),
            })
            .await
            .map_err(|_| AcpError::TransportClosed)?;
        Ok(AcpTurnHandle {
            updates: receiver,
            reply,
            guard: RequestGuard {
                outgoing: self.outgoing.clone(),
                id,
                armed: true,
            },
            deadline,
            phase,
        })
    }

    /// Close stdin, wait for the child, kill it if it overstays, and reap it.
    ///
    /// Returns the child's exit code when the platform reports one. Awaiting this
    /// is what guarantees no zombie is left behind. Safe to call on a shared
    /// handle and safe to repeat: only the first call awaits the loop.
    pub async fn shutdown(&self) -> Option<i32> {
        self.shutdown_inner().await.1
    }

    pub async fn shutdown_reaped(&self) -> bool {
        self.shutdown_inner().await.0
    }

    async fn shutdown_inner(&self) -> (bool, Option<i32>) {
        let handle = self
            .loop_handle
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        let _ = self.outgoing.send(Outgoing::Shutdown).await;
        match handle {
            Some(handle) => match handle.await {
                Ok(code) => (true, code),
                Err(_) => (false, None),
            },
            None => (false, None),
        }
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), AcpError> {
        self.outgoing
            .send(Outgoing::Notification {
                method: method.to_owned(),
                params,
            })
            .await
            .map_err(|_| AcpError::TransportClosed)
    }
}

impl std::fmt::Debug for AcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The child's identity is not rendered: the program and its arguments come
        // from configuration and may carry credentials.
        f.debug_struct("AcpTransport")
            .field("alive", &self.is_alive())
            .finish()
    }
}

fn missing_pipe(pipe: &str) -> AcpError {
    AcpError::Spawn {
        phase: Phase::Spawn,
        source: std::io::Error::other(format!("the child's {pipe} pipe is unavailable")),
    }
}

/// The transport loop: the single owner of the child and its pipes.
async fn run_loop(
    mut child: Child,
    mut stdin: ChildStdin,
    stdout: ChildStdout,
    mut stderr: ChildStderr,
    mut outgoing: mpsc::Receiver<Outgoing>,
    limits: TransportLimits,
    alive: Arc<AtomicBool>,
) -> Option<i32> {
    let mut reader = BufReader::new(stdout);
    let mut frames = FrameReader::new(limits.max_frame_bytes);
    let mut pending: HashMap<i64, Pending> = HashMap::new();
    let mut stderr_buffer = vec![0u8; STDERR_CHUNK];

    'transport: loop {
        sweep_abandoned(&mut pending);
        tokio::select! {
            biased;
            read = reader.fill_buf() => match read {
                Ok([]) => {
                    match frames.finish() {
                        // A half frame is a framing failure, and saying so is the
                        // point of classifying it: the caller learns the stream was
                        // cut, not that the process merely stopped talking.
                        Err(source) => {
                            tracing::warn!(%source, "acp backend closed its output mid-frame");
                            fail_all(&mut pending, |_| frame_failure(&source));
                        }
                        Ok(()) => {
                            // The end of stdout usually means the child is exiting,
                            // so its status is often already reapable; reporting the
                            // code when it is avoids a vaguer message.
                            let known = child
                                .try_wait()
                                .ok()
                                .flatten()
                                .and_then(|status| status.code());
                            let detail = match known {
                                Some(code) => describe_exit(Some(code)),
                                None => "the backend closed its output stream".to_owned(),
                            };
                            fail_all(&mut pending, |phase| AcpError::Exited {
                                phase,
                                detail: detail.clone(),
                            });
                        }
                    }
                    break 'transport;
                }
                Ok(bytes) => {
                    let chunk = bytes.to_vec();
                    reader.consume(chunk.len());
                    frames.push(&chunk);
                    loop {
                        match frames.next_frame() {
                            Ok(Some(value)) => {
                                if let Err(failure) = route(value, &mut pending, &mut stdin, &limits).await {
                                    tracing::warn!(%failure, "acp transport stopping after a failed frame");
                                    fail_all(&mut pending, |_| AcpError::TransportClosed);
                                    break 'transport;
                                }
                            }
                            Ok(None) => break,
                            Err(source) => {
                                tracing::warn!(%source, "acp transport stopping on a framing error");
                                fail_all(&mut pending, |_| frame_failure(&source));
                                break 'transport;
                            }
                        }
                    }
                }
                Err(source) => {
                    tracing::warn!(%source, "acp transport stopping on a read error");
                    fail_all(&mut pending, |_| AcpError::TransportClosed);
                    break 'transport;
                }
            },
            message = outgoing.recv() => match message {
                Some(Outgoing::Request { id, phase, method, params, responder, updates }) => {
                    let session = prompt_session(&method, &params);
                    let envelope = jsonrpc::request(id, &method, params);
                    if let Err(source) = framing::write_frame(&mut stdin, &envelope).await {
                        let failure = AcpError::Frame { source };
                        tracing::warn!(%failure, "acp transport stopping on a write failure");
                        let _ = responder.send(Err(failure));
                        fail_all(&mut pending, |_| AcpError::TransportClosed);
                        break 'transport;
                    }
                    pending.insert(id, Pending {
                        responder,
                        phase,
                        session,
                        output: String::new(),
                        output_truncated: false,
                        updates,
                    });
                }
                Some(Outgoing::Cancel(id)) => {
                    if pending.remove(&id).is_some() {
                        tracing::debug!(id, "request abandoned by caller");
                    }
                }
                Some(Outgoing::Notification { method, params }) => {
                    let envelope = jsonrpc::notification(&method, params);
                    framing::write_frame(&mut stdin, &envelope).await.map_err(|source| AcpError::Frame { source }).unwrap_or_else(|failure| { tracing::warn!(%failure, "acp notification write failed"); });
                }
                // `Shutdown`, or every handle dropped.
                Some(Outgoing::Shutdown) | None => {
                    fail_all(&mut pending, |phase| AcpError::Exited {
                        phase,
                        detail: "the transport was shut down".to_owned(),
                    });
                    break 'transport;
                }
            },
            read = stderr.read(&mut stderr_buffer) => match read {
                // Drain stderr unconditionally: if it filled up, the child would
                // block writing to it and we would block reading stdout.
                Ok(0) | Err(_) => {}
                Ok(count) => {
                    tracing::debug!(bytes = count, "acp backend wrote to stderr");
                    // Backend-authored text may contain anything, so the excerpt is
                    // trace-only (off by default) and never stored.
                    tracing::trace!(
                        excerpt = %bounded(&String::from_utf8_lossy(&stderr_buffer[..count])),
                        "acp backend stderr"
                    );
                }
            },
            status = child.wait() => {
                let detail = match status {
                    Ok(status) => describe_exit(status.code()),
                    Err(_) => "unknown".to_owned(),
                };

                // Process exit can become observable before the pipe driver wakes
                // `fill_buf`. Drain the now-closed stdout so a final response is
                // still routed and a half-written frame is classified as truncated.
                let mut remainder = Vec::new();
                match reader.read_to_end(&mut remainder).await {
                    Ok(_) => {
                        frames.push(&remainder);
                        loop {
                            match frames.next_frame() {
                                Ok(Some(value)) => {
                                    if let Err(failure) =
                                        route(value, &mut pending, &mut stdin, &limits).await
                                    {
                                        tracing::warn!(%failure, "acp transport stopping after a failed final frame");
                                        fail_all(&mut pending, |_| AcpError::TransportClosed);
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    match frames.finish() {
                                        Err(source) => {
                                            tracing::warn!(%source, "acp backend exited mid-frame");
                                            fail_all(&mut pending, |_| frame_failure(&source));
                                        }
                                        Ok(()) => fail_all(&mut pending, |phase| AcpError::Exited {
                                            phase,
                                            detail: detail.clone(),
                                        }),
                                    }
                                    break;
                                }
                                Err(source) => {
                                    tracing::warn!(%source, "acp transport stopping on a final framing error");
                                    fail_all(&mut pending, |_| frame_failure(&source));
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => fail_all(&mut pending, |_| AcpError::TransportClosed),
                }
                break 'transport;
            }
        }
    }

    let code = terminate(&mut child, stdin, limits.shutdown_deadline).await;
    tracing::debug!(code, "acp transport stopped");
    alive.store(false, Ordering::Relaxed);
    code
}

/// Remove requests whose caller no longer owns the reply receiver.
///
/// This is the reliable backstop for cancellation: it runs in the single loop
/// that owns `pending`, so it does not depend on an available outgoing-channel
/// slot and does not require a per-request task or lock.
fn sweep_abandoned(pending: &mut HashMap<i64, Pending>) {
    pending.retain(|id, entry| {
        let keep = !entry.responder.is_closed();
        if !keep {
            tracing::debug!(id, "removed abandoned request");
        }
        keep
    });
}

/// Route one inbound frame.
async fn route(
    value: Value,
    pending: &mut HashMap<i64, Pending>,
    stdin: &mut ChildStdin,
    limits: &TransportLimits,
) -> Result<(), AcpError> {
    match jsonrpc::parse(&value)? {
        jsonrpc::JsonRpcMessage::Response { id, outcome } => {
            let Some(numeric) = id.as_number() else {
                tracing::debug!(%id, "ignoring a response with a non-numeric id");
                return Ok(());
            };
            let Some(entry) = pending.remove(&numeric) else {
                // Abandoned (its caller hit a deadline) or already answered.
                tracing::debug!(
                    id = numeric,
                    "ignoring a response that matches no pending request"
                );
                return Ok(());
            };
            let outcome = match outcome {
                Ok(result) => Ok(CompletedRequest {
                    result,
                    streamed_text: entry.output,
                }),
                Err(source) => Err(AcpError::Agent { source }),
            };
            // A dropped receiver means the caller gave up; nothing to do.
            let _ = entry.responder.send(outcome);
            Ok(())
        }
        jsonrpc::JsonRpcMessage::Notification(notification) => {
            if notification.method == schema::METHOD_SESSION_UPDATE {
                accumulate(&notification.params, pending, limits)?;
            }
            Ok(())
        }
        jsonrpc::JsonRpcMessage::Request(request) => {
            // The policy lives with the schema; the transport only answers.
            let response = schema::agent_request_response(
                request.id.clone(),
                &request.method,
                &request.params,
            );
            tracing::warn!(method = %request.method, "answered an agent-initiated request");
            framing::write_frame(stdin, &response)
                .await
                .map_err(|source| AcpError::Frame { source })
        }
    }
}

/// Add a streamed chunk to the matching in-flight prompt.
fn accumulate(
    params: &Value,
    pending: &mut HashMap<i64, Pending>,
    limits: &TransportLimits,
) -> Result<(), AcpError> {
    let Ok(notification) = serde_json::from_value::<SessionNotification>(params.clone()) else {
        tracing::debug!("ignoring a session/update that does not match the schema");
        return Ok(());
    };
    let SessionUpdate::AgentMessageChunk { content } = notification.update else {
        return Ok(());
    };
    let Some(text) = content.as_text() else {
        return Ok(());
    };
    let mut overflowed = Vec::new();
    for (id, entry) in pending.iter_mut() {
        if entry.session.as_deref() != Some(notification.session_id.as_str()) {
            continue;
        }
        if let Some(updates) = &entry.updates {
            if updates
                .try_send(TurnUpdate::AgentMessageChunk(text.to_owned()))
                .is_err()
            {
                overflowed.push(*id);
                continue;
            }
        }
        if entry.output.len() + text.len() <= limits.max_output_bytes {
            entry.output.push_str(text);
        } else if !entry.output_truncated {
            entry.output.push_str(TRUNCATION_MARKER);
            entry.output_truncated = true;
        }
    }
    for id in overflowed {
        if let Some(entry) = pending.remove(&id) {
            let _ = entry.responder.send(Err(AcpError::StreamBackpressure));
        }
    }
    Ok(())
}

/// The session id of a prompt request, when the request is one and it is usable.
fn prompt_session(method: &str, params: &Value) -> Option<String> {
    if method != schema::METHOD_SESSION_PROMPT {
        return None;
    }
    let session = params.get("sessionId")?.as_str()?;
    if session.is_empty() || session.len() > MAX_SESSION_ID_BYTES {
        return None;
    }
    Some(session.to_owned())
}

/// Complete every pending request with a failure built from its own phase.
fn fail_all(pending: &mut HashMap<i64, Pending>, failure: impl Fn(Phase) -> AcpError) {
    for (_, entry) in pending.drain() {
        let _ = entry.responder.send(Err(failure(entry.phase)));
    }
}

/// Close stdin, wait for the child, kill it if needed, and reap it.
async fn terminate(child: &mut Child, stdin: ChildStdin, deadline: Duration) -> Option<i32> {
    drop(stdin);
    match tokio::time::timeout(deadline, child.wait()).await {
        Ok(Ok(status)) => status.code(),
        // It did not exit in time (or waiting failed): kill and reap.
        _ => {
            let _ = child.start_kill();
            child.wait().await.ok().and_then(|status| status.code())
        }
    }
}

/// Rebuild a framing failure for the waiters that were not told directly.
///
/// [`FrameError::Io`] wraps an `std::io::Error`, which cannot be cloned, so that
/// one case degrades to "the transport is closed" — the caller learns the same
/// practical fact, and the original error is logged where it happened.
fn frame_failure(source: &crate::acp::framing::FrameError) -> AcpError {
    use crate::acp::framing::FrameError;
    match source {
        FrameError::TooLarge { limit } => AcpError::Frame {
            source: FrameError::TooLarge { limit: *limit },
        },
        FrameError::NotUtf8 => AcpError::Frame {
            source: FrameError::NotUtf8,
        },
        FrameError::Truncated => AcpError::Frame {
            source: FrameError::Truncated,
        },
        FrameError::Malformed { detail } => AcpError::Frame {
            source: FrameError::Malformed {
                detail: detail.clone(),
            },
        },
        FrameError::Io(_) => AcpError::TransportClosed,
    }
}

fn describe_exit(code: Option<i32>) -> String {
    match code {
        Some(code) => format!("status {code}"),
        None => "signal or unknown".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_removes_callers_that_dropped_their_reply_receiver() {
        let (sender, receiver) = oneshot::channel();
        let mut pending = HashMap::from([(
            7,
            Pending {
                responder: sender,
                phase: Phase::Prompt,
                session: Some("session-1".to_owned()),
                output: "stale".to_owned(),
                output_truncated: false,
                updates: None,
            },
        )]);
        drop(receiver);

        sweep_abandoned(&mut pending);

        assert!(pending.is_empty());
    }
}
