//! A scriptable mock [`TaskDispatcher`].
//!
//! [`MockAgent`] stands in for an external agent: it models the two-stage
//! dispatch contract ([`deliver`](TaskDispatcher::deliver) takes an explicit
//! acceptance decision, [`execute`](TaskDispatcher::execute) returns one terminal
//! outcome) from an in-memory script, and records every call so a test can assert
//! what the worker actually asked for.
//!
//! # Mock only — not a production adapter
//!
//! This is production *code* (so consumers outside the crate can assemble it), but
//! it is a test double / reference implementation / acceptance placeholder. It
//! performs **no real I/O** — no subprocess, no network, no filesystem, no clock,
//! no `tokio::time` wait — it writes no events, never touches the worker's state
//! machine, and never records `seq`. See [`crate::agents`] for the red lines and
//! the assembly guidance (which transport to key it on).
//!
//! # Behaviour: one default reply plus three override kinds
//!
//! ```text
//! on_task(task_id)  >  on_text(text)  >  on_attempt(k)  >  default reply
//! ```
//!
//! A rule matches *the call*, and the same rule answers both stages: matching a
//! `deliver` uses [`MockReply::deliver`], matching an `execute` uses
//! [`MockReply::execute`]. **The precedence above is fixed and independent of
//! registration order** — identity is more specific than content, content more
//! specific than position — so a test's outcome never depends on the order its
//! builder calls happened to be written in. Within one kind, the first registered
//! rule wins.
//!
//! `on_attempt(k)` means "the k-th call to *this stage*", which is exactly
//! [`DispatchRequest::attempt`]: the worker calls `deliver` once per attempt and
//! `execute` at most once per attempt, so attempt `k` is the k-th call of either
//! stage. (That equivalence is a dependency on the worker's frozen attempt loop;
//! see [`crate::bus::worker`].)
//!
//! ## Unmatched calls are never silent
//!
//! A call that matches no rule gets the **default reply** — accept the delivery
//! and complete with [`DEFAULT_OUTPUT`] (Q1 = A). That keeps the common
//! end-to-end path zero-configuration, and it stays honest because every recorded
//! call carries the [`ReplySource`] that produced its reply: a test can assert
//! [`ReplySource::Default`] and see that the call was *not* scripted.
//!
//! # Observation
//!
//! Calls are appended to a single [`std::sync::Mutex`]`<Vec<`[`MockCall`]`>>`:
//!
//! - the returned futures contain **no `.await`**, so there is no await point at
//!   which a lock could be held; the guard lives only inside one synchronous
//!   statement;
//! - there is exactly one lock, it is never nested, and its critical section is a
//!   single `push` with no user code — so it cannot deadlock, and it cannot stall
//!   the worker's consumption loop;
//! - [`MockAgent::calls`] and friends return an owned **snapshot**; the guard is
//!   never handed to a caller;
//! - a call is recorded when the returned future is first polled. The worker polls
//!   the stage ahead of the cancellation and deadline arms of its `biased`
//!   `select!`, so every attempt it dispatches is observed;
//! - a poisoned lock recovers (`into_inner`) instead of panicking, because the
//!   trait contract is "return `Err`, never panic".
//!
//! An `mpsc` observation channel was rejected: a full channel would either drop
//! evidence (`try_send`) or block the worker inside `send().await` — and the
//! latter would drag the test's own consumption schedule into the path under test.
//!
//! # Sharing
//!
//! [`MockAgent`] is deliberately **not `Clone`**: a clone would split the recorded
//! calls (or need a second layer of sharing), turning "my assertion saw nothing"
//! into a silent failure. Hold one `Arc<MockAgent>` and clone that.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use crate::bus::{BusFuture, DispatchError, DispatchOutcome, DispatchRequest, TaskDispatcher};
use crate::models::{DeliveryId, TaskId};

/// The output of a delivery that matched no rule, and therefore used the default
/// reply.
///
/// A named constant rather than a literal, so the default is visible at the
/// call site of a test and cannot drift unnoticed.
pub const DEFAULT_OUTPUT: &str = "mock: completed";

/// Which dispatch stage a recorded call belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockStage {
    /// [`TaskDispatcher::deliver`] — the acceptance decision.
    Deliver,
    /// [`TaskDispatcher::execute`] — the terminal outcome.
    Execute,
}

/// The acceptance decision [`MockAgent`] returns from `deliver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliverBehavior {
    /// Accept: `Ok(())`. The worker then writes `Running` and calls `execute`.
    Accept,
    /// Refuse: `Err(`[`DispatchError::NotAccepted`]`)`. The task never becomes
    /// `Running` and `execute` is never called for that attempt.
    Refuse {
        /// Reason carried into the task's `Failed` event. Must not contain
        /// credentials.
        reason: String,
    },
}

/// The terminal result [`MockAgent`] returns from `execute`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteBehavior {
    /// `Ok(`[`DispatchOutcome::Completed`]`)`.
    Complete {
        /// Output recorded verbatim in the `Completed` event.
        output: String,
    },
    /// `Ok(`[`DispatchOutcome::Failed`]`)` — a *deterministic* failure verdict.
    /// The worker records `Failed(seq = 4)` and never retries it.
    ReportFailure {
        /// Error recorded verbatim in the `Failed` event.
        error: String,
    },
    /// `Err(`[`DispatchError::ExecutionFailed`]`)` — no terminal result was
    /// produced. The worker records `Failed(seq = 4)` and may retry it (T006).
    Fail {
        /// Reason carried into the task's `Failed` event. Must not contain
        /// credentials.
        reason: String,
    },
}

/// The two-stage reply for one scripted attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockReply {
    /// What `deliver` returns when this reply is selected.
    pub deliver: DeliverBehavior,
    /// What `execute` returns when this reply is selected.
    pub execute: ExecuteBehavior,
}

impl MockReply {
    /// Accept the delivery, then complete with `output`.
    pub fn completed(output: impl Into<String>) -> Self {
        Self {
            deliver: DeliverBehavior::Accept,
            execute: ExecuteBehavior::Complete {
                output: output.into(),
            },
        }
    }

    /// Refuse the delivery with `reason`.
    ///
    /// `execute` is unreachable for a refused delivery — the worker writes
    /// `Failed(seq = 3)` and never calls it — so the `execute` half is left at
    /// "complete with [`DEFAULT_OUTPUT`]". It is inert, not a hidden behaviour.
    pub fn refused(reason: impl Into<String>) -> Self {
        Self {
            deliver: DeliverBehavior::Refuse {
                reason: reason.into(),
            },
            execute: ExecuteBehavior::Complete {
                output: DEFAULT_OUTPUT.into(),
            },
        }
    }

    /// Accept the delivery, then report a deterministic terminal failure.
    pub fn reporting_failure(error: impl Into<String>) -> Self {
        Self {
            deliver: DeliverBehavior::Accept,
            execute: ExecuteBehavior::ReportFailure {
                error: error.into(),
            },
        }
    }

    /// Accept the delivery, then fail execution without a terminal result.
    pub fn failing_execution(reason: impl Into<String>) -> Self {
        Self {
            deliver: DeliverBehavior::Accept,
            execute: ExecuteBehavior::Fail {
                reason: reason.into(),
            },
        }
    }
}

/// Which rule kind answered a call, or that the default did.
///
/// Recorded on every [`MockCall`], so "the script did not match" is an assertable
/// fact instead of an inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplySource {
    /// No rule matched; the default reply was used.
    Default,
    /// A [`MockAgent::on_task`] rule matched.
    TaskId,
    /// A [`MockAgent::on_text`] rule matched.
    Text,
    /// A [`MockAgent::on_attempt`] rule matched.
    Attempt,
}

/// Which calls a scripted reply applies to.
///
/// The variants mirror the three rule kinds documented on the module — and the
/// three [`ReplySource`] values a call can report — so a script can be written as
/// data when that reads better than the typed builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockRule {
    /// Calls for exactly this task id (highest precedence).
    TaskId(TaskId),
    /// Calls whose task text is exactly this string.
    Text(String),
    /// The n-th call to either stage, i.e. attempt `n` (lowest precedence).
    Attempt(u32),
}

/// One recorded `deliver`/`execute` call.
///
/// Owned values only: the borrowed [`DispatchRequest`] is flattened, so a
/// snapshot stays assertable after the call returned and the request is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockCall {
    /// Which stage was called.
    pub stage: MockStage,
    /// The task being dispatched.
    pub task_id: TaskId,
    /// The attempt identity this stage was called with. `deliver` and `execute`
    /// of one attempt carry the **same** value (the T005 contract).
    pub delivery_id: DeliveryId,
    /// The attempt number (`1` = first dispatch; `> 1` = a T006 retry).
    pub attempt: u32,
    /// The dispatched task's text, as the dispatcher received it.
    pub text: String,
    /// Which rule (or the default) produced the reply.
    pub source: ReplySource,
}

/// A scriptable, deterministic [`TaskDispatcher`] for tests and acceptance runs.
///
/// See the module docs for the behaviour model, the observation contract and why
/// this is not a production adapter.
pub struct MockAgent {
    /// Reply used when no rule matches (see the module docs).
    default: MockReply,
    /// Rules keyed by exact task id; first match wins.
    by_task: Vec<(TaskId, MockReply)>,
    /// Rules keyed by exact task text; first match wins.
    by_text: Vec<(String, MockReply)>,
    /// Rules keyed by attempt number (= the stage's call index); first match wins.
    by_attempt: Vec<(u32, MockReply)>,
    /// Every `deliver`/`execute` call in call order.
    calls: Mutex<Vec<MockCall>>,
}

impl MockAgent {
    /// A mock that accepts every delivery and completes with [`DEFAULT_OUTPUT`].
    ///
    /// This is the unmatched-call default; use the `on_*` methods to script
    /// specific calls, or [`MockAgent::with_default`] to change it.
    pub fn new() -> Self {
        Self {
            default: MockReply::completed(DEFAULT_OUTPUT),
            by_task: Vec::new(),
            by_text: Vec::new(),
            by_attempt: Vec::new(),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Replace the reply used for calls that match no rule.
    pub fn with_default(mut self, reply: MockReply) -> Self {
        self.default = reply;
        self
    }

    /// Answer calls for exactly `task_id` with `reply`.
    ///
    /// Highest precedence: an explicit identity beats content and position.
    pub fn on_task(mut self, task_id: TaskId, reply: MockReply) -> Self {
        self.by_task.push((task_id, reply));
        self
    }

    /// Answer calls whose task text is exactly `text` with `reply`.
    ///
    /// Outranks [`MockAgent::on_attempt`], loses to [`MockAgent::on_task`].
    pub fn on_text(mut self, text: impl Into<String>, reply: MockReply) -> Self {
        self.by_text.push((text.into(), reply));
        self
    }

    /// Answer the `attempt`-th call to either stage with `reply`.
    ///
    /// Lowest precedence of the three kinds. `attempt` is the worker's attempt
    /// number, which equals the call index of that stage (see the module docs).
    pub fn on_attempt(mut self, attempt: u32, reply: MockReply) -> Self {
        self.by_attempt.push((attempt, reply));
        self
    }

    /// Script `rule` as data, dispatching to [`Self::on_task`], [`Self::on_text`]
    /// or [`Self::on_attempt`].
    pub fn on(self, rule: MockRule, reply: MockReply) -> Self {
        match rule {
            MockRule::TaskId(task_id) => self.on_task(task_id, reply),
            MockRule::Text(text) => self.on_text(text, reply),
            MockRule::Attempt(attempt) => self.on_attempt(attempt, reply),
        }
    }

    /// A mock whose default reply is "accept, then complete with `output`".
    pub fn completing(output: impl Into<String>) -> Self {
        Self::new().with_default(MockReply::completed(output))
    }

    /// A mock whose default reply is "refuse every delivery with `reason`".
    pub fn refusing(reason: impl Into<String>) -> Self {
        Self::new().with_default(MockReply::refused(reason))
    }

    /// A mock whose default reply is "accept, then report `error` as a terminal
    /// failure".
    pub fn reporting_failure(error: impl Into<String>) -> Self {
        Self::new().with_default(MockReply::reporting_failure(error))
    }

    /// A mock whose default reply is "accept, then fail execution with `reason`".
    pub fn failing_execution(reason: impl Into<String>) -> Self {
        Self::new().with_default(MockReply::failing_execution(reason))
    }

    /// Every recorded call, in call order.
    pub fn calls(&self) -> Vec<MockCall> {
        self.calls_guard().clone()
    }

    /// The recorded `deliver` calls, in call order.
    pub fn deliver_calls(&self) -> Vec<MockCall> {
        self.stage_calls(MockStage::Deliver)
    }

    /// The recorded `execute` calls, in call order.
    pub fn execute_calls(&self) -> Vec<MockCall> {
        self.stage_calls(MockStage::Execute)
    }

    /// How many calls have been recorded.
    pub fn call_count(&self) -> usize {
        self.calls_guard().len()
    }

    fn stage_calls(&self, stage: MockStage) -> Vec<MockCall> {
        self.calls()
            .into_iter()
            .filter(|call| call.stage == stage)
            .collect()
    }

    /// Select the reply for a call and report where it came from.
    ///
    /// Reads only immutable fields, so it needs no lock. The precedence is fixed
    /// (see the module docs): the three kinds are tried in order, never by
    /// registration order.
    fn select(&self, request: &DispatchRequest<'_>) -> (MockReply, ReplySource) {
        if let Some((_, reply)) = self
            .by_task
            .iter()
            .find(|(task_id, _)| *task_id == request.task.task_id)
        {
            return (reply.clone(), ReplySource::TaskId);
        }
        if let Some((_, reply)) = self
            .by_text
            .iter()
            .find(|(text, _)| *text == request.task.text)
        {
            return (reply.clone(), ReplySource::Text);
        }
        if let Some((_, reply)) = self
            .by_attempt
            .iter()
            .find(|(attempt, _)| *attempt == request.attempt)
        {
            return (reply.clone(), ReplySource::Attempt);
        }
        (self.default.clone(), ReplySource::Default)
    }

    /// Append one call to the observation table.
    ///
    /// The guard is confined to this synchronous method: no caller can hold it,
    /// and no `.await` can run while it is alive.
    fn record(&self, stage: MockStage, request: &DispatchRequest<'_>, source: ReplySource) {
        self.calls_guard().push(MockCall {
            stage,
            task_id: request.task.task_id,
            delivery_id: request.delivery_id,
            attempt: request.attempt,
            text: request.task.text.clone(),
            source,
        });
    }

    /// The observation lock, recovering from poisoning (see the module docs).
    fn calls_guard(&self) -> MutexGuard<'_, Vec<MockCall>> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for MockAgent {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MockAgent {
    /// Counts only: the configured replies are caller-authored strings and the
    /// recorded calls may carry task text, so neither is rendered here.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockAgent")
            .field("by_task", &self.by_task.len())
            .field("by_text", &self.by_text.len())
            .field("by_attempt", &self.by_attempt.len())
            .field("calls", &self.call_count())
            .finish()
    }
}

impl TaskDispatcher for MockAgent {
    /// Record the call, then return the selected acceptance decision.
    ///
    /// The returned future has no `.await`, so the observation lock is released
    /// before the worker can do anything else.
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            let (reply, source) = self.select(&request);
            self.record(MockStage::Deliver, &request, source);
            match reply.deliver {
                DeliverBehavior::Accept => Ok(()),
                DeliverBehavior::Refuse { reason } => Err(DispatchError::NotAccepted { reason }),
            }
        })
    }

    /// Record the call, then return the selected terminal outcome.
    ///
    /// The returned future has no `.await` (see [`MockAgent::deliver`]).
    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            let (reply, source) = self.select(&request);
            self.record(MockStage::Execute, &request, source);
            match reply.execute {
                ExecuteBehavior::Complete { output } => Ok(DispatchOutcome::Completed { output }),
                ExecuteBehavior::ReportFailure { error } => Ok(DispatchOutcome::Failed { error }),
                ExecuteBehavior::Fail { reason } => Err(DispatchError::ExecutionFailed { reason }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::bus::EndpointRegistry;
    use crate::bus::registry::derive_endpoint_id;
    use crate::config::{Config, load_from_str_with_env};
    use crate::models::{AgentTask, ConversationId, EndpointId, Priority};

    /// The `acp` target is deliberately addressable, so the "no address in the
    /// error text" tests have a real `command`/`args` to leak.
    const CONFIG: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
args = ["--stdio", "--token=super-secret"]
workspace = "/tmp"
enabled = true
"#;

    /// One validated target plus one task, so a test can build a real
    /// [`DispatchRequest`] from the *same* public path the worker uses.
    struct Harness {
        task: AgentTask,
        registry: EndpointRegistry,
    }

    impl Harness {
        fn new(text: &str) -> Self {
            let mut env = BTreeMap::new();
            env.insert("HOME".to_string(), "/home/tester".to_string());
            let config: Config =
                load_from_str_with_env(CONFIG, &env).expect("the test config must be valid");
            let task_id = TaskId::generate();
            Self {
                task: AgentTask {
                    task_id,
                    root_task_id: task_id,
                    parent_task_id: None,
                    from_agent: EndpointId::generate(),
                    to_agent: derive_endpoint_id("worker"),
                    conversation_id: ConversationId::generate(),
                    reply_to: None,
                    text: text.into(),
                    priority: Priority::DEFAULT,
                    depth: 0,
                    hops: 0,
                    deadline: None,
                    version: 0,
                },
                registry: EndpointRegistry::from_config(&config),
            }
        }

        fn request_with(&self, attempt: u32) -> DispatchRequest<'_> {
            DispatchRequest {
                task: &self.task,
                target: self
                    .registry
                    .validate_target(self.task.to_agent)
                    .expect("the target is declared, enabled and addressable"),
                delivery_id: DeliveryId::generate(),
                attempt,
            }
        }

        fn request(&self) -> DispatchRequest<'_> {
            self.request_with(1)
        }
    }

    async fn complete(mock: &MockAgent, request: DispatchRequest<'_>) -> String {
        match mock.execute(request).await.expect("execute must not fail") {
            DispatchOutcome::Completed { output } => output,
            other => panic!("expected a Completed outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_default_reply_accepts_and_completes() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::new();

        mock.deliver(harness.request()).await.expect("accepted");
        let output = complete(&mock, harness.request()).await;

        assert_eq!(output, DEFAULT_OUTPUT);
        assert_eq!(
            mock.calls()
                .iter()
                .map(|call| call.source)
                .collect::<Vec<_>>(),
            [ReplySource::Default, ReplySource::Default],
            "an unscripted call is recorded as such, never silently"
        );
        assert_eq!(
            format!("{mock:?}"),
            "MockAgent { by_task: 0, by_text: 0, by_attempt: 0, calls: 2 }"
        );
    }

    #[tokio::test]
    async fn default_and_new_agree() {
        let harness = Harness::new("do the thing");
        assert_eq!(
            complete(&MockAgent::default(), harness.request()).await,
            DEFAULT_OUTPUT
        );
    }

    #[tokio::test]
    async fn the_convenience_constructors_configure_the_expected_stages() {
        assert_eq!(
            MockAgent::completing("done").default,
            MockReply::completed("done")
        );
        assert_eq!(
            MockAgent::refusing("nope").default,
            MockReply {
                deliver: DeliverBehavior::Refuse {
                    reason: "nope".into()
                },
                execute: ExecuteBehavior::Complete {
                    output: DEFAULT_OUTPUT.into()
                },
            },
            "a refused delivery never reaches execute, so its half is inert"
        );
        assert_eq!(
            MockAgent::reporting_failure("bad output").default,
            MockReply {
                deliver: DeliverBehavior::Accept,
                execute: ExecuteBehavior::ReportFailure {
                    error: "bad output".into()
                },
            }
        );
        assert_eq!(
            MockAgent::failing_execution("crashed").default,
            MockReply {
                deliver: DeliverBehavior::Accept,
                execute: ExecuteBehavior::Fail {
                    reason: "crashed".into()
                },
            }
        );
    }

    #[tokio::test]
    async fn a_task_id_rule_beats_a_text_rule_beats_an_attempt_rule() {
        let harness = Harness::new("match-me");
        let mock = MockAgent::new()
            .on_attempt(1, MockReply::completed("by-attempt"))
            .on_text("match-me", MockReply::completed("by-text"))
            .on_task(harness.task.task_id, MockReply::completed("by-id"));

        assert_eq!(complete(&mock, harness.request()).await, "by-id");
        assert_eq!(mock.calls()[0].source, ReplySource::TaskId);

        // Same text and attempt, different identity: the text rule wins next.
        let same_text = Harness::new("match-me");
        assert_eq!(complete(&mock, same_text.request()).await, "by-text");
        assert_eq!(mock.calls()[1].source, ReplySource::Text);

        // A different text leaves only the attempt rule.
        let other = Harness::new("something else");
        assert_eq!(complete(&mock, other.request()).await, "by-attempt");
        assert_eq!(mock.calls()[2].source, ReplySource::Attempt);

        // And nothing matches attempt 2.
        assert_eq!(complete(&mock, other.request_with(2)).await, DEFAULT_OUTPUT);
        assert_eq!(mock.calls()[3].source, ReplySource::Default);
    }

    #[tokio::test]
    async fn precedence_does_not_depend_on_registration_order() {
        let harness = Harness::new("match-me");
        let reversed = MockAgent::new()
            .on_task(harness.task.task_id, MockReply::completed("by-id"))
            .on_text("match-me", MockReply::completed("by-text"))
            .on_attempt(1, MockReply::completed("by-attempt"));

        assert_eq!(complete(&reversed, harness.request()).await, "by-id");
        assert_eq!(reversed.calls()[0].source, ReplySource::TaskId);
    }

    #[tokio::test]
    async fn rules_of_the_same_kind_use_registration_order() {
        let harness = Harness::new("match-me");
        let mock = MockAgent::new()
            .on_text("match-me", MockReply::completed("first"))
            .on_text("match-me", MockReply::completed("second"));

        assert_eq!(complete(&mock, harness.request()).await, "first");
    }

    #[tokio::test]
    async fn rules_can_also_be_scripted_as_data() {
        let harness = Harness::new("match-me");
        let mock = MockAgent::new()
            .on(MockRule::Attempt(1), MockReply::completed("by-attempt"))
            .on(
                MockRule::Text("match-me".into()),
                MockReply::completed("by-text"),
            );

        assert_eq!(complete(&mock, harness.request()).await, "by-text");
        assert_eq!(mock.calls()[0].source, ReplySource::Text);
    }

    #[tokio::test]
    async fn an_attempt_rule_drives_both_stages_of_that_attempt() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::new().on_attempt(1, MockReply::refused("handshake lost"));

        let error = mock
            .deliver(harness.request())
            .await
            .expect_err("the attempt rule refuses");
        assert_eq!(
            error,
            DispatchError::NotAccepted {
                reason: "handshake lost".into()
            }
        );
        assert_eq!(
            mock.calls()[0].source,
            ReplySource::Attempt,
            "the refusal came from the attempt rule"
        );
    }

    #[tokio::test]
    async fn calls_are_recorded_in_order_with_their_identity() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::completing("done");

        let first = harness.request_with(1);
        let (delivery_id, attempt) = (first.delivery_id, first.attempt);
        mock.deliver(first).await.expect("accepted");
        let second = harness.request_with(1);
        mock.execute(second).await.expect("completed");

        let calls = mock.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].stage, MockStage::Deliver);
        assert_eq!(calls[0].task_id, harness.task.task_id);
        assert_eq!(calls[0].delivery_id, delivery_id);
        assert_eq!(calls[0].attempt, attempt);
        assert_eq!(calls[0].text, "do the thing");
        assert_eq!(calls[1].stage, MockStage::Execute);

        assert_eq!(mock.deliver_calls().len(), 1);
        assert_eq!(mock.execute_calls().len(), 1);
        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn calls_is_a_snapshot_that_later_calls_do_not_mutate() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::new();

        mock.deliver(harness.request()).await.expect("accepted");
        let snapshot = mock.calls();

        mock.execute(harness.request()).await.expect("completed");

        assert_eq!(snapshot.len(), 1, "the snapshot is owned, not a view");
        assert_eq!(mock.calls().len(), 2);
    }

    #[tokio::test]
    async fn a_refusal_replays_its_reason_verbatim_without_the_target_address() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::refusing("mock refusal");

        let error = mock
            .deliver(harness.request())
            .await
            .expect_err("the default reply refuses");
        let rendered = error.to_string();

        assert!(
            rendered.contains("mock refusal"),
            "the configured reason is replayed: {rendered}"
        );
        for leak in ["worker-acp", "--stdio", "super-secret", "token"] {
            assert!(
                !rendered.contains(leak),
                "the address must never be rendered, found {leak:?} in {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn a_terminal_failure_replays_its_error_verbatim() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::reporting_failure("adapter said no");

        let outcome = mock.execute(harness.request()).await.expect("executed");
        assert_eq!(
            outcome,
            DispatchOutcome::Failed {
                error: "adapter said no".into()
            }
        );
    }

    #[tokio::test]
    async fn an_execution_failure_replays_its_reason_verbatim() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::failing_execution("connection dropped");

        let error = mock
            .execute(harness.request())
            .await
            .expect_err("execution failed");
        assert_eq!(
            error,
            DispatchError::ExecutionFailed {
                reason: "connection dropped".into()
            }
        );
        assert_eq!(
            error.to_string(),
            "execution failed after acceptance: connection dropped"
        );
    }

    #[tokio::test]
    async fn extreme_and_empty_inputs_do_not_panic() {
        let harness = Harness::new("");
        let mock = MockAgent::new().on_text("", MockReply::completed("empty text"));

        assert_eq!(complete(&mock, harness.request_with(0)).await, "empty text");
        assert_eq!(
            complete(&mock, harness.request_with(u32::MAX)).await,
            "empty text"
        );
        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn debug_reports_counts_and_never_the_configured_reason() {
        let harness = Harness::new("do the thing");
        let mock = MockAgent::new().on_text("do the thing", MockReply::refused("token=secret"));
        let _ = &harness;

        let rendered = format!("{mock:?}");
        assert!(!rendered.contains("secret"), "got: {rendered}");
        assert!(rendered.contains("by_text: 1"), "got: {rendered}");
        assert!(rendered.contains("calls: 0"), "got: {rendered}");
    }

    #[test]
    fn mock_handles_are_send_sync_and_object_safe() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}
        assert_send_sync::<MockAgent>();
        assert_send_sync::<MockCall>();
        assert_send::<BusFuture<'_, Result<(), DispatchError>>>();

        let mock: Arc<dyn TaskDispatcher> = Arc::new(MockAgent::new());
        assert!(Arc::strong_count(&mock) == 1);
    }
}
