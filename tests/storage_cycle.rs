//! T009 cycle-gate tests: the persisted-tree checks behind T006's declared-field
//! limits, driven through the real repository, pool and migrations.
//!
//! The tests build real task trees (task rows plus their `Queued` events) and then
//! ask [`detect`] about a task that has not been written yet — the submission
//! gate's position. The block itself is exercised with the documented recipe:
//! accept the task, then append [`CycleHit::failed_event`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use guigu_agent_bridge::bus::derive_endpoint_id;
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::models::{
    AgentTask, Conversation, ConversationId, EndpointId, EventId, Priority, TaskEvent,
    TaskEventPayload, TaskId, TaskStatus,
};
use guigu_agent_bridge::storage::{
    CycleKind, CycleLimits, Repository, SqliteRepository, connect, detect, migrate,
};

const TS: &str = "2026-09-16T10:00:00.000000000Z";
const SECRET_BODY: &str = "SECRET-BODY-DO-NOT-LEAK";

fn ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

fn load(toml: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(toml, &env).expect("test config must be valid")
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct TestDb {
    repository: SqliteRepository,
    pool: SqlitePool,
    path: PathBuf,
    conversation: ConversationId,
}

impl TestDb {
    async fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "guigu-storage-cycle-{tag}-{}.db",
            uuid::Uuid::now_v7()
        ));
        let pool = connect(&path).await.expect("connect");
        migrate(&pool).await.expect("migrate");
        let repository = SqliteRepository::new(pool.clone());
        let conversation = Conversation {
            id: ConversationId::generate(),
            participants: Vec::new(),
            external_ref: None,
        };
        repository
            .insert_conversation(&conversation)
            .await
            .expect("conversation");
        Self {
            repository,
            pool,
            path,
            conversation: conversation.id,
        }
    }

    async fn cleanup(&self) {
        self.pool.close().await;
        remove_db_files(&self.path);
    }
}

fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(candidate));
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn endpoint(agent_id: &str) -> EndpointId {
    derive_endpoint_id(agent_id)
}

/// A task that has not been stored yet, so it can be handed to [`detect`].
fn pending_task(db: &TestDb, to_agent: EndpointId, parent: Option<TaskId>) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: parent,
        from_agent: endpoint("requester"),
        to_agent,
        conversation_id: db.conversation,
        reply_to: None,
        text: SECRET_BODY.into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

/// Store `task` (with its `Queued` event) and return it.
async fn store(db: &TestDb, task: AgentTask) -> AgentTask {
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: task.task_id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: ts(),
        payload: TaskEventPayload::Queued,
    };
    db.repository
        .insert_task_and_event(&task, &event)
        .await
        .expect("store task");
    task
}

/// Build a linear ancestor chain of `length` tasks, each targeting a distinct
/// agent, and return the deepest task.
async fn chain(db: &TestDb, length: usize) -> AgentTask {
    let mut current: Option<AgentTask> = None;
    for index in 0..length {
        let parent = current.as_ref().map(|task| task.task_id);
        let task = pending_task(db, endpoint(&format!("agent-{index}")), parent);
        current = Some(store(db, task).await);
    }
    current.expect("a non-empty chain")
}

// ---------------------------------------------------------------------------
// C1: A → B → A is blocked, recorded, and terminal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_revisited_agent_is_blocked_and_recorded_as_a_terminal_failure() {
    let db = TestDb::new("visited").await;
    let limits = CycleLimits::default();

    // A → B, then B → A: asking B again is the loop.
    let first = store(&db, pending_task(&db, endpoint("b"), None)).await;
    let second = store(&db, pending_task(&db, endpoint("a"), Some(first.task_id))).await;

    let looping = pending_task(&db, endpoint("b"), Some(second.task_id));
    let hit = detect(&db.repository, &looping, limits)
        .await
        .expect("detect")
        .expect("a revisit must be detected");

    assert_eq!(hit.kind(), CycleKind::VisitedAgent);
    assert_eq!(hit.revisited(), Some(endpoint("b")));
    assert_eq!(hit.observed(), 2, "the nearest ancestor wins");

    // The documented wiring: accept the task, then record the terminal block.
    store(&db, looping.clone()).await;
    let failed = hit.failed_event(ts());
    assert_eq!(failed.seq, 2, "Queued took seq 1, so the block is seq 2");
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.task_id, looping.task_id);
    db.repository
        .append_event(&failed)
        .await
        .expect("append block");

    assert_eq!(
        db.repository
            .latest_event(looping.task_id)
            .await
            .expect("latest"),
        Some(failed.clone())
    );
    assert!(
        !db.repository
            .unfinished_tasks()
            .await
            .expect("unfinished")
            .contains(&looping.task_id),
        "a blocked task is terminal, so recovery must not re-run it"
    );

    let reason = hit.reason();
    assert!(
        reason.contains("visited-agent"),
        "the reason names the hit: {reason}"
    );
    for expected in [
        &format!("task_id={}", looping.task_id),
        "root_task_id=",
        "parent_task_id=",
        "from_agent=",
        "to_agent=",
        "chain=",
        "revisited=",
    ] {
        assert!(reason.contains(expected), "missing {expected} in {reason}");
    }
    assert!(
        !reason.contains(SECRET_BODY),
        "the task body must never be rendered: {reason}"
    );
    assert!(
        !reason.contains("worker-acp") && !reason.contains("--stdio"),
        "no address may be rendered: {reason}"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C2: siblings are not ancestors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_sibling_targeting_the_same_agent_is_not_a_cycle() {
    let db = TestDb::new("siblings").await;
    let limits = CycleLimits::default();

    let parent = store(&db, pending_task(&db, endpoint("b"), None)).await;
    store(&db, pending_task(&db, endpoint("c"), Some(parent.task_id))).await;

    // A second child of the same parent, aimed at the same agent as its sibling.
    let sibling = pending_task(&db, endpoint("c"), Some(parent.task_id));
    assert_eq!(
        detect(&db.repository, &sibling, limits)
            .await
            .expect("detect"),
        None,
        "only the ancestor chain counts, so fan-out to one agent is allowed"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C3: the chain ceiling, boundary included
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_chain_ceiling_is_a_strict_boundary() {
    let db = TestDb::new("chain").await;
    let limits = CycleLimits {
        max_chain: 3,
        max_subtasks: 32,
    };

    // Three ancestors: exactly at the ceiling.
    let deepest = chain(&db, 3).await;
    let at_limit = pending_task(&db, endpoint("fresh"), Some(deepest.task_id));
    assert_eq!(
        detect(&db.repository, &at_limit, limits)
            .await
            .expect("detect"),
        None,
        "a chain of exactly max_chain ancestors is allowed"
    );

    // One more ancestor: over the ceiling.
    let deeper = store(
        &db,
        pending_task(&db, endpoint("deeper"), Some(deepest.task_id)),
    )
    .await;
    let over_limit = pending_task(&db, endpoint("fresh"), Some(deeper.task_id));
    let hit = detect(&db.repository, &over_limit, limits)
        .await
        .expect("detect")
        .expect("an over-long chain must be detected");

    assert_eq!(hit.kind(), CycleKind::ChainTooLong);
    assert_eq!(hit.observed(), 4);
    assert_eq!(hit.limit(), 3);
    assert!(hit.reason().contains("chain=4 (max 3)"));

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C3b: a revisit landing exactly on the chain ceiling still outranks it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_revisit_past_the_chain_ceiling_is_still_a_visited_agent_hit() {
    let db = TestDb::new("collision").await;
    let limits = CycleLimits {
        max_chain: 3,
        max_subtasks: 32,
    };

    // Four ancestors: agent-0 (root) through agent-3 (deepest). Walking up from
    // the deepest, the root is reached at step 4 — one past the ceiling — and the
    // root is aimed at the very agent the new task targets, so the chain-length
    // and visited-agent conditions hold at the same position.
    let deepest = chain(&db, 4).await;
    let collision = pending_task(&db, endpoint("agent-0"), Some(deepest.task_id));

    let hit = detect(&db.repository, &collision, limits)
        .await
        .expect("detect")
        .expect("a hit");

    assert_eq!(
        hit.kind(),
        CycleKind::VisitedAgent,
        "a revisit outranks the chain ceiling (D11), even one step past it"
    );
    assert_eq!(hit.revisited(), Some(endpoint("agent-0")));
    assert_eq!(hit.observed(), 4, "the chain really is over the ceiling");
    assert_eq!(hit.limit(), 3);
    let reason = hit.reason();
    assert!(reason.contains("visited-agent"), "got: {reason}");
    assert!(reason.contains("chain=4 (max 3)"), "got: {reason}");
    assert!(!reason.contains(SECRET_BODY), "got: {reason}");

    // The control at the same depth, aimed at an agent that is not on the chain:
    // a too-long chain, not a revisit.
    let control = pending_task(&db, endpoint("nowhere"), Some(deepest.task_id));
    let control_hit = detect(&db.repository, &control, limits)
        .await
        .expect("detect")
        .expect("a hit");
    assert_eq!(
        control_hit.kind(),
        CycleKind::ChainTooLong,
        "the ceiling still fires when nothing was revisited"
    );
    assert_eq!(control_hit.observed(), 4);

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C4: the subtask ceiling, boundary included
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_subtask_ceiling_blocks_the_next_child() {
    let db = TestDb::new("subtasks").await;
    let limits = CycleLimits {
        max_chain: 8,
        max_subtasks: 2,
    };

    let parent = store(&db, pending_task(&db, endpoint("b"), None)).await;
    store(&db, pending_task(&db, endpoint("c"), Some(parent.task_id))).await;

    // One child so far: the second is still accepted.
    let second = pending_task(&db, endpoint("d"), Some(parent.task_id));
    assert_eq!(
        detect(&db.repository, &second, limits)
            .await
            .expect("detect"),
        None
    );
    store(&db, second).await;

    // Two children: the third is blocked.
    let third = pending_task(&db, endpoint("e"), Some(parent.task_id));
    let hit = detect(&db.repository, &third, limits)
        .await
        .expect("detect")
        .expect("the subtask ceiling must be enforced");

    assert_eq!(hit.kind(), CycleKind::TooManySubtasks);
    assert_eq!(hit.observed(), 2);
    assert_eq!(hit.limit(), 2);
    assert!(hit.reason().contains("parent_children=2 (max 2)"));
    assert!(!hit.reason().contains(SECRET_BODY));

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C5: nothing to walk
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_root_task_and_an_unknown_parent_are_both_allowed() {
    let db = TestDb::new("no-ancestors").await;
    let limits = CycleLimits {
        max_chain: 1,
        max_subtasks: 1,
    };

    let root = pending_task(&db, endpoint("b"), None);
    assert_eq!(
        detect(&db.repository, &root, limits).await.expect("detect"),
        None,
        "a root task has no ancestors and no parent whose children could be counted"
    );

    // An unknown parent is the foreign key's business, not this gate's.
    let orphan = pending_task(&db, endpoint("b"), Some(TaskId::generate()));
    assert_eq!(
        detect(&db.repository, &orphan, limits)
            .await
            .expect("detect"),
        None
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C6: layering against T006
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_gate_checks_the_observed_chain_and_never_hops() {
    let config = load("");
    let depth_ceiling = config.bridge.max_task_depth as usize;
    let limits = CycleLimits::from_bridge(&config.bridge);
    let db = TestDb::new("layering").await;

    // A deep real chain behind a task whose declared depth is well within the
    // configured limit: T006's declared-field check would pass, the observed
    // chain does not.
    let deepest = chain(&db, depth_ceiling + 1).await;
    let mut liar = pending_task(&db, endpoint("fresh"), Some(deepest.task_id));
    liar.depth = 1;
    liar.hops = 1;

    let hit = detect(&db.repository, &liar, limits)
        .await
        .expect("detect")
        .expect("the observed chain exceeds the ceiling");
    assert_eq!(hit.kind(), CycleKind::ChainTooLong);
    assert!(
        liar.depth <= config.bridge.max_task_depth,
        "the declared field is deliberately within its own limit"
    );

    // Hops have no evidence in the tree, so an absurd declared value changes
    // nothing here: that check belongs to T006.
    let shallow = store(&db, pending_task(&db, endpoint("b"), None)).await;
    let mut hopper = pending_task(&db, endpoint("c"), Some(shallow.task_id));
    hopper.hops = 1000;
    assert_eq!(
        detect(&db.repository, &hopper, limits)
            .await
            .expect("detect"),
        None,
        "T009 must not duplicate the worker's hops check"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// C7: a duplicate submission is detected exactly like the first one
// ---------------------------------------------------------------------------

#[tokio::test]
async fn detecting_twice_for_one_task_id_gives_one_answer() {
    let db = TestDb::new("duplicate").await;
    let limits = CycleLimits::default();

    let first = store(&db, pending_task(&db, endpoint("b"), None)).await;
    let second = store(&db, pending_task(&db, endpoint("a"), Some(first.task_id))).await;
    let looping = pending_task(&db, endpoint("b"), Some(second.task_id));

    let before = detect(&db.repository, &looping, limits)
        .await
        .expect("detect")
        .expect("hit");
    store(&db, looping.clone()).await;
    let after = detect(&db.repository, &looping, limits)
        .await
        .expect("detect")
        .expect("hit");

    assert_eq!(before.kind(), after.kind());
    assert_eq!(before.reason(), after.reason());
    assert_eq!(
        before.observed(),
        after.observed(),
        "detection keeps no cross-task state: the same tree gives the same answer"
    );

    db.cleanup().await;
}
