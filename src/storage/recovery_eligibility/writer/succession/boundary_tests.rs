use super::tests::{Fixture, fixture};
use super::*;

async fn pending(f: &Fixture, id: &str, from: &str, to: &str) {
    let digest =
        crate::matrix::response_identity::digest(Some(from), to, f.payloads.clone()).unwrap();
    f.store
        .begin_batch(
            id,
            Some(from),
            to,
            &digest,
            from,
            "owner",
            f.epoch,
            &["$notice:example.test".into()],
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
}
async fn reserve(f: &Fixture, batch: &str) {
    let digest = format!(
        "{:x}",
        Sha256::digest(f.payloads[0].1.to_string().as_bytes())
    );
    f.store
        .reserve_event(
            "@bridge:example.test",
            "!room:example.test",
            "$notice:example.test",
            &f.operation,
            &digest,
            batch,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
}
async fn read(f: &Fixture) -> (State, Exclusion) {
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    let proof = Exclusion::modeled(state.owner.clone());
    (state, proof)
}

#[tokio::test]
async fn terminal_receipt_is_deduplicated_without_second_disposition() {
    let f = fixture(false).await;
    let now = chrono::Utc::now().to_rfc3339();
    pending(&f, "first", "B", "C").await;
    reserve(&f, "first").await;
    f.store
        .finish_event(
            "@bridge:example.test",
            "!room:example.test",
            "$notice:example.test",
            &f.operation,
            "first",
            "owner",
            f.epoch,
            &now,
        )
        .await
        .unwrap();
    f.store
        .complete_batch("first", "C", "owner", f.epoch, &now)
        .await
        .unwrap();
    pending(&f, "second", "C", "D").await;
    assert!(f.store.reconcile_restart().await.is_err());
    let (state, proof) = read(&f).await;
    publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
        .await
        .unwrap();
    let receipt: (i64, String) =
        sqlx::query_as("SELECT count(*),min(batch_id) FROM matrix_recovery_receipts")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(receipt, (1, "first".into()));
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    f.pool.close().await;
}

#[tokio::test]
async fn pending_and_processing_receipts_never_use_no_receipt_recovery() {
    for started in [false, true] {
        let f = fixture(false).await;
        pending(&f, "pending", "B", "C").await;
        reserve(&f, "pending").await;
        if started {
            f.store
                .start_event(
                    "@bridge:example.test",
                    "!room:example.test",
                    "$notice:example.test",
                    &f.operation,
                    "pending",
                    &chrono::Utc::now().to_rfc3339(),
                )
                .await
                .unwrap();
        }
        assert!(f.store.reconcile_restart().await.is_err());
        let mut c = f.pool.acquire().await.unwrap();
        let before: Option<State> = state::read(&mut c).await.ok().flatten();
        drop(c);
        if let Some(state) = before {
            let proof = Exclusion::modeled(state.owner.clone());
            assert!(
                publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
                    .await
                    .is_err()
            );
        }
        let row:(String,String,Option<String>)=sqlx::query_as("SELECT r.state,c.watermark,c.restart_hold_state FROM matrix_recovery_receipts r CROSS JOIN matrix_recovery_control c").fetch_one(&f.pool).await.unwrap();
        assert_eq!(row.0, if started { "processing" } else { "pending" });
        assert_eq!(row.1, "B");
        assert!(row.2.is_some());
        f.pool.close().await;
    }
}

#[test]
fn abrupt_process_exit_before_and_after_publication_is_atomic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Some(path) = std::env::var_os("R105_SUCCESSION_CRASH_ROOT") {
        runtime.block_on(crash_child(Path::new(&path)));
        return;
    }
    runtime.block_on(async {
        for stage in ["before","after"] {
            let f=fixture(true).await;let (state,_)=read(&f).await;let database=f.root.path().join("state.db");f.pool.close().await;
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","storage::recovery_eligibility::writer::succession::boundary_tests::abrupt_process_exit_before_and_after_publication_is_atomic","--nocapture"])
                .env("R105_SUCCESSION_CRASH_ROOT",f.root.path()).env("R105_SUCCESSION_CRASH_STAGE",stage).output().unwrap();
            assert_eq!(output.status.code(),Some(if stage=="before"{87}else{88}),"{}",String::from_utf8_lossy(&output.stderr));
            let proof:Exclusion=serde_json::from_slice(&std::fs::read(f.root.path().join("proof.json")).unwrap()).unwrap();
            assert_eq!(confirmed(&database,&state,&proof).await.unwrap(),stage=="after");
            let pool=crate::storage::connect(&database).await.unwrap();let mut c=pool.acquire().await.unwrap();
            if stage=="before" {assert_eq!(state::read(&mut c).await.unwrap(),Some(state));}
            else {assert!(state::read(&mut c).await.unwrap().is_none());}
            drop(c);pool.close().await;
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
    let proof = Exclusion::modeled(state.owner.clone());
    std::fs::write(root.join("proof.json"), serde_json::to_vec(&proof).unwrap()).unwrap();
    let payloads = vec![(
        "!room:example.test".into(),
        json!({"type":"m.room.message","event_id":"$notice:example.test","sender":"@alice:example.test","origin_server_ts":1,"content":{"msgtype":"m.notice","body":"fixture notice"}}),
    )];
    let calls = std::cell::Cell::new(0);
    publish(&pool, &state, &proof, &payloads, || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 && std::env::var("R105_SUCCESSION_CRASH_STAGE").unwrap() == "before" {
            std::process::exit(87);
        }
        Ok(())
    })
    .await
    .unwrap();
    std::process::exit(88);
}

#[tokio::test]
async fn native_classified_nonterminal_execution_refuses_without_partial_retirement() {
    let f = fixture(false).await;
    let scope = RecoveryScope::new(
        crate::storage::recovery_eligibility::readback_nonadmin(&f.pool, true)
            .await
            .unwrap(),
        EffectDomain::Local,
        None,
    );
    let mut tx = super::super::begin(&f.pool, Some(&scope)).await.unwrap();
    for (query, table, key) in [
        (
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
            "conversations",
            "conversation",
        ),
        (
            "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','from','to','conversation','harmless',1,0,0,0)",
            "tasks",
            "task",
        ),
        (
            "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','invocation',1,'active','t','t','t')",
            "execution_leases",
            "resource",
        ),
    ] {
        sqlx::query(query).execute(&mut *tx).await.unwrap();
        super::super::inserted(&mut tx, Some(&scope), &text_identity(table, key).unwrap())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let (state, proof) = read(&f).await;
    let error = publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("business-effects"), "{error}");
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(state::read(&mut c).await.unwrap(), Some(state));
    drop(c);
    f.pool.close().await;
}
