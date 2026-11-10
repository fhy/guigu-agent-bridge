//! Real process exits around the production transactions, not a journal model.
use super::*;
use crate::matrix::coverage::{Clock, Coverage, Page, Room, Target};
fn config(root: &Path) -> crate::config::Config {
    let mut c = crate::config::load_from_str("").unwrap();
    c.bridge.database = root.join("state.db");
    c.transports.matrix.user_id = "@bridge:example.test".into();
    c
}
async fn input(phase: &str) -> (tests::Fixture, State, Option<Coverage>, Coverage) {
    let (f, state, c, mut row) = coverage_tests::setup().await;
    if phase == "initializing" {
        return (f, state, None, row);
    }
    coverage_tests::save(&f, &state, &c, None, &row).await;
    let mut next = row.clone();
    next.charge_sync(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &c, Some(&row), &next).await;
    row = next;
    let anchor = json!({"event_id":"$anchor","type":"m.room.message","content":{"msgtype":"m.notice","body":"own"},"sender":"@bridge:example.test"});
    let mut next = row.clone();
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: "!room:example.test".into(),
            initial: vec![],
            anchor: if phase == "page" {
                Some(crate::matrix::rebuild_gap::Anchor {
                    room: "!room:example.test".into(),
                    event: "$anchor".into(),
                    digest: format!("{:x}", Sha256::digest(anchor.to_string().as_bytes())),
                    batch: "prior".into(),
                })
            } else {
                None
            },
            origin: if phase == "page" {
                Some("p0".into())
            } else {
                None
            },
            cursor: if phase == "page" {
                Some("p0".into())
            } else {
                None
            },
            complete: phase != "page",
        }],
    })
    .unwrap();
    next.revision += 1;
    if phase == "target" {
        return (f, state, Some(row), next);
    }
    coverage_tests::save(&f, &state, &c, Some(&row), &next).await;
    row = next;
    let mut next = row.clone();
    if phase == "page" {
        next.charge_page(Clock::read().unwrap()).unwrap();
    } else {
        next.charge_publish(Clock::read().unwrap()).unwrap();
    }
    next.revision += 1;
    coverage_tests::save(&f, &state, &c, Some(&row), &next).await;
    row = next;
    if phase == "page" {
        let mut next = row.clone();
        next.accept(Page {
            room: "!room:example.test".into(),
            start: "p0".into(),
            end: Some("p1".into()),
            events: vec![f.payloads[0].1.clone()],
            anchor: None,
        })
        .unwrap();
        next.revision += 1;
        return (f, state, Some(row), next);
    }
    (f, state, Some(row.clone()), row)
}

#[test]
fn coverage_initial_target_page_and_final_crashes_are_atomic() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Some(root) = std::env::var_os("R112_COVERAGE_CRASH_ROOT") {
        rt.block_on(child(Path::new(&root)));
        return;
    }
    rt.block_on(async {
        for phase in ["initializing","target","page","final"] {for stage in ["before","after"] {
            let (f,state,old,next)=input(phase).await;
            std::fs::write(f.root.path().join("input.json"),serde_json::to_vec(&(&state,&old,&next)).unwrap()).unwrap();
            f.pool.close().await;
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","storage::recovery_eligibility::writer::succession::coverage_crash_tests::coverage_initial_target_page_and_final_crashes_are_atomic","--nocapture"])
                .env("R112_COVERAGE_CRASH_ROOT",f.root.path()).env("R112_COVERAGE_CRASH_PHASE",phase).env("R112_COVERAGE_CRASH_STAGE",stage).output().unwrap();
            assert_eq!(output.status.code(),Some(if stage=="before"{87}else{88}),"{phase}/{stage}: {}",String::from_utf8_lossy(&output.stderr));
            let c=config(f.root.path());let pool=crate::storage::connect(&c.bridge.database).await.unwrap();let mut conn=pool.acquire().await.unwrap();
            if phase=="final" && stage=="after" {
                let mut proof:Exclusion=serde_json::from_slice(&std::fs::read(f.root.path().join("proof.json")).unwrap()).unwrap();proof.coverage=Some(next.clone());proof.decision_config=Some(c.clone());
                assert!(confirmed(&c.bridge.database,&state,&proof).await.unwrap());
                assert_eq!(coverage::read(&mut conn).await.unwrap().unwrap().status,"published");
            }else{
                assert_eq!(state::read(&mut conn).await.unwrap().as_ref(),Some(&state));
                assert_eq!(coverage::read(&mut conn).await.unwrap(),if stage=="before"{old}else{Some(next)});
            }
            drop(conn);pool.close().await;
            println!("coverage_crash phase={phase} stage={stage} exact_readback=true");
        }}
    });
}
async fn child(root: &Path) {
    let (state, old, next): (State, Option<Coverage>, Coverage) =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    let c = config(root);
    let pool = crate::storage::connect(&c.bridge.database).await.unwrap();
    let calls = std::cell::Cell::new(0);
    let check = || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 && std::env::var("R112_COVERAGE_CRASH_STAGE").unwrap() == "before" {
            std::process::exit(87);
        }
        Ok(())
    };
    if std::env::var("R112_COVERAGE_CRASH_PHASE").unwrap() == "final" {
        let proof = coverage_cost_tests::proof(&state, &c, next, &[]);
        std::fs::write(root.join("proof.json"), serde_json::to_vec(&proof).unwrap()).unwrap();
        publish(&pool, &state, &proof, &[], check).await.unwrap();
    } else {
        coverage::save(&pool, &state, old.as_ref(), &next, &c, check)
            .await
            .unwrap();
    }
    std::process::exit(88);
}
