//! Build the maximum final input through bounded production page reservations/CAS.
use super::*;
use crate::{
    config::Config,
    matrix::coverage::{Clock, Coverage, Page, Room, Target},
};
const ROOM: &str = "!room:example.test";
fn event(id: &str) -> Value {
    json!({"type":"m.room.message","event_id":id,"sender":"@a:example.test","origin_server_ts":1,"content":{"msgtype":"m.notice","body":"ignored"}})
}
async fn prior_anchor(f: &tests::Fixture, anchor: &Value) {
    let at = chrono::Utc::now().to_rfc3339();
    let digest = format!("{:x}", Sha256::digest(anchor.to_string().as_bytes()));
    f.store
        .begin_batch(
            "cost-anchor",
            Some("B"),
            "ANCHOR",
            "anchor-response",
            "B",
            "owner",
            f.epoch,
            &["$cost-anchor".into()],
            &at,
        )
        .await
        .unwrap();
    f.store
        .reserve_event(
            "@bridge:example.test",
            ROOM,
            "$cost-anchor",
            &f.operation,
            &digest,
            "cost-anchor",
            &at,
        )
        .await
        .unwrap();
    f.store
        .start_event(
            "@bridge:example.test",
            ROOM,
            "$cost-anchor",
            &f.operation,
            "cost-anchor",
            &at,
        )
        .await
        .unwrap();
    f.store
        .finish_event(
            "@bridge:example.test",
            ROOM,
            "$cost-anchor",
            &f.operation,
            "cost-anchor",
            "owner",
            f.epoch,
            &at,
        )
        .await
        .unwrap();
    f.store
        .complete_batch("cost-anchor", "ANCHOR", "owner", f.epoch, &at)
        .await
        .unwrap();
}
async fn seed() -> (tests::Fixture, State, Config, Coverage, Value) {
    let f = tests::fixture(false).await;
    let anchor = event("$cost-anchor");
    prior_anchor(&f, &anchor).await;
    f.store
        .refuse_response(
            &f.operation,
            "owner",
            f.epoch,
            "rebuild-history-gap",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let mut config = crate::config::load_from_str("").unwrap();
    config.bridge.database = f.root.path().join("state.db");
    config.transports.matrix.user_id = "@bridge:example.test".into();
    let row = Coverage::new(
        &state,
        Exclusion::modeled(state.owner.clone())
            .pairing(&config)
            .unwrap(),
        Clock::read().unwrap(),
    )
    .unwrap();
    (f, state, config, row, anchor)
}
async fn change(
    f: &tests::Fixture,
    state: &State,
    cfg: &Config,
    row: &mut Coverage,
    action: impl FnOnce(&mut Coverage),
) {
    let mut next = row.clone();
    action(&mut next);
    next.revision += 1;
    coverage_tests::save(f, state, cfg, Some(row), &next).await;
    *row = next;
}
pub(super) async fn maximum() -> (
    tests::Fixture,
    State,
    Config,
    Coverage,
    Vec<(String, Value)>,
) {
    let (f, state, cfg, mut row, anchor) = seed().await;
    coverage_tests::save(&f, &state, &cfg, None, &row).await;
    change(&f, &state, &cfg, &mut row, |r| {
        r.charge_sync(Clock::read().unwrap()).unwrap()
    })
    .await;
    let digest = format!("{:x}", Sha256::digest(anchor.to_string().as_bytes()));
    change(&f, &state, &cfg, &mut row, |r| {
        r.freeze(Target {
            next: "C".into(),
            rooms: vec![Room {
                id: ROOM.into(),
                initial: vec![event("$cost-initial")],
                anchor: Some(crate::matrix::rebuild_gap::Anchor {
                    room: ROOM.into(),
                    event: "$cost-anchor".into(),
                    digest,
                    batch: "cost-anchor".into(),
                }),
                origin: Some("p0".into()),
                cursor: Some("p0".into()),
                complete: false,
            }],
        })
        .unwrap()
    })
    .await;
    for n in 0..128 {
        if row.segment_pages == 12
            || row.segment_events > 1100
            || row
                .segment_remaining(&Clock::read().unwrap())
                .is_ok_and(|t| t.as_secs() < 15)
        {
            change(&f, &state, &cfg, &mut row, |r| {
                r.segment(Clock::read().unwrap()).unwrap()
            })
            .await;
        }
        change(&f, &state, &cfg, &mut row, |r| {
            r.charge_page(Clock::read().unwrap()).unwrap()
        })
        .await;
        let mut events = (0..if n == 127 { 99 } else { 100 })
            .map(|i| event(&format!("$cost-{n}-{i}")))
            .collect::<Vec<_>>();
        if n == 127 {
            events.push(anchor.clone());
        }
        let page = Page {
            room: ROOM.into(),
            start: format!("p{n}"),
            end: Some(format!("p{}", n + 1)),
            events,
            anchor: None,
        };
        change(&f, &state, &cfg, &mut row, |r| {
            r.tick(Clock::read().unwrap()).unwrap();
            r.accept(page).unwrap();
        })
        .await;
    }
    assert_eq!(row.pages.len(), 128);
    let values = row.payloads().unwrap();
    assert_eq!(values.len(), 12800);
    (f, state, cfg, row, values)
}
