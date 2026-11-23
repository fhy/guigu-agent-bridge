use super::*;
use serde_json::json;
fn clock(ms: i64) -> Clock {
    Clock {
        utc_ms: ms,
        boot: "boot-one".into(),
        boot_ms: ms,
    }
}
fn row() -> Coverage {
    let state = State {
        owner: crate::native_succession::state::Owner {
            token: "old".into(),
            generation: 1,
            revision: 1,
            pid: 1,
            starttime: 1,
            boot: "old-boot".into(),
            fingerprint: "old-process".into(),
        },
        native_generation: "native".into(),
        revision: 1,
        operation: "op".into(),
        watermark: "B".into(),
        hold: Some("refused".into()),
        batch: None,
        leases: vec![(
            "MatrixSync".into(),
            "old".into(),
            1,
            "expired".into(),
            "past".into(),
        )],
        isolated_projections: vec![],
        pre_accept: None,
        history: vec![],
        history_hold_owner: false,
    };
    Coverage::new(&state, "pair".into(), clock(0)).unwrap()
}
fn event(id: &str) -> Value {
    json!({"type":"m.room.message","event_id":id,"sender":"@sender:example.test","content":{"msgtype":"m.notice","body":"no effect"}})
}
fn target() -> Target {
    let anchor = event("$anchor");
    Target {
        next: "C".into(),
        rooms: vec![
            Room {
                id: "!r:example.test".into(),
                initial: vec![event("$initial")],
                anchor: Some(Anchor {
                    room: "!r:example.test".into(),
                    event: "$anchor".into(),
                    digest: validation::digest(&anchor).unwrap(),
                    batch: "old-batch".into(),
                }),
                origin: Some("p0".into()),
                cursor: Some("p0".into()),
                complete: false,
            },
            Room {
                id: "!second:example.test".into(),
                initial: vec![],
                anchor: None,
                origin: None,
                cursor: None,
                complete: true,
            },
        ],
    }
}
fn start() -> Coverage {
    let mut r = row();
    r.charge_sync(clock(0)).unwrap();
    r.freeze(target()).unwrap();
    r
}
fn page(n: usize, last: bool) -> Page {
    let mut events = (0..if last { 99 } else { 100 })
        .map(|i| event(&format!("$p{n}e{i}")))
        .collect::<Vec<_>>();
    if last {
        events.push(event("$anchor"));
    }
    Page {
        room: "!r:example.test".into(),
        start: format!("p{n}"),
        end: Some(format!("p{}", n + 1)),
        events,
        anchor: None,
    }
}
#[test]
fn bounded_coverage_12800_full_encoding_and_resume() {
    let mut r = start();
    let began = std::time::Instant::now();
    for n in 0..128 {
        let now = clock(n as i64 * 5000);
        if r.segment_pages == 12 || r.segment_events > 1100 {
            let encoded = r.encoded().unwrap();
            r = serde_json::from_str(&encoded).unwrap();
            r.validate().unwrap();
            r.segment(now.clone()).unwrap();
            assert_eq!(r.frontier().unwrap().unwrap().1, format!("p{n}"));
        }
        r.charge_page(now).unwrap();
        r.accept(page(n, n == 127)).unwrap();
    }
    assert_eq!(r.status, "ready");
    assert_eq!(r.payloads().unwrap().len(), 12800);
    assert_eq!(r.segments, 11);
    println!(
        "coverage_12800_encoding_bytes={} elapsed_ms={}",
        r.encoded().unwrap().len(),
        began.elapsed().as_millis()
    );
    assert!(r.charge_page(clock(650000)).is_err());
}
#[test]
fn complete_encoded_row_counts_metadata_not_only_events() {
    let mut r = row();
    r.pairing = "x".repeat(MAX_BYTES - 1);
    assert!(r.encoded().is_err());
    r.pairing = "x".repeat(MAX_BYTES / 2);
    assert!(r.encoded().is_ok());
    r.pairing = "\"".repeat(MAX_BYTES / 4);
    assert!(r.encoded().is_err(), "JSON string escaping must count");
}
#[test]
fn target_anchor_tokens_duplicates_and_payload_corruption_refuse() {
    for mode in 0..5 {
        let mut r = start();
        r.charge_page(clock(1)).unwrap();
        let mut p = page(0, true);
        match mode {
            0 => p.start = "other".into(),
            1 => p.end = Some("p0".into()),
            2 => p.events[99]["content"]["body"] = "changed anchor".into(),
            3 => p.events[1] = p.events[0].clone(),
            4 => p.events[0] = event("$initial"),
            _ => unreachable!(),
        }
        assert!(r.accept(p).is_err(), "{mode}");
    }
    let mut r = start();
    r.charge_page(clock(1)).unwrap();
    r.accept(page(0, false)).unwrap();
    r.pages[0].start = "forged".into();
    assert!(r.validate().is_err());
}
#[test]
fn budgets_are_not_reset_by_interruption_or_boot() {
    let mut r = start();
    r.charge_page(clock(1)).unwrap();
    r = serde_json::from_str(&r.encoded().unwrap()).unwrap();
    let mut next = clock(2);
    next.boot = "boot-two".into();
    r.segment(next).unwrap();
    assert_eq!(r.attempts, 1);
    assert!(r.reservation.is_none());
    assert_eq!(r.deadline_ms, 1800000);
    assert!(r.tick(clock(1)).is_err());
    assert!(r.segment(clock(1800001)).is_err());
    r = row();
    for _ in 1..16 {
        r.segment(clock(1)).unwrap();
    }
    assert!(r.segment(clock(2)).is_err());
    r = row();
    r.charge_sync(clock(0)).unwrap();
    r.charge_sync(clock(0)).unwrap();
    assert!(r.charge_sync(clock(0)).is_err());
}
#[test]
fn page_time_and_attempt_precharge_cannot_be_refunded() {
    let mut r = start();
    for _ in 0..15 {
        r.charge_page(clock(1)).unwrap();
    }
    assert!(r.charge_page(clock(1)).is_err());
    assert_eq!(r.attempts, 15);
    r.segment(clock(2)).unwrap();
    r.charge_page(clock(3)).unwrap();
    assert_eq!(r.attempts, 16);
    assert!(r.segment_remaining(&clock(90003)).is_err());
    r.attempts = 180;
    assert!(r.charge_page(clock(4)).is_err());
    r = start();
    r.charge_page(clock(1)).unwrap();
    r.accept(page(0, true)).unwrap();
    for _ in 0..3 {
        r.charge_publish(clock(2)).unwrap();
    }
    assert!(r.charge_publish(clock(2)).is_err());
}

#[test]
fn monotonic_elapsed_cannot_extend_lifetime_when_wall_clock_stalls() {
    let mut r = row();
    let mut now = clock(1);
    now.boot_ms = 1_799_999;
    r.tick(now.clone()).unwrap();
    assert_eq!(r.elapsed_ms, 1_799_999);
    now.boot_ms += 2;
    assert!(r.tick(now).is_err());
    let encoded = r.encoded().unwrap();
    let mut r: Coverage = serde_json::from_str(&encoded).unwrap();
    let mut other = clock(3);
    other.boot = "another".into();
    assert!(
        r.tick(other).is_err(),
        "cross-boot wall delta is not a refund"
    );
}

#[test]
fn initial_timeline_consumes_segment_event_and_byte_budget() {
    let mut r = row();
    r.charge_sync(clock(0)).unwrap();
    let mut t = target();
    t.rooms[0].initial = (0..1201).map(|n| event(&format!("$init-{n}"))).collect();
    assert!(r.freeze(t).is_err());
    let mut r = row();
    r.charge_sync(clock(0)).unwrap();
    let mut t = target();
    t.rooms[0].initial[0]["padding"] = serde_json::Value::String("x".repeat(4 * 1024 * 1024));
    assert!(r.freeze(t).is_err());
}
