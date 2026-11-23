//! Measures the actual native final transaction, including bounded refusal/rollback.
use super::*;
use crate::matrix::coverage::{Clock, Coverage, Room, Target};

pub(super) async fn ready(
    f: &tests::Fixture,
    state: &State,
    config: &crate::config::Config,
    mut row: Coverage,
    count: usize,
) -> (Coverage, Vec<(String, Value)>) {
    coverage_tests::save(f, state, config, None, &row).await;
    let mut next = row.clone();
    next.charge_sync(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(f, state, config, Some(&row), &next).await;
    row = next;
    let values = (0..count)
        .map(|n| {
            json!({"type":"m.room.message",
        "event_id":format!("$cost-{n}"),"sender":"@a:example.test",
        "origin_server_ts":1,"content":{"msgtype":"m.notice","body":"ignored"}})
        })
        .collect::<Vec<_>>();
    let mut next = row.clone();
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: "!room:example.test".into(),
            initial: values.clone(),
            anchor: None,
            origin: None,
            cursor: None,
            complete: true,
        }],
    })
    .unwrap();
    next.revision += 1;
    coverage_tests::save(f, state, config, Some(&row), &next).await;
    (
        next,
        values
            .into_iter()
            .map(|v| ("!room:example.test".into(), v))
            .collect(),
    )
}

pub(super) fn proof(
    state: &State,
    config: &crate::config::Config,
    row: Coverage,
    payloads: &[(String, Value)],
) -> Exclusion {
    let mut proof = Exclusion::modeled(state.owner.clone());
    let mut expected = payloads
        .iter()
        .map(|(_, v)| v["event_id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    proof.decision_config = Some(config.clone());
    proof.rebuild = Some(crate::matrix::rebuild_gap::Rebuild {
        batch: crate::native_succession::state::Batch {
            id: row.batch_id.clone(),
            request: Some(state.watermark.clone()),
            previous: state.watermark.clone(),
            response: "C".into(),
            digest: crate::matrix::response_identity::digest(
                Some(&state.watermark),
                "C",
                payloads.to_vec(),
            )
            .unwrap(),
            owner: state.owner.token.clone(),
            epoch: state.leases.iter().find(|l| l.0 == "MatrixSync").unwrap().2,
            expected,
            covered: vec![],
        },
        anchors: row
            .target
            .as_ref()
            .unwrap()
            .rooms
            .iter()
            .filter_map(|r| r.anchor.clone())
            .collect(),
        unrouted: vec![],
    });
    proof.coverage = Some(row);
    proof
}

#[tokio::test]
#[ignore = "explicit 12800-event native publication cost measurement"]
async fn coverage_native_12800_publication_cost_and_deadline_rollback() {
    let began = std::time::Instant::now();
    let (f, state, config, row, payloads) = super::coverage_cost_fixture::maximum().await;
    println!(
        "native_12800_prepare_ms={} document_bytes={}",
        began.elapsed().as_millis(),
        row.encoded().unwrap().len()
    );
    let mut next = row.clone();
    next.charge_publish(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    let proof = proof(&state, &config, next.clone(), &payloads);
    let began = std::time::Instant::now();
    let result = publish(&f.pool, &state, &proof, &payloads, || Ok(())).await;
    println!(
        "native_12800_publish_ms={} result={result:?}",
        began.elapsed().as_millis()
    );
    match result {
        Ok(()) => assert!(
            confirmed(&config.bridge.database, &state, &proof)
                .await
                .unwrap()
        ),
        Err(error) => {
            assert!(
                error.to_string().contains("coverage-publication-deadline"),
                "{error}"
            );
            // A bounded refusal is reported, never called a successful publication.
            let mut c = f.pool.acquire().await.unwrap();
            assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
            assert_eq!(coverage::read(&mut c).await.unwrap().as_ref(), Some(&next));
            let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts")
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert_eq!(receipts, 1);
            println!("native_12800_bounded_refusal=true rollback=true hold_preserved=true");
        }
    }
    f.pool.close().await;
}
