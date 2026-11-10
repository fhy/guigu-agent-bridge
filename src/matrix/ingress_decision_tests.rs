use super::ingress_decision::no_effect;
use serde_json::{Value, json};
const ROOM: &str = "!room:example.test";
fn event() -> Value {
    json!({"type":"m.room.message","event_id":"$input","sender":"@a:example.test","content":{"msgtype":"m.text","body":"@missing: hello"}})
}
fn config() -> crate::config::Config {
    crate::config::load_from_str("[agents.worker]\ntransport='acp'\ncommand='must-not-start'\nworkspace='/tmp'\nenabled=true\n[transports.matrix]\nuser_id='@bridge:example.test'\nallowed_users=['@a:example.test']\n").unwrap()
}
#[tokio::test]
async fn coverage_decision_rejects_admin_route_reply_and_unknown_effects() {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let mut c = pool.acquire().await.unwrap();
    let cfg = config();
    let value = event();
    no_effect(&mut c, &cfg, ROOM, &value).await.unwrap();
    for mode in 0..8 {
        let mut cfg = cfg.clone();
        let mut value = value.clone();
        match mode {
            0 => value["content"]["body"] = "/status".into(),
            1 => {
                cfg.transports
                    .matrix
                    .routes
                    .aliases
                    .insert("missing".into(), "worker".into());
            }
            2 => {
                cfg.transports.matrix.monitor_room = ROOM.into();
            }
            3 => {
                cfg.transports
                    .matrix
                    .routes
                    .rooms
                    .insert(ROOM.into(), "worker".into());
            }
            4 => {
                value["content"]["body"] = "hello".into();
                cfg.transports
                    .matrix
                    .routes
                    .direct
                    .insert(ROOM.into(), vec!["worker".into()]);
            }
            5 => value["type"] = "m.room.encrypted".into(),
            6 => value["content"]["msgtype"] = "com.guigu.bridge.a2a.v1".into(),
            7 => {
                value.as_object_mut().unwrap().remove("sender");
            }
            _ => unreachable!(),
        }
        assert!(
            no_effect(&mut c, &cfg, ROOM, &value).await.is_err(),
            "mode {mode}"
        );
    }
    drop(c);
    pool.close().await;
}
#[tokio::test]
async fn coverage_decision_reads_uncommitted_intent_on_supplied_connection() {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let cfg = config();
    let value = event();
    for reply in [false, true] {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        no_effect(&mut tx, &cfg, ROOM, &value).await.unwrap();
        sqlx::query("INSERT INTO transport_receipts(transport,external_event_id,room_id,reply_event_id,receipt_kind,route_kind,state,result_code,created_at,updated_at) VALUES('matrix',?,?,?,'ordinary','fixture','reserved','unknown','time','time')")
            .bind(if reply {"$another"} else {"$input"}).bind(ROOM).bind(if reply {Some("$input")} else {None}).execute(&mut *tx).await.unwrap();
        assert!(no_effect(&mut tx, &cfg, ROOM, &value).await.is_err());
        // A second pool connection cannot see this uncommitted intent.
        let external: i64 = sqlx::query_scalar("SELECT count(*) FROM transport_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(external, 0);
        tx.rollback().await.unwrap();
        let mut c = pool.acquire().await.unwrap();
        no_effect(&mut c, &cfg, ROOM, &value).await.unwrap();
    }
    pool.close().await;
}
