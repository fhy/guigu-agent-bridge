use super::*;
use serde_json::json;
use sqlx::Connection;

#[tokio::test]
async fn historical_optional_fields_are_not_new_recovery_requirements() {
    let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    let owner = Owner {
        token: "historical".into(),
        generation: 1,
        revision: 17,
        pid: 100,
        starttime: 200,
        boot: "boot".into(),
        fingerprint: "process".into(),
    };
    let mut previous = owner.clone();
    previous.revision -= 1;
    let mut record = json!({
        "kind":"safe-succession-v1", "historical_joined":false,
        "source":{"owner":previous, "native_generation":"native", "operation":"op", "revision":3},
        "exclusion":{"kind":"current-scoped-exclusion-v1", "owner":previous}
    });
    // Old R105 records precede isolated_projections, pre_accept and history.
    for shape in 0..3 {
        if shape == 1 {
            record["source"]["isolated_projections"] = json!([]);
        }
        if shape == 2 {
            record["source"]["pre_accept"] = json!(null);
        }
        let row = sqlx::query("SELECT ? AS child_snapshot")
            .bind(record.to_string())
            .fetch_one(&mut c)
            .await
            .unwrap();
        succeeded(&row, &owner, "native", "op", 9).unwrap();
        assert!(succeeded(&row, &owner, "other", "op", 9).is_err());
        assert!(succeeded(&row, &owner, "native", "op", 3).is_err());
    }
    record["historical_joined"] = json!(true);
    let row = sqlx::query("SELECT ? AS child_snapshot")
        .bind(record.to_string())
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert!(succeeded(&row, &owner, "native", "op", 9).is_err());
}
