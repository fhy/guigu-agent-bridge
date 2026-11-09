//! Model a prior boot in disposable classified records; current /proc stays real.
use super::history_fixture;
use crate::storage::recovery_eligibility::{EffectDomain, RowIdentity, writer};

pub(super) const OLD_BOOT: &str = "11111111-1111-4111-8111-111111111111";

// Atomicity fixtures model exclusion; the AppRuntime fixture uses real capture.
pub(super) fn modeled_proof(owner: super::state::Owner) -> super::process::Exclusion {
    let mut proof = super::process::Exclusion::modeled(owner);
    proof.kind = "current-scoped-exclusion-v2".into();
    proof.boot = Some(super::process::boot::BootObservation {
        current: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap()
            .trim()
            .into(),
        absence: "different-boot".into(),
    });
    assert!(proof.boot.as_ref().unwrap().valid_for(&proof.owner));
    proof
}

pub(super) async fn check_record(pool: &sqlx::SqlitePool) {
    let (boot, start, barrier, record): (String, i64, String, String) = sqlx::query_as(
        "SELECT boot_id,proc_starttime,barrier_state,child_snapshot FROM runtime_owner_evidence WHERE instance_token='unknown' AND is_current=1"
    ).fetch_one(pool).await.unwrap();
    assert_eq!(boot, OLD_BOOT);
    assert_eq!(start, 9000000000);
    assert_eq!(barrier, "open");
    let record: serde_json::Value = serde_json::from_str(&record).unwrap();
    assert_eq!(record["historical_joined"], false);
    assert_eq!(record["source"]["owner"]["boot"], OLD_BOOT);
    assert_eq!(record["exclusion"]["owner"]["boot"], OLD_BOOT);
    assert_eq!(record["exclusion"]["boot"]["absence"], "different-boot");
    assert_eq!(record["exclusion"]["kind"], "current-scoped-exclusion-v2");
    assert_ne!(record["exclusion"]["boot"]["current"], OLD_BOOT);
    let unchanged: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE instance_token='unknown' AND boot_id=? AND proc_starttime=9000000000 AND evidence_revision<=38")
        .bind(OLD_BOOT).fetch_one(pool).await.unwrap();
    assert_eq!(unchanged, 38);
}

pub(super) async fn previous_boot(pool: &sqlx::SqlitePool) {
    let scope = history_fixture::scope(pool).await;
    let mut tx = writer::begin(pool, Some(&scope)).await.unwrap();
    let keys: Vec<String> = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token='unknown' ORDER BY evidence_revision")
        .fetch_all(&mut *tx).await.unwrap();
    assert_eq!(keys.len(), 38);
    for (index, key) in keys.into_iter().enumerate() {
        let id = RowIdentity {
            table: "runtime_owner_evidence".into(),
            key,
        };
        let mutation = writer::prepare_mutation(&mut tx, Some(&scope), &id)
            .await
            .unwrap();
        sqlx::query("UPDATE runtime_owner_evidence SET boot_id=?,proc_starttime=9000000000 WHERE instance_token='unknown' AND evidence_revision=?")
            .bind(OLD_BOOT).bind(index as i64 + 1).execute(&mut *tx).await.unwrap();
        writer::complete_mutation(&mut tx, Some(&scope), mutation)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let context = crate::storage::recovery_eligibility::readback_nonadmin(pool, true)
        .await
        .unwrap();
    let mut c = pool.acquire().await.unwrap();
    crate::storage::recovery_eligibility::require_lineage(
        &mut c,
        &context,
        EffectDomain::Matrix,
        &writer::text_identity("runtime_instances", "unknown").unwrap(),
    )
    .await
    .unwrap();
}
