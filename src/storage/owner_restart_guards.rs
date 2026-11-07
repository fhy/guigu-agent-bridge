use sqlx::SqliteConnection;

pub(super) async fn work_clear(connection: &mut SqliteConnection) -> Result<bool, sqlx::Error> {
    for (predicate, query) in [
        (
            "execution-leases",
            "SELECT count(*) FROM execution_leases WHERE state!='released'",
        ),
        (
            "continuations",
            "SELECT count(*) FROM task_continuations WHERE state NOT IN ('terminal','released')",
        ),
        (
            "work-queue",
            "SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','expired','superseded')",
        ),
        ("workspace-claims", "SELECT count(*) FROM workspace_claims"),
        (
            "component-leases",
            "SELECT count(*) FROM matrix_component_leases WHERE state IN ('active','unknown')",
        ),
        (
            "deliveries",
            "SELECT count(*) FROM deliveries d LEFT JOIN delivery_dispositions p USING(delivery_id) WHERE d.acknowledged_at IS NULL OR p.state IS NULL OR p.state!='terminal'",
        ),
        (
            "output-sets",
            "SELECT count(*) FROM acp_output_sets o WHERE state NOT IN ('delivered','cleaned') AND NOT (state='cleaning' AND segment_count>0 AND acknowledged_cursor=segment_count-1 AND segment_count=(SELECT count(*) FROM acp_output_delivery_segments d WHERE d.set_id=o.set_id) AND NOT EXISTS(SELECT 1 FROM acp_output_delivery_segments d WHERE d.set_id=o.set_id AND d.delivery_index>=o.segment_count) AND NOT EXISTS(SELECT 1 FROM acp_output_segments s WHERE s.set_id=o.set_id AND s.state NOT IN ('landed','cleaned'))) AND NOT (state='abandoned' AND total_bytes=0 AND total_hash='' AND segment_count=0 AND acknowledged_cursor=-1 AND NOT EXISTS(SELECT 1 FROM acp_output_segments s WHERE s.set_id=o.set_id) AND NOT EXISTS(SELECT 1 FROM acp_output_delivery_segments d WHERE d.set_id=o.set_id))",
        ),
        (
            "admissions",
            "SELECT count(*) FROM task_admissions a WHERE a.state!='terminal' AND NOT (a.state='recovery_needed' AND EXISTS(SELECT 1 FROM runtime_instances r WHERE r.instance_token=a.runtime_instance AND r.state='stopped') AND EXISTS(SELECT 1 FROM transport_receipts receipt WHERE receipt.transport='offline-admission-hold' AND receipt.task_id=a.task_id AND receipt.external_event_id=a.task_id AND receipt.receipt_kind='admin' AND receipt.route_kind='unknown-dispatch-hold-v1' AND receipt.state='rejected' AND receipt.result_code='unknown-preserved:'||a.runtime_instance||':'||a.revision) AND NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.task_id=a.task_id))",
        ),
        (
            "tasks",
            "SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT max(seq) FROM task_events WHERE task_id=t.task_id) WHERE (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')) AND NOT EXISTS(SELECT 1 FROM task_admissions a JOIN runtime_instances r ON r.instance_token=a.runtime_instance JOIN transport_receipts receipt ON receipt.task_id=a.task_id WHERE a.task_id=t.task_id AND a.state='recovery_needed' AND r.state='stopped' AND receipt.transport='offline-admission-hold' AND receipt.external_event_id=a.task_id AND receipt.receipt_kind='admin' AND receipt.route_kind='unknown-dispatch-hold-v1' AND receipt.state='rejected' AND receipt.result_code='unknown-preserved:'||a.runtime_instance||':'||a.revision AND NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.task_id=a.task_id))",
        ),
    ] {
        if sqlx::query_scalar::<_, i64>(query)
            .fetch_one(&mut *connection)
            .await?
            != 0
        {
            tracing::warn!(
                phase = "owner-controlled-stop",
                predicate,
                "controlled stop has unresolved work"
            );
            return Ok(false);
        }
    }
    Ok(true)
}
