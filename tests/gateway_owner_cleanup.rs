use guigu_agent_bridge::gateway::GatewayStore;
use guigu_agent_bridge::storage::BusinessStoreOwner;

#[tokio::test]
async fn owner_cleanup_claim_preserves_limit_and_reclaim_rules() {
    let path =
        std::env::temp_dir().join(format!("guigu-gateway-cleanup-{}.db", uuid::Uuid::now_v7()));
    let owner = BusinessStoreOwner::open(&path).expect("owner").facade();
    owner.execute(|connection| {
        connection.execute_batch("CREATE TABLE gateway_envelopes(envelope_id TEXT PRIMARY KEY, state TEXT NOT NULL, terminal_at TEXT, cleanup_owner TEXT, cleanup_revision INTEGER NOT NULL DEFAULT 0, cleanup_claimed_at TEXT); CREATE TABLE runtime_instances(instance_token TEXT PRIMARY KEY, state TEXT NOT NULL)").map_err(|error| guigu_agent_bridge::storage::StorageError::OwnerQuery(error.to_string()))
    }).expect("schema");
    owner.execute(|connection| {
        connection.execute("INSERT INTO runtime_instances VALUES ('stopped-owner','stopped'),('active-owner','active')", []).map_err(|error| guigu_agent_bridge::storage::StorageError::OwnerQuery(error.to_string()))?;
        connection.execute("INSERT INTO gateway_envelopes(envelope_id,state,terminal_at) VALUES ('one','terminal','2020-01-01'),('two','terminal','2020-01-01')", []).map_err(|error| guigu_agent_bridge::storage::StorageError::OwnerQuery(error.to_string()))
    }).expect("seed");
    let store = GatewayStore::new_owner(owner);
    assert_eq!(
        store
            .claim_cleanup("stopped-owner", "2026-01-01", 1)
            .await
            .expect("claim"),
        1
    );
    assert_eq!(
        store
            .claim_cleanup("stopped-owner", "2026-01-01", 1)
            .await
            .expect("reclaim"),
        1
    );
}
