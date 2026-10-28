use std::os::unix::fs::PermissionsExt;

use crate::{
    offline_identity::CollectorRequest,
    offline_production_migration::{
        Failure, evidence::Evidence, identity_records::IdentityFailure, records,
        test_fixture::Fixture, verification, version2, version2_records as graph,
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn failed_rooted_capture_retains_redacted_audit_and_pending_binding_without_retry() {
    let mut fixture = Fixture::new().await;
    fixture.request.target_schema = 25;
    let mut release: crate::offline_production_migration::evidence::Provenance =
        records::load(&fixture.request.release_provenance).unwrap();
    release.migration_set_sha256 =
        crate::offline_production_migration::evidence::migration_digest_through(25);
    std::fs::write(
        &fixture.request.release_provenance,
        serde_json::to_vec(&release).unwrap(),
    )
    .unwrap();
    version2::execute(fixture.request.clone(), false)
        .await
        .unwrap();
    let req = &fixture.request;
    let provenance = Evidence::read(req).unwrap();
    let root = graph::root(req, &provenance, 25).unwrap();
    let mut connection = verification::open(&req.database, false).await.unwrap();
    let schema = super::super::schema_binding(&mut connection, req, &root)
        .await
        .unwrap();
    let store = fixture.root.path().join("unopened-store");
    std::fs::create_dir(&store).unwrap();
    std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        store.join("synthetic.db"),
        b"not opened by failing observation",
    )
    .unwrap();
    std::fs::set_permissions(
        store.join("synthetic.db"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/account/whoami"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let capture = CollectorRequest {
        homeserver: server.uri(),
        user_id: "@synthetic:example.test".into(),
        device_id: "SYNTHETIC".into(),
        access_token: "never-retain-this-synthetic-token".into(),
        store,
        passphrase: None,
    };
    assert!(matches!(
        super::super::capture_identity(req, &root, &schema, capture, &mut connection).await,
        Err(Failure::Preflight)
    ));
    let failure_path = req.manifest_dir.join("identity-failure-v1.json");
    let audit: IdentityFailure = graph::load(&failure_path).unwrap();
    let intent: super::super::IdentityIntent =
        graph::load(&req.manifest_dir.join("identity-intent-v1.json")).unwrap();
    assert_eq!(
        audit.intent_digest,
        records::encoded_digest(&intent).unwrap()
    );
    assert_eq!(audit.stage, "before-observation");
    assert_eq!(audit.category, "identity-preflight");
    let bytes = std::fs::read(&failure_path).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("never-retain-this-synthetic-token"));
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(phase, "identity_pending");
    assert!(!req.manifest_dir.join("identity-evidence-v1.json").exists());
    assert!(matches!(
        super::super::execute(req.clone(), false).await,
        Err(Failure::ManualRequired)
    ));
    assert_eq!(std::fs::read(failure_path).unwrap(), bytes);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
