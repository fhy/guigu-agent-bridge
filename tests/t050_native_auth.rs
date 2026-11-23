//! Native Qwen's observed API-key method, without changing unknown-method policy.
use guigu_agent_bridge::acp::{AcpClient, AcpLimits};
use guigu_agent_bridge::models::EndpointAddress;

fn address(scenario: &str) -> EndpointAddress {
    let executable = std::env::current_exe().unwrap();
    let binary = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/acp-mock-backend");
    EndpointAddress::Acp {
        command: binary.to_string_lossy().into_owned(),
        args: vec![scenario.into()],
    }
}

#[tokio::test]
async fn native_openai_method_authenticates_before_session() {
    let client = AcpClient::connect(
        &address("need-auth-openai"),
        "native-auth-fixture",
        None,
        AcpLimits::default(),
    )
    .await
    .unwrap();
    assert!(client.new_session("/tmp").await.is_ok());
    assert_eq!(client.shutdown().await, Some(0));
}

#[tokio::test]
async fn native_resume_keeps_requested_identity_without_an_echo() {
    let client = AcpClient::connect(
        &address("resume-native"),
        "native-resume-fixture",
        None,
        AcpLimits::default(),
    )
    .await
    .unwrap();
    let session = client.new_session("/tmp").await.unwrap();
    assert_eq!(
        client.resume_session(&session, "/tmp").await.unwrap(),
        session
    );
    assert_eq!(client.shutdown().await, Some(0));
}

#[tokio::test]
async fn malformed_or_foreign_resume_result_is_still_refused() {
    for scenario in ["resume-foreign", "resume-null", "resume-array"] {
        let client = AcpClient::connect(
            &address(scenario),
            "native-resume-fixture",
            None,
            AcpLimits::default(),
        )
        .await
        .unwrap();
        let session = client.new_session("/tmp").await.unwrap();
        assert!(client.resume_session(&session, "/tmp").await.is_err());
        assert_eq!(client.shutdown().await, Some(0));
    }
}
