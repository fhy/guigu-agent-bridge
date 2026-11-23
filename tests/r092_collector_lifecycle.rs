#![cfg(feature = "test-support")]

use std::{path::Path, time::Duration};

use guigu_agent_bridge::offline_identity::{CollectorRequest, collect_from_executable};
use tokio::time::{sleep, timeout};

fn request(root: &Path) -> CollectorRequest {
    CollectorRequest {
        homeserver: "http://127.0.0.1:1".into(),
        user_id: "@synthetic:example.test".into(),
        device_id: "SYNTHETIC".into(),
        access_token: "must-not-reach-unverified-child".into(),
        store: root.join("must-not-open"),
        passphrase: None,
    }
}

fn fixture(root: &Path, frame: Option<&str>) -> std::path::PathBuf {
    let source = root.join("fixture.c");
    let executable = root.join("fixture");
    let frame = serde_json::to_string(frame.unwrap_or("")).unwrap();
    let pid = serde_json::to_string(&root.join("pid")).unwrap();
    let leak = serde_json::to_string(&root.join("received")).unwrap();
    std::fs::write(
        &source,
        format!(
            r#"
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
int main(void) {{
    FILE *pid = fopen({pid}, "w");
    fprintf(pid, "%d", getpid());
    fclose(pid);
    const char frame[] = {frame};
    if (sizeof(frame) > 1) {{
        uint32_t length = htonl(sizeof(frame) - 1);
        if (write(1, &length, 4) != 4) return 1;
        if (write(1, frame, sizeof(frame) - 1) < 0) return 1;
    }}
    int output = open({leak}, O_WRONLY | O_CREAT | O_EXCL, 0600);
    char buffer[4096];
    ssize_t received;
    while ((received = read(0, buffer, sizeof(buffer))) > 0) {{
        if (write(output, buffer, received) != received) return 1;
    }}
    sleep(60);
    return 0;
}}
"#
        ),
    )
    .unwrap();
    let output = std::process::Command::new("cc")
        .args(["-Wall", "-Wextra", "-Werror"])
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    executable
}

async fn child_pid(root: &Path) -> u32 {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(root.join("pid"))
                && let Ok(pid) = text.parse()
            {
                break pid;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn assert_reaped(pid: u32, root: &Path) {
    timeout(Duration::from_secs(5), async {
        while Path::new(&format!("/proc/{pid}")).exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!root.join("must-not-open").exists());
    if let Ok(received) = std::fs::read(root.join("received")) {
        assert!(received.is_empty());
    }
}

#[tokio::test]
async fn parent_setup_timeout_reaps_only_its_child_without_delivering_credentials() {
    let root = tempfile::Builder::new()
        .prefix("r092-parent-timeout-")
        .tempdir_in("/tmp")
        .unwrap();
    let executable = fixture(root.path(), None);
    let task = tokio::spawn(collect_from_executable(request(root.path()), executable));
    let pid = child_pid(root.path()).await;
    assert!(
        timeout(Duration::from_secs(8), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_reaped(pid, root.path()).await;
}

#[tokio::test]
async fn cancelling_parent_reaps_owned_child_before_setup_deadline() {
    let root = tempfile::Builder::new()
        .prefix("r092-parent-cancel-")
        .tempdir_in("/tmp")
        .unwrap();
    let executable = fixture(root.path(), None);
    let task = tokio::spawn(collect_from_executable(request(root.path()), executable));
    let pid = child_pid(root.path()).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_reaped(pid, root.path()).await;
}

#[tokio::test]
async fn malformed_ready_is_refused_without_go_and_child_is_reaped() {
    let root = tempfile::Builder::new()
        .prefix("r092-parent-ready-")
        .tempdir_in("/tmp")
        .unwrap();
    let executable = fixture(root.path(), Some("{}"));
    let task = tokio::spawn(collect_from_executable(request(root.path()), executable));
    let pid = child_pid(root.path()).await;
    assert!(
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_reaped(pid, root.path()).await;
}
