use super::Failure;
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

const UNIT: &str = "guigu-agent-bridge-observer.service";

pub(super) async fn check(listeners: &[SocketAddr]) -> Result<(), Failure> {
    let show = query(&["show", UNIT, "--no-pager"]).await?;
    validate_show(&show)?;
    let jobs = query(&["list-jobs", "--no-legend", "--no-pager"]).await?;
    if !jobs.trim().is_empty() {
        return Err(Failure::Busy);
    }
    let reverse = query(&[
        "list-dependencies",
        "--reverse",
        "--plain",
        "--no-legend",
        UNIT,
    ])
    .await?;
    if reverse
        .lines()
        .any(|line| !matches!(line.trim(), "" | UNIT))
    {
        return Err(Failure::Busy);
    }
    for address in listeners {
        let listener = std::net::TcpListener::bind(address).map_err(|_| Failure::Busy)?;
        drop(listener);
    }
    no_other_bridge()?;
    Ok(())
}

fn no_other_bridge() -> Result<(), Failure> {
    use std::os::unix::fs::MetadataExt;
    for entry in std::fs::read_dir("/proc").map_err(|_| Failure::Busy)? {
        let entry = entry.map_err(|_| Failure::Busy)?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(Failure::Busy),
        };
        if meta.uid() != unsafe { libc::geteuid() } {
            continue;
        }
        match std::fs::read_link(entry.path().join("exe")) {
            Ok(executable)
                if executable.file_name().is_some_and(|name| {
                    name.to_string_lossy().starts_with("guigu-agent-bridge")
                }) =>
            {
                return Err(Failure::Busy);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Failure::Busy),
        }
    }
    Ok(())
}

async fn query(arguments: &[&str]) -> Result<String, Failure> {
    let mut command = tokio::process::Command::new("/usr/bin/systemctl");
    command.arg("--user").args(arguments).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| Failure::Busy)?
        .map_err(|_| Failure::Busy)?;
    if !output.status.success() {
        return Err(Failure::Busy);
    }
    String::from_utf8(output.stdout).map_err(|_| Failure::Busy)
}

pub(super) fn validate_show(show: &str) -> Result<(), Failure> {
    let fields = show
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<BTreeMap<_, _>>();
    for (key, expected) in [
        ("LoadState", "masked"),
        ("UnitFileState", "masked-runtime"),
        ("ActiveState", "inactive"),
        ("SubState", "dead"),
        ("MainPID", "0"),
        ("ControlPID", "0"),
        ("FragmentPath", "/dev/null"),
        ("Job", ""),
        ("TriggeredBy", ""),
    ] {
        if fields.get(key).copied() != Some(expected) {
            return Err(Failure::Busy);
        }
    }
    Ok(())
}
