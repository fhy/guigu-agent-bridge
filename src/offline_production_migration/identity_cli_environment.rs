use std::os::{fd::FromRawFd, unix::fs::PermissionsExt};

pub(super) async fn isolated_rooted_test(scenario: &str) -> bool {
    if std::env::var_os("R092_ROOTED_NAMESPACE").is_some() {
        return false;
    }
    let home = std::path::PathBuf::from(std::env::var_os("HOME").unwrap());
    let parent = home
        .parent()
        .filter(|path| *path != std::path::Path::new("/"))
        .unwrap();
    let alias = tempfile::Builder::new()
        .prefix("r092-home-alias-")
        .tempdir()
        .unwrap();
    let output = tokio::process::Command::new("/usr/bin/unshare")
        .args(["--user", "--map-root-user", "--pid", "--fork", "--kill-child", "--mount", "--mount-proc", "--propagation", "private", "/bin/sh", "-c",
            "mount --bind \"$1\" \"$2\" && mount -t tmpfs -o mode=755 tmpfs \"$3\" && mkdir -p \"$1\" && mount --bind \"$2\" \"$1\" && exec \"$4\" --exact offline_production_migration::identity_capture::tests::failures::rooted_identity_cli_child_reports_binding_result --test-threads=1", "r092-isolated-test"])
        .arg(&home).arg(alias.path()).arg(parent).arg(std::env::current_exe().unwrap())
        .env("R092_ROOTED_NAMESPACE", "1").env("R092_ROOTED_SCENARIO", scenario).kill_on_drop(true).output().await.unwrap();
    assert!(
        output.status.success(),
        "namespace child status {:?}: {}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

pub(super) struct PrivateManager {
    pub(super) process: std::process::Child,
    manager: Option<std::os::fd::OwnedFd>,
    pub(super) runtime: std::path::PathBuf,
    pub(super) bus: String,
    _root: tempfile::TempDir,
}

impl PrivateManager {
    pub(super) async fn start() -> Self {
        use std::os::unix::process::CommandExt;
        let root = tempfile::Builder::new()
            .prefix("r092-manager-")
            .tempdir_in(std::env::var_os("HOME").unwrap())
            .unwrap();
        let runtime = root.path().join("runtime");
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let home = root.path().join("home");
        let units = runtime.join("systemd/user");
        std::fs::create_dir_all(&units).unwrap();
        std::fs::create_dir(&home).unwrap();
        for directory in [&runtime, &runtime.join("systemd"), &units, &home] {
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::os::unix::fs::symlink(
            "/dev/null",
            units.join("guigu-agent-bridge-observer.service"),
        )
        .unwrap();
        std::fs::write(
            units.join("r092.target"),
            "[Unit]\nDescription=R092 isolated target\n",
        )
        .unwrap();
        let process = std::process::Command::new("/usr/bin/dbus-run-session")
            .args(["--", "/bin/sh", "-c", "printf '%s' \"$DBUS_SESSION_BUS_ADDRESS\" > \"$XDG_RUNTIME_DIR/bus-address\"; printf '%s' \"$$\" > \"$XDG_RUNTIME_DIR/manager-pid\"; exec /usr/lib/systemd/systemd --user --unit=r092.target"])
            .env_clear()
            .env("HOME", &home)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("SYSTEMD_UNIT_PATH", &units)
            .env("PATH", "/usr/bin:/bin")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(root.path().join("manager.log")).unwrap())
            .process_group(0)
            .spawn()
            .unwrap();
        let mut manager = Self {
            process,
            manager: None,
            runtime,
            bus: String::new(),
            _root: root,
        };
        manager.ready().await;
        manager
    }

    async fn ready(&mut self) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut last_status = String::new();
        while tokio::time::Instant::now() < deadline {
            if self.manager.is_none()
                && let Some(pid) = std::fs::read_to_string(self.runtime.join("manager-pid"))
                    .ok()
                    .and_then(|value| value.parse::<i32>().ok())
            {
                let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
                assert!(descriptor >= 0, "private manager pidfd");
                self.manager =
                    Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(descriptor as i32) });
            }
            assert!(
                self.process.try_wait().unwrap().is_none(),
                "private manager exited"
            );
            if let Ok(bus) = std::fs::read_to_string(self.runtime.join("bus-address")) {
                let status = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    tokio::process::Command::new("/usr/bin/systemctl")
                        .args([
                            "--user",
                            "show",
                            "guigu-agent-bridge-observer.service",
                            "-p",
                            "LoadState",
                            "--value",
                        ])
                        .env_clear()
                        .env("XDG_RUNTIME_DIR", &self.runtime)
                        .env("DBUS_SESSION_BUS_ADDRESS", &bus)
                        .kill_on_drop(true)
                        .output(),
                )
                .await;
                if let Ok(Ok(status)) = status {
                    last_status = format!(
                        "{:?}: {} {}",
                        status.status.code(),
                        String::from_utf8_lossy(&status.stdout),
                        String::from_utf8_lossy(&status.stderr)
                    );
                    if status.status.success() && status.stdout == b"masked\n" {
                        self.bus = bus;
                        return;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!(
            "private manager readiness deadline: {last_status}; {}",
            std::fs::read_to_string(self._root.path().join("manager.log")).unwrap()
        );
    }
}

impl Drop for PrivateManager {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        if let Some(manager) = &self.manager {
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    manager.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                );
            }
        }
        unsafe {
            libc::kill(-(self.process.id() as i32), libc::SIGTERM);
        }
        for _attempt in 0..100 {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        unsafe {
            libc::kill(-(self.process.id() as i32), libc::SIGKILL);
        }
        let _ = self.process.wait();
    }
}
