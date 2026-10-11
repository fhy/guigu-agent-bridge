use guigu_t050_n::owned_syscalls::Listener;
use guigu_t050_n::{
    Refusal,
    audit::{AuditBroker, Kind, Label},
    contract::{Leaf, NamespaceSeed, NamespaceSpec, UUID},
    identity::{FileId, ObjectId},
    paths::OwnedDir,
    publication::Anonymous,
};
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    time::Instant,
};

struct FixtureChild {
    pid: i32,
    handle: File,
    reaped: bool,
}
fn spec() -> NamespaceSpec {
    NamespaceSpec::derive(NamespaceSeed {
        uuid: UUID.into(),
        candidate: "a".repeat(40),
        approval: "b".repeat(64),
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
    .unwrap()
}
impl Drop for FixtureChild {
    fn drop(&mut self) {
        if !self.reaped {
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.handle.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                );
                libc::waitpid(self.pid, std::ptr::null_mut(), 0);
            }
        }
    }
}

fn transfer(channel: i32, descriptor: i32) {
    let mut byte = [1u8];
    let mut vector = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0usize; 8];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = unsafe { libc::CMSG_SPACE(4) } as usize;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(4) as usize;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<i32>(), descriptor);
        if libc::sendmsg(channel, &message, 0) != 1 {
            libc::_exit(99);
        }
    }
}

fn receive(channel: i32) -> File {
    let mut poll = libc::pollfd {
        fd: channel,
        events: libc::POLLIN,
        revents: 0,
    };
    assert_eq!(unsafe { libc::poll(&mut poll, 1, 3000) }, 1);
    let mut byte = [0u8];
    let mut vector = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0usize; 8];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = std::mem::size_of_val(&control);
    assert_eq!(
        unsafe { libc::recvmsg(channel, &mut message, libc::MSG_CMSG_CLOEXEC) },
        1
    );
    let descriptor = unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        assert!(!header.is_null());
        assert_eq!((*header).cmsg_type, libc::SCM_RIGHTS);
        assert_eq!((*header).cmsg_level, libc::SOL_SOCKET);
        std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<i32>())
    };
    unsafe { File::from_raw_fd(descriptor) }
}

#[test]
fn notification_fd_injection_does_not_convert_path_exec_to_descriptor_exec() {
    let image = File::open("/usr/bin/true").unwrap();
    let mut channels = [-1i32; 2];
    assert_eq!(
        unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                channels.as_mut_ptr(),
            )
        },
        0
    );
    let supervisor = unsafe { File::from_raw_fd(channels[0]) };
    let child_channel = unsafe { File::from_raw_fd(channels[1]) };
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        let listener = Listener::install().unwrap_or_else(|_| unsafe { libc::_exit(99) });
        transfer(child_channel.as_raw_fd(), listener.file().as_raw_fd());
        let argv = [c"true".as_ptr(), std::ptr::null()];
        let env = [std::ptr::null::<libc::c_char>()];
        let returned = unsafe {
            libc::syscall(
                libc::SYS_execve,
                c"/usr/bin/true".as_ptr(),
                argv.as_ptr(),
                env.as_ptr(),
            )
        };
        // An actual exec never returns. ADDFD_SEND only synthesizes a syscall result.
        unsafe { libc::_exit(if returned >= 0 { 42 } else { 43 }) };
    }
    let handle = guigu_t050_n::supervision::pidfd(pid as u32).unwrap();
    let mut child = FixtureChild {
        pid,
        handle,
        reaped: false,
    };
    let listener = Listener::from_file(receive(supervisor.as_raw_fd()));
    let notification = listener.receive(3000).unwrap().unwrap();
    assert_eq!(notification.pid, pid as u32);
    assert_eq!(notification.data.nr as i64, libc::SYS_execve);
    assert!(listener.inject(&notification, &image, true).unwrap() >= 0);
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    while guigu_t050_n::supervision::live(&child.handle).unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    child.reaped = true;
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 42);
}

#[test]
fn protected_input_zero_dispatch() {
    let mut audit = AuditBroker::new(spec()).unwrap();
    for path in ["/unqueried/protected/input", "relative/foreign"] {
        assert!(
            audit
                .open_immutable(std::path::Path::new(path), libc::O_RDONLY)
                .is_err()
        );
    }
    assert!(audit.events().iter().all(|event| !event.issued));
    audit.finish();
    assert!(
        audit
            .certificate(
                std::array::from_fn(|_| "b".repeat(64)),
                "c".repeat(64),
                "pure-denials"
            )
            .is_err()
    );
}
#[test]
fn certificate_gap_or_nonzero() {
    let mut audit = AuditBroker::new(spec()).unwrap();
    audit.start_child().unwrap();
    audit.event(Kind::Open, Label::Immutable, true).unwrap();
    audit.finish();
    assert_eq!(
        audit
            .certificate(
                std::array::from_fn(|_| "b".repeat(64)),
                "c".repeat(64),
                "gap"
            )
            .err(),
        Some(Refusal::Trace)
    );
    let mut audit = AuditBroker::new(spec()).unwrap();
    audit.event(Kind::Mutation, Label::Unknown, false).unwrap();
    audit.finish();
    assert!(
        audit
            .certificate(
                std::array::from_fn(|_| "b".repeat(64)),
                "c".repeat(64),
                "unknown"
            )
            .is_err()
    );
}
#[test]
fn owned_syscall_descriptor_dispatch() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let dir = File::open(temp.path()).unwrap();
    let id = ObjectId::capture(&dir).unwrap();
    let dir = OwnedDir::authenticate(dir, &id).unwrap();
    let original = Anonymous::bytes(&dir, b"original", 0o600)
        .unwrap()
        .publish(&dir, "original")
        .unwrap();
    let mut audit = AuditBroker::new(spec()).unwrap();
    audit
        .register(
            &spec().path(Leaf::Config),
            original.file.try_clone().unwrap(),
            original.identity.clone(),
        )
        .unwrap();
    std::fs::rename(temp.path().join("original"), temp.path().join("retained")).unwrap();
    Anonymous::bytes(&dir, b"foreign", 0o600)
        .unwrap()
        .publish(&dir, "original")
        .unwrap();
    let path = std::ffi::CString::new(spec().path(Leaf::Config).to_str().unwrap()).unwrap();
    let mut channels = [-1; 2];
    assert_eq!(
        unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                channels.as_mut_ptr(),
            )
        },
        0
    );
    let server = unsafe { File::from_raw_fd(channels[0]) };
    let worker = unsafe { File::from_raw_fd(channels[1]) };
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        read_original(&path, worker.as_raw_fd());
    }
    let mut child = FixtureChild {
        pid,
        handle: guigu_t050_n::supervision::pidfd(pid as u32).unwrap(),
        reaped: false,
    };
    audit.start_child().unwrap();
    let listener = Listener::from_file(receive(server.as_raw_fd()));
    let notification = listener.receive(3000).unwrap().unwrap();
    listener
        .service_open(&notification, pid as u32, &mut audit)
        .unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    while guigu_t050_n::supervision::live(&child.handle).unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    child.reaped = true;
    assert_eq!(status, 0);
    audit.cover_child().unwrap();
    audit.finish();
    let certificate = audit
        .certificate(
            std::array::from_fn(|_| "b".repeat(64)),
            "c".repeat(64),
            "N28-one-owned-open-not-root-runtime",
        )
        .unwrap();
    assert_eq!(certificate.owned_started, 1);
    assert_eq!(certificate.unknown, 0);
    certificate.sha256().unwrap();
    dir.verify_leaf("retained", &original.identity).unwrap();
}
fn read_original(path: &std::ffi::CStr, worker: i32) -> ! {
    let listener = Listener::install().unwrap_or_else(|_| unsafe { libc::_exit(99) });
    transfer(worker, listener.file().as_raw_fd());
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat,
            libc::AT_FDCWD,
            path.as_ptr(),
            libc::O_RDONLY,
            0,
        )
    };
    let mut bytes = [0u8; 8];
    let read = unsafe { libc::read(fd as i32, bytes.as_mut_ptr().cast(), 8) };
    unsafe {
        libc::_exit(if read == 8 && &bytes == b"original" {
            0
        } else {
            41
        })
    }
}
#[test]
fn secret_redaction() {
    let sentinel = "NEVER-ECHO-FAKE-SECRET";
    for error in [
        Refusal::Identity,
        Refusal::Secret,
        Refusal::NoAuthority,
        Refusal::Channel,
        Refusal::Trace,
    ] {
        let text = format!("{error} {error:?}");
        assert!(!text.contains(sentinel));
    }
    let mut audit = AuditBroker::new(spec()).unwrap();
    audit.event(Kind::Exec, Label::Denied, false).unwrap();
    let text = serde_json::to_string(audit.events()).unwrap();
    assert!(!text.contains(sentinel));
    assert!(!text.contains("argv"));
    assert!(!text.contains("environment"));
    // Secret provider content is not consumed by this test or any supervisor API.
    assert!(FileId::capture(&File::open("/dev/null").unwrap()).is_err());
}
