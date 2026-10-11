use crate::{
    Refusal, Result,
    audit::{AuditBroker, Kind, Label},
    syscall_paths::*,
};
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
};

pub struct Listener {
    file: File,
}
impl Listener {
    /// Installs in the single-threaded owned child, before exec/application code.
    /// No tracing-disabled fallback. A filter inherits into every descendant.
    pub fn install() -> Result<Self> {
        if std::env::consts::ARCH != "x86_64" {
            return Err(Refusal::Unsupported);
        }
        let mut code = vec![
            stmt(0x20, 4),
            jump(0x15, 0xc000003e, 1, 0),
            stmt(0x06, 0x80000000),
            stmt(0x20, 0),
        ];
        for syscall in notify_syscalls() {
            code.push(jump(0x15, syscall as u32, 0, 1));
            code.push(stmt(0x06, 0x7fc00000));
        }
        // x32 syscall aliases must never bypass the native table.
        code.push(jump(0x45, 0x40000000, 0, 1));
        code.push(stmt(0x06, 0x80000000));
        code.push(stmt(0x06, 0x7fff0000));
        let program = libc::sock_fprog {
            len: code.len() as u16,
            filter: code.as_mut_ptr(),
        };
        if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
            return Err(Refusal::Unsupported);
        }
        let fd = unsafe { libc::syscall(libc::SYS_seccomp, 1, 8, &program) };
        if fd < 0 {
            return Err(Refusal::Unsupported);
        }
        Ok(Self {
            file: unsafe { File::from_raw_fd(fd as i32) },
        })
    }
    pub fn from_file(file: File) -> Self {
        Self { file }
    }
    pub fn file(&self) -> &File {
        &self.file
    }
    pub fn receive(&self, timeout_ms: i32) -> Result<Option<Notification>> {
        let mut poll = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, timeout_ms) };
        if ready < 0 {
            return Err(Refusal::Trace);
        }
        if ready == 0 {
            return Ok(None);
        }
        let mut notification = Notification::default();
        if unsafe { libc::ioctl(self.file.as_raw_fd(), RECV, &mut notification) } != 0 {
            return Err(Refusal::Trace);
        }
        Ok(Some(notification))
    }
    pub fn valid(&self, id: u64) -> bool {
        unsafe { libc::ioctl(self.file.as_raw_fd(), VALID, &id) == 0 }
    }
    pub fn inject(&self, notification: &Notification, file: &File, cloexec: bool) -> Result<i32> {
        if !self.valid(notification.id) {
            return Err(Refusal::Trace);
        }
        let request = AddFd {
            id: notification.id,
            flags: 2,
            srcfd: file.as_raw_fd() as u32,
            newfd: 0,
            newfd_flags: if cloexec { libc::O_CLOEXEC as u32 } else { 0 },
        };
        let fd = unsafe { libc::ioctl(self.file.as_raw_fd(), ADD_FD, &request) };
        if fd < 0 {
            return Err(Refusal::Unsupported);
        }
        Ok(fd)
    }
    pub fn refuse(&self, notification: &Notification, errno: i32) -> Result<()> {
        let response = Response {
            id: notification.id,
            val: 0,
            error: -errno,
            flags: 0,
        };
        if unsafe { libc::ioctl(self.file.as_raw_fd(), SEND, &response) } != 0 {
            return Err(Refusal::Trace);
        }
        Ok(())
    }
    pub fn service_open(
        &self,
        notification: &Notification,
        owned_pid: u32,
        audit: &mut AuditBroker,
    ) -> Result<()> {
        if notification.pid != owned_pid
            || notification.data.arch != 0xc000003e
            || !self.valid(notification.id)
        {
            return Err(Refusal::Process);
        }
        let (pointer, flags) = match notification.data.nr as i64 {
            libc::SYS_open => (notification.data.args[0], notification.data.args[1] as i32),
            libc::SYS_openat if notification.data.args[0] as i32 == libc::AT_FDCWD => {
                (notification.data.args[1], notification.data.args[2] as i32)
            }
            _ => {
                audit.event(Kind::Mutation, Label::Unknown, false)?;
                self.refuse(notification, libc::EPERM)?;
                return Err(Refusal::Trace);
            }
        };
        let path = owned_path(owned_pid, pointer)?;
        match audit.open_immutable(&path, flags) {
            Ok(file) => {
                self.inject(notification, &file, flags & libc::O_CLOEXEC != 0)?;
                Ok(())
            }
            Err(error) => {
                self.refuse(notification, libc::EPERM)?;
                Err(error)
            }
        }
    }
}
fn stmt(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}
fn jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

/// Reject kernel feature absence before any operational publication.
pub fn require_features(anonymous: bool, notification_fd: bool, pidfd: bool) -> Result<()> {
    if !anonymous || !notification_fd || !pidfd {
        return Err(Refusal::Unsupported);
    }
    Ok(())
}

/// Runs only in the single-threaded gated fork. No pathname execution or fallback.
pub(crate) fn gated_exec(
    image: i32,
    channel: i32,
    parent_channel: i32,
    argv: &[*const libc::c_char],
    worker_channels: Option<(i32, i32)>,
) -> ! {
    let environment = [std::ptr::null::<libc::c_char>()];
    unsafe {
        libc::close(parent_channel);
        let mut grant = [0u8];
        if libc::recv(channel, grant.as_mut_ptr().cast(), 1, 0) != 1 || grant[0] != b'G' {
            libc::_exit(126);
        }
        if let Some((worker, provider)) = worker_channels
            && (libc::dup2(worker, 3) < 0 || libc::dup2(provider, 4) < 0)
        {
            libc::_exit(126);
        }
        libc::syscall(
            libc::SYS_execveat,
            image,
            c"".as_ptr(),
            argv.as_ptr(),
            environment.as_ptr(),
            libc::AT_EMPTY_PATH,
        );
        libc::_exit(126);
    }
}
