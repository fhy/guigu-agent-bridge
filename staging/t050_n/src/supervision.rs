pub use crate::secret_channel::{ExecutionBinding, LaunchWitness};
use crate::{
    Refusal, Result,
    identity::{FileId, RetainedImage},
    journal::Outcome,
};
use serde::{Deserialize, Serialize};
use std::{
    ffi::CString,
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    os::unix::net::UnixStream,
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildCertificate {
    pub pid: u32,
    pub uid: u32,
    pub start_ticks: u64,
    pub image: FileId,
    pub nonce: String,
    pub argv_digest: String,
    pub namespace: Option<ExecutionBinding>,
}
pub fn start_ticks(pid: u32) -> Result<u64> {
    let path = format!("/proc/{pid}/stat");
    let text = std::fs::read_to_string(path).map_err(|_| Refusal::Process)?;
    let end = text.rfind(')').ok_or(Refusal::Process)?;
    text[end + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or(Refusal::Process)?
        .parse()
        .map_err(|_| Refusal::Process)
}
pub fn pidfd(pid: u32) -> Result<File> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(Refusal::Unsupported);
    }
    Ok(unsafe { File::from_raw_fd(fd as i32) })
}
pub fn live(file: &File) -> Result<bool> {
    let mut poll = libc::pollfd {
        fd: file.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let count = unsafe { libc::poll(&mut poll, 1, 0) };
    if count < 0 {
        return Err(Refusal::Process);
    }
    Ok(count == 0)
}
pub fn validate_peer(
    expected: &ChildCertificate,
    pid: u32,
    uid: u32,
    ticks: u64,
    image: &FileId,
    nonce: &str,
) -> Result<()> {
    if expected.pid != pid
        || expected.uid != uid
        || expected.start_ticks != ticks
        || expected.image != *image
        || expected.nonce != nonce
    {
        return Err(Refusal::Process);
    }
    Ok(())
}
pub struct OwnedChild {
    certificate: ChildCertificate,
    handle: File,
    reaped: bool,
    gate: Option<UnixStream>,
    raw_status: Option<i32>,
    channels: Option<crate::secret_channel::WorkerChannels>,
    refused: std::cell::Cell<bool>,
}

impl OwnedChild {
    pub fn reserve_optional(
        image: Option<&RetainedImage>,
        argv: &[String],
        nonce: &str,
        expected: &FileId,
        witness: &LaunchWitness<'_>,
    ) -> Result<Self> {
        Self::reserve(
            image.ok_or(Refusal::Identity)?,
            argv,
            nonce,
            expected,
            witness,
        )
    }
    /// The launch path never opens an executable by name. Expected inode/hash is
    /// checked again immediately before fork; execveat consumes the retained FD.
    pub fn reserve(
        image: &RetainedImage,
        argv: &[String],
        nonce: &str,
        expected: &FileId,
        witness: &LaunchWitness<'_>,
    ) -> Result<Self> {
        Self::reserve_with_channels(image, argv, nonce, expected, witness, None)
    }
    pub fn reserve_with_channels(
        image: &RetainedImage,
        argv: &[String],
        nonce: &str,
        expected: &FileId,
        witness: &LaunchWitness<'_>,
        channels: Option<&crate::secret_channel::WorkerChannels>,
    ) -> Result<Self> {
        if !crate::contract::hex(nonce, 64)
            || argv.is_empty()
            || argv.len() > 16
            || expected != image.identity()
        {
            return Err(Refusal::Identity);
        }
        witness.validate()?;
        image.recheck()?;
        witness.validate_argv(argv)?;
        let channels = channels.map(|value| value.duplicate()).transpose()?;
        let executable = crate::secret_channel::duplicate(image.file().as_raw_fd())?;
        expected.recheck(&executable)?;
        let arguments: Vec<CString> = argv
            .iter()
            .map(|v| CString::new(v.as_str()).map_err(|_| Refusal::Contract))
            .collect::<Result<_>>()?;
        let mut pointers: Vec<_> = arguments.iter().map(|v| v.as_ptr()).collect();
        pointers.push(std::ptr::null());
        let (gate, child_gate) = UnixStream::pair().map_err(|_| Refusal::Channel)?;
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(Refusal::Process);
        }
        if pid == 0 {
            crate::owned_syscalls::gated_exec(
                executable.as_raw_fd(),
                child_gate.as_raw_fd(),
                gate.as_raw_fd(),
                &pointers,
                channels.as_ref().map(|value| value.descriptors()),
            );
        }
        drop(child_gate);
        let (handle, ticks) = match child_identity(pid as u32) {
            Ok(identity) => identity,
            Err(error) => {
                drop(gate);
                unsafe {
                    libc::waitpid(pid, std::ptr::null_mut(), 0);
                }
                return Err(error);
            }
        };
        Ok(Self {
            certificate: ChildCertificate {
                pid: pid as u32,
                uid: expected.object.uid,
                start_ticks: ticks,
                image: expected.clone(),
                nonce: nonce.into(),
                argv_digest: crate::digest(
                    &serde_json::to_vec(argv).map_err(|_| Refusal::Contract)?,
                ),
                namespace: Some(Self::execution_binding(witness, channels.as_ref())),
            },
            handle,
            reaped: false,
            gate: Some(gate),
            raw_status: None,
            channels,
            refused: std::cell::Cell::new(false),
        })
    }
    /// Explicit staging-only FD launcher: grant exactly once after durable ownership.
    pub fn grant(&mut self, image: &RetainedImage, witness: &LaunchWitness<'_>) -> Result<()> {
        self.authenticate_reservation(image, witness)?;
        let gate = self.gate.as_ref().ok_or(Refusal::NoAuthority)?;
        if unsafe {
            libc::send(
                gate.as_raw_fd(),
                b"G".as_ptr().cast(),
                1,
                libc::MSG_NOSIGNAL,
            )
        } != 1
        {
            return Err(Refusal::Channel);
        }
        self.gate.take();
        Ok(())
    }
    fn execution_binding(
        witness: &LaunchWitness<'_>,
        channels: Option<&crate::secret_channel::WorkerChannels>,
    ) -> ExecutionBinding {
        let mut binding = witness.binding();
        binding.channels = channels.map(|value| value.binding());
        binding
    }
    pub fn authenticate_reservation(
        &self,
        image: &RetainedImage,
        witness: &LaunchWitness<'_>,
    ) -> Result<()> {
        if self.refused.get() {
            return Err(Refusal::NoAuthority);
        }
        let result = self.check_reservation(image, witness);
        if result.is_err() {
            self.refused.set(true);
        }
        result
    }
    fn check_reservation(&self, image: &RetainedImage, witness: &LaunchWitness<'_>) -> Result<()> {
        witness.validate()?;
        image.recheck()?;
        if let Some(channels) = &self.channels {
            channels.recheck()?;
        }
        if image.identity() != &self.certificate.image
            || !live(&self.handle)?
            || self.certificate.namespace.as_ref()
                != Some(&Self::execution_binding(witness, self.channels.as_ref()))
            || start_ticks(self.certificate.pid)? != self.certificate.start_ticks
        {
            return Err(Refusal::Process);
        }
        if self.gate.is_none() {
            return Err(Refusal::NoAuthority);
        }
        Ok(())
    }
    pub fn certificate(&self) -> &ChildCertificate {
        &self.certificate
    }
    pub fn authenticate_running(&self) -> Result<()> {
        if !live(&self.handle)?
            || start_ticks(self.certificate.pid)? != self.certificate.start_ticks
        {
            return Err(Refusal::Process);
        }
        let executable = File::open(format!("/proc/{}/exe", self.certificate.pid))
            .map_err(|_| Refusal::Process)?;
        self.certificate
            .image
            .recheck(&executable)
            .map_err(|_| Refusal::Process)
    }
    pub fn signal(&self, signal: i32) -> Result<()> {
        if self.gate.is_some() {
            if !live(&self.handle)?
                || start_ticks(self.certificate.pid)? != self.certificate.start_ticks
            {
                return Err(Refusal::Process);
            }
        } else {
            self.authenticate_running()?;
        }
        if unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.handle.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        } != 0
        {
            return Err(Refusal::Process);
        }
        Ok(())
    }
    pub fn wait(&mut self, timeout: Duration) -> Result<Option<Outcome>> {
        if self.reaped {
            return Err(Refusal::NoAuthority);
        }
        let millis = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut poll = libc::pollfd {
            fd: self.handle.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let count = unsafe { libc::poll(&mut poll, 1, millis) };
        if count < 0 {
            return Err(Refusal::Process);
        }
        if count == 0 {
            return Ok(None);
        }
        let mut status = 0;
        if unsafe { libc::waitpid(self.certificate.pid as i32, &mut status, libc::WNOHANG) }
            != self.certificate.pid as i32
        {
            return Err(Refusal::Process);
        }
        self.reaped = true;
        self.raw_status = Some(status);
        Ok(Some(status_outcome(status)))
    }
    pub fn stop(&mut self, original: Outcome, graceful: Duration) -> Result<(Outcome, Outcome)> {
        if let Some(_exit) = self.wait(Duration::ZERO)? {
            return Ok((original, Outcome::Exit0));
        }
        self.signal(libc::SIGTERM)?;
        if self.wait(graceful)?.is_none() {
            self.signal(libc::SIGKILL)?;
            if self.wait(Duration::from_secs(5))?.is_none() {
                return Err(Refusal::ExecutionUnknown);
            }
        }
        Ok((original, Outcome::Exit0))
    }
    pub fn reaped(&self) -> bool {
        self.reaped
    }
    pub fn raw_status(&self) -> Option<i32> {
        self.raw_status
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            if self.wait(Duration::ZERO).ok().flatten().is_some() {
                return;
            }
            if self.gate.is_none() && self.authenticate_running().is_err() {
                return;
            }
            // pidfd came from this exact fork; never reacquire a numeric PID here.
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.handle.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                );
            }
            let _ = self.wait(Duration::from_secs(5));
        }
    }
}
pub fn status_outcome(status: i32) -> Outcome {
    if libc::WIFEXITED(status) {
        if libc::WEXITSTATUS(status) == 0 {
            Outcome::Exit0
        } else {
            Outcome::Nonzero
        }
    } else if libc::WIFSIGNALED(status) {
        match libc::WTERMSIG(status) {
            libc::SIGINT => Outcome::Interrupt,
            libc::SIGTERM => Outcome::Terminate,
            _ => Outcome::Unknown,
        }
    } else {
        Outcome::Unknown
    }
}
fn child_identity(pid: u32) -> Result<(File, u64)> {
    Ok((pidfd(pid)?, start_ticks(pid)?))
}
pub use crate::secret_channel::Deadline;
