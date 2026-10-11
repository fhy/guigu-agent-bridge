use crate::{
    Refusal, Result,
    contract::{NamespaceSeed, NamespaceSpec},
};
use std::{
    fs::File,
    io::Read,
    os::fd::{AsRawFd, FromRawFd},
};

/// Metadata-only peer admission. No body/token/env is returned to a supervisor.
pub fn peer(file: &File, expected_pid: u32, expected_uid: u32) -> Result<()> {
    let mut credentials = std::mem::MaybeUninit::<libc::ucred>::zeroed();
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            file.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    } != 0
    {
        return Err(Refusal::Channel);
    }
    let credentials = unsafe { credentials.assume_init() };
    if length as usize != std::mem::size_of::<libc::ucred>()
        || credentials.pid as u32 != expected_pid
        || credentials.uid != expected_uid
    {
        return Err(Refusal::Channel);
    }
    Ok(())
}
pub fn worker_spec(arguments: &[String]) -> Result<NamespaceSpec> {
    if arguments.len() != 4 || arguments[1] != "--worker" {
        return Err(Refusal::Contract);
    }
    let candidate = arguments[2].clone();
    let approval = arguments[3].clone();
    NamespaceSpec::derive(NamespaceSeed {
        uuid: crate::contract::UUID.into(),
        candidate,
        approval,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
}
/// Worker-only consumption, before creating any runtime threads. The provider FD
/// is never read/hashed/copied by the supervisor. Single grant, fixed environment key.
pub fn install_worker_secret(
    channel: File,
    mut provider: File,
    expected_owner: u32,
    spec: &NamespaceSpec,
) -> Result<()> {
    peer(&channel, expected_owner, spec.seed().uid)?;
    let mut grant = [0u8; 1];
    if unsafe {
        libc::recv(
            channel.as_raw_fd(),
            grant.as_mut_ptr().cast(),
            1,
            libc::MSG_WAITALL,
        )
    } != 1
        || grant[0] != b'G'
    {
        return Err(Refusal::NoAuthority);
    }
    let mut bytes = Vec::new();
    provider
        .by_ref()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| Refusal::Secret)?;
    if bytes.is_empty() || bytes.len() > 4096 || bytes.iter().any(|b| b.is_ascii_control()) {
        return Err(Refusal::Secret);
    }
    let value = std::str::from_utf8(&bytes).map_err(|_| Refusal::Secret)?;
    // The binary calls this before Tokio/thread startup, never from a supervisor.
    unsafe {
        std::env::set_var("T050_N_A2A_TOKEN", value);
    }
    bytes.fill(0);
    Ok(())
}
/// Explicit fixed descriptor map, no EnvironmentFile, path or token fallback.
pub fn worker_channels() -> Result<(File, File, u32)> {
    let owner = unsafe { libc::getppid() } as u32;
    for fd in [3, 4] {
        if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
            return Err(Refusal::Channel);
        }
    }
    Ok((
        unsafe { File::from_raw_fd(3) },
        unsafe { File::from_raw_fd(4) },
        owner,
    ))
}

/// Bounded non-secret command packet; secret providers never use this parser.
pub fn read_packet<T: serde::de::DeserializeOwned>(file: &File) -> Result<T> {
    if file.metadata().map_err(|_| Refusal::Identity)?.len() > 32768 {
        return Err(Refusal::Length);
    }
    let id = crate::identity::FileId::capture(file)?;
    id.validate(
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() },
        0o600,
    )?;
    if id.len > 32768 {
        return Err(Refusal::Length);
    }
    let mut bytes = vec![0u8; id.len as usize];
    if unsafe { libc::pread(file.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len(), 0) }
        != bytes.len() as isize
    {
        return Err(Refusal::Length);
    }
    id.recheck(file)?;
    serde_json::from_slice(&bytes).map_err(|_| Refusal::Contract)
}

pub fn retained_input(fd: i32) -> Result<File> {
    if fd < 3 {
        return Err(Refusal::Identity);
    }
    let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 16) };
    if copy < 0 {
        return Err(Refusal::Identity);
    }
    Ok(unsafe { File::from_raw_fd(copy) })
}

use crate::identity::{FileId, ObjectId};
use crate::supervision::{ChildCertificate, start_ticks};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBinding {
    pub uuid: String,
    pub root: ObjectId,
    pub config: FileId,
    pub lock: FileId,
    pub channels: Option<(ObjectId, ObjectId)>,
}
/// Original creator authority, not a path or a recaptured replacement identity.
pub struct LaunchWitness<'a> {
    pub spec: &'a crate::contract::NamespaceSpec,
    pub root: &'a crate::paths::OwnedDir,
    pub root_id: &'a ObjectId,
    pub config: &'a File,
    pub config_id: &'a FileId,
    pub control: &'a crate::paths::OwnedDir,
    pub lock: &'a crate::paths::NamespaceLock,
    pub owner: &'a ChildCertificate,
}
impl LaunchWitness<'_> {
    pub fn validate_argv(&self, argv: &[String]) -> Result<()> {
        use crate::contract::Leaf;
        if argv.len() != 4
            || argv[1] != "--worker"
            || argv[2] != self.spec.seed().candidate
            || argv[3] != self.spec.seed().approval
            || ![Leaf::Parent, Leaf::Helper, Leaf::Provisioner]
                .iter()
                .any(|role| self.spec.path(*role).to_str() == Some(&argv[0]))
        {
            return Err(Refusal::Contract);
        }
        Ok(())
    }
    pub fn binding(&self) -> ExecutionBinding {
        ExecutionBinding {
            uuid: self.spec.seed().uuid.clone(),
            root: self.root_id.clone(),
            config: self.config_id.clone(),
            lock: self.lock.identity().clone(),
            channels: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if self.root.identity() != self.root_id
            || self.root_id.uid != self.spec.seed().uid
            || self.root_id.gid != self.spec.seed().gid
            || self.owner.pid != std::process::id()
            || self.owner.uid != self.spec.seed().uid
            || !crate::contract::hex(&self.owner.nonce, 64)
        {
            return Err(Refusal::Identity);
        }
        self.root.recheck()?;
        self.lock.recheck(self.control)?;
        self.config_id
            .validate(self.root_id.uid, self.root_id.gid, 0o600)?;
        self.config_id.recheck(self.config)?;
        self.root
            .subdir("config")?
            .verify_leaf("config.toml", self.config_id)?;
        if start_ticks(self.owner.pid)? != self.owner.start_ticks {
            return Err(Refusal::Process);
        }
        self.owner
            .image
            .recheck(&File::open("/proc/self/exe").map_err(|_| Refusal::Process)?)
            .map_err(|_| Refusal::Process)
    }
}

/// Opaque provider descriptors: metadata only, never read or digested here.
pub struct WorkerChannels {
    channel: File,
    provider: File,
    identities: (ObjectId, ObjectId),
    refused: std::cell::Cell<bool>,
}
impl WorkerChannels {
    pub fn authenticate(channel: &File, provider: &File) -> Result<Self> {
        let channel = duplicate(channel.as_raw_fd())?;
        let provider = duplicate(provider.as_raw_fd())?;
        let identities = (ObjectId::capture(&channel)?, ObjectId::capture(&provider)?);
        let result = Self {
            channel,
            provider,
            identities,
            refused: std::cell::Cell::new(false),
        };
        result.recheck()?;
        Ok(result)
    }
    pub fn recheck(&self) -> Result<()> {
        if self.refused.get() {
            return Err(Refusal::Channel);
        }
        let result = self.check_metadata();
        if result.is_err() {
            self.refused.set(true);
        }
        result
    }
    fn check_metadata(&self) -> Result<()> {
        peer(&self.channel, std::process::id(), unsafe {
            libc::geteuid()
        })?;
        if ObjectId::capture(&self.channel)? != self.identities.0
            || ObjectId::capture(&self.provider)? != self.identities.1
        {
            return Err(Refusal::Channel);
        }
        let metadata = self.provider.metadata().map_err(|_| Refusal::Channel)?;
        use std::os::unix::fs::MetadataExt;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.gid() != unsafe { libc::getegid() }
            || metadata.mode() & 0o7777 != 0o600
            || metadata.nlink() != 1
            || unsafe { libc::fcntl(self.provider.as_raw_fd(), libc::F_GETFL) } & libc::O_ACCMODE
                != libc::O_RDONLY
        {
            return Err(Refusal::Channel);
        }
        Ok(())
    }
    pub(crate) fn descriptors(&self) -> (i32, i32) {
        (self.channel.as_raw_fd(), self.provider.as_raw_fd())
    }
    pub(crate) fn binding(&self) -> (ObjectId, ObjectId) {
        self.identities.clone()
    }
    pub(crate) fn duplicate(&self) -> Result<Self> {
        self.recheck()?;
        Self::authenticate(&self.channel, &self.provider)
    }
}
pub(crate) fn duplicate(fd: i32) -> Result<File> {
    let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 32) };
    if copy < 0 {
        return Err(Refusal::Identity);
    }
    Ok(unsafe { File::from_raw_fd(copy) })
}
pub struct Deadline {
    end: std::time::Instant,
    cancelled: bool,
}
impl Deadline {
    pub fn new(duration: std::time::Duration) -> Self {
        Self {
            end: std::time::Instant::now() + duration,
            cancelled: false,
        }
    }
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }
    pub fn check(&self) -> Result<()> {
        if self.cancelled {
            return Err(Refusal::Cancelled);
        }
        if std::time::Instant::now() >= self.end {
            return Err(Refusal::Timeout);
        }
        Ok(())
    }
}
