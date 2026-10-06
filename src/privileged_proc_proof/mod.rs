use std::{
    collections::BTreeSet,
    ffi::CString,
    fs::{File, OpenOptions},
    io::Read,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

include!(concat!(env!("OUT_DIR"), "/t050_build.rs"));

pub const HELPER_PATH: &str = "/usr/libexec/guigu-agent-bridge/t050-proc-proof";
pub const MANIFEST_PATH: &str = "/etc/guigu-agent-bridge/t050-proc-proof/artifacts.v1";
pub const POLICY_PATH: &str = "/etc/guigu-agent-bridge/t050-proc-proof/policy.v1";
pub const MAX_POLICY_SIZE: usize = 4096;
pub const MAX_PACKET_SIZE: usize = 8192;
const MAX_HELPER_SIZE: u64 = 1024 * 1024 * 1024;
const HASH_BUFFER_SIZE: usize = 64 * 1024;
const MANIFEST_MAGIC: &[u8; 8] = b"GGPFM001";
const POLICY_MAGIC: &[u8; 8] = b"GGPFP001";
const REQUEST_MAGIC: &[u8; 8] = b"GGPFR001";
const RESULT_MAGIC: &[u8; 8] = b"GGPFO001";
const MAX_CMDLINE: usize = 1024 * 1024;
const MAX_FDS: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofError;

impl From<()> for ProofError {
    fn from((): ()) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofResult {
    Clean,
    Process,
    Listener,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub source_commit: [u8; 20],
    pub build_identity: [u8; 32],
    pub parent_hash: [u8; 32],
    pub helper_hash: [u8; 32],
    pub policy_hash: [u8; 32],
    pub parent_uid: u32,
    pub parent_gid: u32,
    pub parent_mode: u32,
    pub helper_uid: u32,
    pub helper_gid: u32,
    pub helper_mode: u32,
    pub capability_permitted: u64,
    pub capability_inheritable: u64,
    pub capability_flags: u32,
    pub policy_uid: u32,
    pub policy_gid: u32,
    pub policy_mode: u32,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIdentity {
    pub path: Vec<u8>,
    pub device: u64,
    pub inode: u64,
    pub file_type: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub operator_uid: u32,
    pub operator_gid: u32,
    pub binary: FileIdentity,
    pub config: FileIdentity,
    pub database_root: FileIdentity,
    pub admission_lock: Vec<u8>,
    pub unit: Vec<u8>,
    pub ports: [u16; 2],
}

include!("codec.rs");
mod bootstrap;
#[cfg(test)]
mod bootstrap_tests;
mod publication;
#[cfg(test)]
mod publication_tests;
mod stage;
pub use stage::{
    FixturePaths, PRODUCTION_HELPER_SOURCE, PRODUCTION_PARENT_SOURCE, StageCause, stage_policy,
};
include!("provision.rs");
include!("provision_temp.rs");
include!("protocol.rs");
include!("proc_lookup.rs");
include!("scan.rs");
include!("artifact.rs");
include!("capability_evidence.rs");
include!("client.rs");
include!("sandbox.rs");
include!("helper.rs");
include!("tests.rs");
