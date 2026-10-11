use guigu_t050_n::{
    Refusal,
    contract::{Leaf, NamespaceSeed, NamespaceSpec, UUID},
    identity::{FileId, ObjectId, RetainedImage},
    journal::Outcome,
    paths::{NamespaceLock, OwnedDir},
    publication::{Anonymous, Published},
    supervision::{ChildCertificate, LaunchWitness, OwnedChild, start_ticks, validate_peer},
};
use std::{
    fs::{self, File},
    os::{fd::AsRawFd, unix::fs::PermissionsExt},
    time::Duration,
};

pub(crate) struct Context {
    temp: tempfile::TempDir,
    pub(crate) root: OwnedDir,
    control: OwnedDir,
    lock: NamespaceLock,
    config: Published,
    spec: NamespaceSpec,
    owner: ChildCertificate,
}
impl Context {
    pub(crate) fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let f = File::open(temp.path()).unwrap();
        let id = ObjectId::capture(&f).unwrap();
        let root = OwnedDir::authenticate(f, &id).unwrap();
        let control = root.mkdir("control").unwrap();
        let lock = NamespaceLock::create(&control, "namespace.lock", &"a".repeat(64)).unwrap();
        let cfg = root.mkdir("config").unwrap();
        let config = Anonymous::bytes(&cfg, b"non-secret-config", 0o600)
            .unwrap()
            .publish(&cfg, "config.toml")
            .unwrap();
        let spec = NamespaceSpec::derive(NamespaceSeed {
            uuid: UUID.into(),
            candidate: "b".repeat(40),
            approval: "c".repeat(64),
            uid: id.uid,
            gid: id.gid,
        })
        .unwrap();
        let owner = ChildCertificate {
            pid: std::process::id(),
            uid: id.uid,
            start_ticks: start_ticks(std::process::id()).unwrap(),
            image: FileId::capture(&File::open("/proc/self/exe").unwrap()).unwrap(),
            nonce: "d".repeat(64),
            argv_digest: "e".repeat(64),
            namespace: None,
        };
        Self {
            temp,
            root,
            control,
            lock,
            config,
            spec,
            owner,
        }
    }
    fn witness(&self) -> LaunchWitness<'_> {
        LaunchWitness {
            spec: &self.spec,
            root: &self.root,
            root_id: self.root.identity(),
            config: &self.config.file,
            config_id: &self.config.identity,
            control: &self.control,
            lock: &self.lock,
            owner: &self.owner,
        }
    }
    pub(crate) fn image(&self, program: &str, name: &str) -> RetainedImage {
        let f = File::open(program).unwrap();
        let id = FileId::capture(&f).unwrap();
        let p = Anonymous::copy(&self.root, &f, &id, 0o700)
            .unwrap()
            .publish(&self.root, name)
            .unwrap();
        RetainedImage::authenticate(
            p.file,
            &p.identity,
            self.spec.seed().uid,
            self.spec.seed().gid,
        )
        .unwrap()
    }
    fn argv(&self) -> Vec<String> {
        vec![
            self.spec.path(Leaf::Helper).to_str().unwrap().into(),
            "--worker".into(),
            self.spec.seed().candidate.clone(),
            self.spec.seed().approval.clone(),
        ]
    }
    pub(crate) fn reserve(&self, image: &RetainedImage) -> OwnedChild {
        OwnedChild::reserve(
            image,
            &self.argv(),
            &"f".repeat(64),
            image.identity(),
            &self.witness(),
        )
        .unwrap()
    }
}
#[test]
fn namespace_lock_replacement() {
    let c = Context::new();
    let image = c.image("/usr/bin/true", "image");
    fs::rename(
        c.temp.path().join("control/namespace.lock"),
        c.temp.path().join("control/retained.lock"),
    )
    .unwrap();
    Anonymous::bytes(&c.control, b"foreign", 0o600)
        .unwrap()
        .publish(&c.control, "namespace.lock")
        .unwrap();
    assert_eq!(
        OwnedChild::reserve(
            &image,
            &c.argv(),
            &"f".repeat(64),
            image.identity(),
            &c.witness()
        )
        .err(),
        Some(Refusal::Lock)
    );
}
#[test]
fn child_pid_start_peer_mismatch() {
    let c = Context::new();
    let image = c.image("/usr/bin/true", "image");
    let child = c.reserve(&image);
    let id = child.certificate();
    for field in 0..4 {
        let (mut pid, mut uid, mut ticks, mut nonce) =
            (id.pid, id.uid, id.start_ticks, id.nonce.clone());
        match field {
            0 => pid ^= 1,
            1 => uid ^= 1,
            2 => ticks += 1,
            _ => nonce = "a".repeat(64),
        };
        assert_eq!(
            validate_peer(id, pid, uid, ticks, &id.image, &nonce),
            Err(Refusal::Process)
        );
    }
    // Drop stops/reaps only the gated child created by this invocation.
}
#[test]
fn preexec_image_reservation_race() {
    let c = Context::new();
    let original = c.image("/usr/bin/true", "image");
    let mut child = c.reserve(&original);
    let foreign = c.image("/usr/bin/true", "foreign");
    assert_eq!(child.grant(&foreign, &c.witness()), Err(Refusal::Process));
    assert!(!child.reaped());
    assert_eq!(
        child
            .stop(Outcome::Nonzero, Duration::from_millis(50))
            .unwrap(),
        (Outcome::Nonzero, Outcome::Exit0)
    );
    assert!(child.reaped());
}
#[test]
fn descriptor_execution_and_single_grant() {
    let c = Context::new();
    let image = c.image("/usr/bin/true", "image");
    let mut child = c.reserve(&image);
    // The canonical argv path is absent. Execution must use the retained private FD.
    child.grant(&image, &c.witness()).unwrap();
    assert!(child.grant(&image, &c.witness()).is_err());
    assert_eq!(
        child.wait(Duration::from_secs(3)).unwrap(),
        Some(Outcome::Exit0)
    );
    assert!(libc::WIFEXITED(child.raw_status().unwrap()));
}
#[test]
fn descriptor_aba_never_grants() {
    let c = Context::new();
    let original = c.image("/usr/bin/true", "image");
    let mut child = c.reserve(&original);
    let backup = original.file().try_clone().unwrap();
    let foreign = c.image("/usr/bin/false", "foreign");
    assert_eq!(
        unsafe { libc::dup2(foreign.file().as_raw_fd(), original.file().as_raw_fd()) },
        original.file().as_raw_fd()
    );
    assert_eq!(child.grant(&original, &c.witness()), Err(Refusal::Identity));
    assert_eq!(
        unsafe { libc::dup2(backup.as_raw_fd(), original.file().as_raw_fd()) },
        original.file().as_raw_fd()
    );
    assert_eq!(
        child.grant(&original, &c.witness()),
        Err(Refusal::NoAuthority)
    );
    child
        .stop(Outcome::Nonzero, Duration::from_millis(50))
        .unwrap();
}
#[test]
fn missing_fd_and_config_drift_are_redacted() {
    let c = Context::new();
    let image = c.image("/usr/bin/true", "image");
    let error = OwnedChild::reserve_optional(
        None,
        &c.argv(),
        &"f".repeat(64),
        image.identity(),
        &c.witness(),
    )
    .err()
    .unwrap();
    assert_eq!(error.to_string(), "identity");
    let mut child = c.reserve(&image);
    fs::rename(
        c.temp.path().join("config/config.toml"),
        c.temp.path().join("config/original"),
    )
    .unwrap();
    let cfg = c.root.subdir("config").unwrap();
    Anonymous::bytes(&cfg, b"non-secret-config", 0o600)
        .unwrap()
        .publish(&cfg, "config.toml")
        .unwrap();
    assert_eq!(child.grant(&image, &c.witness()), Err(Refusal::Identity));
    child
        .stop(Outcome::Nonzero, Duration::from_millis(50))
        .unwrap();
}
#[test]
fn cancel_timeout_nonzero() {
    let c = Context::new();
    let image = c.image("/usr/bin/false", "image");
    for outcome in [Outcome::Interrupt, Outcome::Terminate, Outcome::Timeout] {
        let mut child = c.reserve(&image);
        assert_eq!(
            child.stop(outcome, Duration::from_millis(50)).unwrap(),
            (outcome, Outcome::Exit0)
        );
        assert!(child.reaped());
        persist_original(&c, outcome, child.raw_status().unwrap());
    }
    let mut child = c.reserve(&image);
    child.grant(&image, &c.witness()).unwrap();
    assert_eq!(
        child.wait(Duration::from_secs(3)).unwrap(),
        Some(Outcome::Nonzero)
    );
    assert_eq!(libc::WEXITSTATUS(child.raw_status().unwrap()), 1);
    persist_original(&c, Outcome::Nonzero, child.raw_status().unwrap());
    let mut deadline = guigu_t050_n::supervision::Deadline::new(Duration::ZERO);
    assert_eq!(deadline.check(), Err(Refusal::Timeout));
    deadline.cancel();
    assert_eq!(deadline.check(), Err(Refusal::Cancelled));
}
fn persist_original(c: &Context, outcome: Outcome, status: i32) {
    use guigu_t050_n::journal::{Binding, Journal, Stage};
    let name = format!("journal-{outcome:?}");
    let binding = Binding {
        uuid: UUID.into(),
        root: c.root.identity().clone(),
        epoch: "a".repeat(64),
        candidate: "b".repeat(40),
    };
    let mut j = Journal::new(c.root.mkdir(&name).unwrap(), binding.clone());
    for stage in [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
        Stage::PublicationCommitted,
        Stage::OperationReserved,
        Stage::ChildIntent,
        Stage::ChildAuthenticated,
        Stage::ScopeAuthenticated,
        Stage::Running,
    ] {
        j.append(stage, Outcome::None, Outcome::None, None, None)
            .unwrap();
    }
    j.append_status(
        Stage::Failure,
        outcome,
        Outcome::Exit0,
        None,
        None,
        Some(status),
    )
    .unwrap();
    let recovered =
        Journal::recover(c.root.subdir(&name).unwrap(), binding, &j.witnesses()).unwrap();
    let record = recovered.last().unwrap();
    assert_eq!(record.original, outcome);
    assert_eq!(record.raw_status, Some(status));
    assert_eq!(record.cleanup, Outcome::Exit0);
    assert_eq!(recovered.disposition(), Err(Refusal::NoAuthority));
}
#[test]
fn unsupported_kernel_boundary() {
    for missing in 0..3 {
        let mut facts = [true; 3];
        facts[missing] = false;
        assert_eq!(
            guigu_t050_n::owned_syscalls::require_features(facts[0], facts[1], facts[2]),
            Err(Refusal::Unsupported)
        );
    }
    guigu_t050_n::owned_syscalls::require_features(true, true, true).unwrap();
}

#[test]
fn opaque_worker_fd_map_and_replacement_refusal() {
    use guigu_t050_n::secret_channel::WorkerChannels;
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    let c = Context::new();
    let image = c.image("/usr/bin/true", "channel-image");
    let (socket, _peer) = UnixStream::pair().unwrap();
    let channel = File::from(OwnedFd::from(socket));
    let p = Anonymous::bytes(&c.root, b"opaque-fixture-only", 0o600)
        .unwrap()
        .publish(&c.root, "provider")
        .unwrap();
    let provider = File::open(c.temp.path().join("provider")).unwrap();
    let channels = WorkerChannels::authenticate(&channel, &provider).unwrap();
    let mut child = OwnedChild::reserve_with_channels(
        &image,
        &c.argv(),
        &"f".repeat(64),
        image.identity(),
        &c.witness(),
        Some(&channels),
    )
    .unwrap();
    assert!(
        child
            .certificate()
            .namespace
            .as_ref()
            .unwrap()
            .channels
            .is_some()
    );
    child
        .authenticate_reservation(&image, &c.witness())
        .unwrap();
    child.grant(&image, &c.witness()).unwrap();
    assert_eq!(
        child.wait(Duration::from_secs(5)).unwrap(),
        Some(Outcome::Exit0)
    );
    assert!(child.reaped());
    fs::set_permissions(
        c.temp.path().join("provider"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert_eq!(channels.recheck(), Err(Refusal::Channel));
    fs::set_permissions(
        c.temp.path().join("provider"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert_eq!(channels.recheck(), Err(Refusal::Channel));
    assert!(
        OwnedChild::reserve_with_channels(
            &image,
            &c.argv(),
            &"f".repeat(64),
            image.identity(),
            &c.witness(),
            Some(&channels)
        )
        .is_err()
    );
    // Provider content is not parsed or hashed: even the retained original p FD
    // is intentionally unused by the metadata-only supervisor interface.
    drop(p);
}
