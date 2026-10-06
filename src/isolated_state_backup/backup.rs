use std::fs::File;

use chrono::Utc;
use sha2::{Digest, Sha256};

use super::{
    Failure, NAMESPACE, Paths,
    codec::{MANIFEST, Manifest, SNAPSHOT},
    fs, sqlite,
};

const COMMAND: &str = "backup-isolated-t050-state";

#[derive(Clone)]
pub(crate) struct SourceSet {
    pub(crate) main: super::codec::Identity,
    pub(crate) wal: Option<super::codec::Identity>,
    pub(crate) shm: Option<super::codec::Identity>,
    pub(crate) hash: [u8; 32],
    pub(crate) bytes: u64,
    pub(crate) main_hash: [u8; 32],
    pub(crate) wal_hash: Option<[u8; 32]>,
}

pub(crate) async fn run(paths: Paths) -> Result<String, Failure> {
    let guard = crate::runtime_admission::Guard::acquire(&paths.database, &paths.config, "backup")
        .map_err(|_| Failure::rejected(COMMAND, "identity"))?;
    match crate::privileged_proc_proof::prove_runtime_absence(&guard) {
        Ok(crate::privileged_proc_proof::ProofResult::Clean) => {}
        _ => return Err(Failure::rejected(COMMAND, "runtime")),
    }
    create(paths).await
}

async fn create(paths: Paths) -> Result<String, Failure> {
    let root = fs::open_dir(&paths.root).map_err(|_| Failure::rejected(COMMAND, "identity"))?;
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let namespace = fs::create_namespace(&root, NAMESPACE, uid, gid)
        .map_err(|_| Failure::rejected(COMMAND, "target"))?;
    let namespace_identity = fs::dir_identity(
        &namespace
            .metadata()
            .map_err(|_| Failure::rejected(COMMAND, "target"))?,
    )
    .map_err(|_| Failure::rejected(COMMAND, "target"))?;
    let mut publication = Publication::default();
    let outcome = create_in_namespace(&paths, &root, &namespace, uid, gid, &mut publication).await;
    if outcome.is_err() {
        publication
            .rollback(&namespace)
            .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
        fs::remove_dir_exact(&root, NAMESPACE, namespace_identity)
            .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
        root.sync_all()
            .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
    }
    outcome
}

#[cfg(test)]
pub(crate) async fn create_fixture(paths: Paths) -> Result<String, Failure> {
    create(paths).await
}

#[cfg(test)]
pub(crate) async fn verify_fixture(paths: &Paths) -> Result<String, &'static str> {
    verify_inner(paths).await
}

async fn create_in_namespace(
    paths: &Paths,
    _root: &File,
    namespace: &File,
    uid: u32,
    gid: u32,
    publication: &mut Publication,
) -> Result<String, Failure> {
    let operation =
        fs::OperationLock::acquire(namespace).map_err(|_| Failure::rejected(COMMAND, "target"))?;
    let outcome = create_locked(paths, namespace, uid, gid, publication).await;
    if operation.finish().is_err() {
        return Err(Failure::rejected(COMMAND, "cleanup"));
    }
    outcome
}

async fn create_locked(
    paths: &Paths,
    namespace: &File,
    uid: u32,
    gid: u32,
    publication: &mut Publication,
) -> Result<String, Failure> {
    let (before, source_main) =
        source_set_retained(paths).map_err(|_| Failure::rejected(COMMAND, "identity"))?;
    let temporary = format!("snapshot.tmp.{}", uuid::Uuid::now_v7().simple());
    let destination = fs::create_leaf(namespace, &temporary, 0o600)
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication
        .arm(namespace, &temporary, &destination)
        .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
    let (facts, after) = sqlite::online_backup(paths, &before, &source_main, &destination)
        .await
        .map_err(|_| Failure::rejected(COMMAND, "sqlite"))?;
    destination
        .sync_all()
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    let verified = sqlite::verify_retained(&destination)
        .await
        .map_err(|_| Failure::rejected(COMMAND, "sqlite"))?;
    if facts != verified {
        return Err(Failure::rejected(COMMAND, "sqlite"));
    }
    let mut destination_hash_file = destination
        .try_clone()
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    let (snapshot_hash, _) = fs::hash(&mut destination_hash_file)
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication
        .promote(&temporary, &destination_hash_file)
        .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
    fs::rename_noreplace(namespace, &temporary, namespace, SNAPSHOT)
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication.rename(&temporary, SNAPSHOT);
    namespace
        .sync_all()
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    let snapshot = fs::open_leaf(namespace, SNAPSHOT, false)
        .and_then(|file| fs::identity(&file.metadata().map_err(|_| ())?))
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    let manifest = Manifest {
        operator_uid: uid,
        operator_gid: gid,
        namespace: fs::dir_identity(
            &namespace
                .metadata()
                .map_err(|_| Failure::rejected(COMMAND, "identity"))?,
        )
        .map_err(|_| Failure::rejected(COMMAND, "identity"))?,
        source_main: before.main,
        source_wal: before.wal,
        source_shm: after.shm,
        source_set_hash: after.hash,
        source_bytes: after.bytes,
        snapshot,
        snapshot_hash,
        sqlite_version: facts.version,
        page_size: facts.page_size,
        page_count: facts.page_count,
        user_version: facts.user_version,
        schema_hash: facts.schema_hash,
        snapshot_id: *uuid::Uuid::now_v7().as_bytes(),
        created_nanos: Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| Failure::rejected(COMMAND, "publication"))?,
    };
    let manifest_temp = format!("manifest.tmp.{}", uuid::Uuid::now_v7().simple());
    let mut manifest_file = fs::create_leaf(namespace, &manifest_temp, 0o600)
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication
        .arm(namespace, &manifest_temp, &manifest_file)
        .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
    fs::write_sync(&mut manifest_file, &manifest.encode())
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication
        .promote(&manifest_temp, &manifest_file)
        .map_err(|_| Failure::rejected(COMMAND, "cleanup"))?;
    fs::rename_noreplace(namespace, &manifest_temp, namespace, MANIFEST)
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication.rename(&manifest_temp, MANIFEST);
    namespace
        .sync_all()
        .map_err(|_| Failure::rejected(COMMAND, "publication"))?;
    publication.disarm();
    Ok(format!(
        "backup-isolated-t050-state result=created status=0 snapshot_sha256={} source_set_sha256={} bytes={}",
        hex(snapshot_hash),
        hex(after.hash),
        snapshot.size
    ))
}

pub(crate) async fn verify(paths: Paths) -> Result<String, Failure> {
    const VERIFY: &str = "verify-isolated-t050-backup";
    let guard =
        crate::runtime_admission::Guard::acquire(&paths.database, &paths.config, "backup-verify")
            .map_err(|_| Failure::rejected(VERIFY, "identity"))?;
    match crate::privileged_proc_proof::prove_runtime_absence(&guard) {
        Ok(crate::privileged_proc_proof::ProofResult::Clean) => {}
        _ => return Err(Failure::rejected(VERIFY, "runtime")),
    }
    verify_inner(&paths)
        .await
        .map_err(|cause| Failure::rejected(VERIFY, cause))
}

pub(crate) async fn verify_inner(paths: &Paths) -> Result<String, &'static str> {
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| "identity")?;
    let operation = fs::OperationLock::acquire(&namespace).map_err(|_| "cleanup")?;
    let outcome = verify_locked(paths, &namespace).await;
    if operation.finish().is_err() {
        return Err("cleanup");
    }
    outcome
}

async fn verify_locked(paths: &Paths, namespace: &File) -> Result<String, &'static str> {
    let manifest = load_manifest(namespace).map_err(|_| "manifest")?;
    let mut snapshot = fs::open_leaf(namespace, SNAPSHOT, false).map_err(|_| "snapshot")?;
    let (snapshot_hash, snapshot_identity) = fs::hash(&mut snapshot).map_err(|_| "snapshot")?;
    if snapshot_hash != manifest.snapshot_hash || !fs::same(snapshot_identity, manifest.snapshot) {
        return Err("snapshot");
    }
    let facts = sqlite::verify_retained(&snapshot)
        .await
        .map_err(|_| "snapshot")?;
    if facts.page_size != manifest.page_size
        || facts.page_count != manifest.page_count
        || facts.user_version != manifest.user_version
        || facts.schema_hash != manifest.schema_hash
    {
        return Err("snapshot");
    }
    let current = source_set(paths).map_err(|_| "current")?;
    let output = format!(
        "verify-isolated-t050-backup result=verified status=0 snapshot_sha256={} current_set_sha256={} bytes={}",
        hex(snapshot_hash),
        hex(current.hash),
        snapshot_identity.size
    );
    Ok(output)
}

pub(crate) fn load_manifest(namespace: &File) -> Result<Manifest, ()> {
    let mut file = fs::open_leaf(namespace, MANIFEST, false)?;
    Manifest::decode(&fs::read_small(&mut file)?)
}

pub(crate) fn source_set(paths: &Paths) -> Result<SourceSet, ()> {
    source_set_retained(paths).map(|(set, _)| set)
}

pub(crate) fn source_set_retained(paths: &Paths) -> Result<(SourceSet, File), ()> {
    let root = fs::open_dir(&paths.root)?;
    let mut main_file = fs::open_leaf(&root, "state.db", false)?;
    let (main_hash, main) = fs::hash(&mut main_file)?;
    let (wal, wal_hash) = optional_source(&root, "state.db-wal")?;
    let (shm, _) = optional_source(&root, "state.db-shm")?;
    for sidecar in [wal, shm].into_iter().flatten() {
        if sidecar.uid != main.uid || sidecar.gid != main.gid || sidecar.mode != main.mode {
            return Err(());
        }
    }
    let mut digest = Sha256::new();
    encode_component(&mut digest, 1, Some((main, main_hash)));
    encode_component(&mut digest, 2, wal.zip(wal_hash));
    if let Some(shm) = shm {
        digest.update([3, 1]);
        digest.update(shm.device.to_be_bytes());
        digest.update(shm.inode.to_be_bytes());
        digest.update(shm.size.to_be_bytes());
    } else {
        digest.update([3, 0]);
    }
    let bytes = main
        .size
        .checked_add(wal.map_or(0, |value| value.size))
        .and_then(|v| v.checked_add(shm.map_or(0, |value| value.size)))
        .ok_or(())?;
    if bytes > 2 * super::MAX_FILE {
        return Err(());
    }
    Ok((
        SourceSet {
            main,
            wal,
            shm,
            hash: digest.finalize().into(),
            bytes,
            main_hash,
            wal_hash,
        },
        main_file,
    ))
}

fn source_leaf(dir: &File, name: &str) -> Result<(super::codec::Identity, [u8; 32]), ()> {
    let mut file = fs::open_leaf(dir, name, false)?;
    let (hash, identity) = fs::hash(&mut file)?;
    Ok((identity, hash))
}
fn optional_source(
    dir: &File,
    name: &str,
) -> Result<(Option<super::codec::Identity>, Option<[u8; 32]>), ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok((None, None));
    }
    match source_leaf(dir, name) {
        Ok((identity, hash)) => Ok((Some(identity), Some(hash))),
        Err(()) => Err(()),
    }
}
fn encode_component(
    digest: &mut Sha256,
    kind: u8,
    value: Option<(super::codec::Identity, [u8; 32])>,
) {
    digest.update([kind, u8::from(value.is_some())]);
    if let Some((identity, hash)) = value {
        digest.update(identity.size.to_be_bytes());
        digest.update(hash);
    }
}
#[derive(Default)]
struct Publication {
    leaves: Vec<(String, super::codec::Identity)>,
}

impl Publication {
    fn arm(&mut self, namespace: &File, name: &str, file: &File) -> Result<(), ()> {
        super::fault::hit("before-publication-metadata")?;
        let identity = fs::identity(&file.metadata().map_err(|_| ())?)?;
        self.leaves.push((name.to_owned(), identity));
        let current = fs::open_leaf(namespace, name, false)?;
        let observed = fs::identity(&current.metadata().map_err(|_| ())?)?;
        if !fs::same(identity, observed) {
            return Err(());
        }
        super::fault::hit("after-publication-create")?;
        Ok(())
    }

    fn promote(&mut self, name: &str, file: &File) -> Result<(), ()> {
        let entry = self
            .leaves
            .iter_mut()
            .find(|(candidate, _)| candidate == name)
            .ok_or(())?;
        let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
        if !fs::same_object(entry.1, observed) {
            return Err(());
        }
        entry.1 = observed;
        Ok(())
    }

    fn rename(&mut self, old: &str, new: &str) {
        if let Some((name, _)) = self
            .leaves
            .iter_mut()
            .find(|(candidate, _)| candidate == old)
        {
            *name = new.to_owned();
        }
    }

    fn rollback(&mut self, namespace: &File) -> Result<(), ()> {
        while let Some((name, expected)) = self.leaves.pop() {
            let file = fs::open_leaf(namespace, &name, false)?;
            let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
            if !fs::same(expected, observed) {
                return Err(());
            }
            fs::unlink(namespace, &name)?;
            namespace.sync_all().map_err(|_| ())?;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.leaves.clear();
    }
}
pub(crate) fn hex(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
