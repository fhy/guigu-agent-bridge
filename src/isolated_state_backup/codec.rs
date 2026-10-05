use sha2::{Digest, Sha256};

pub(crate) const MANIFEST: &str = "snapshot.manifest.v1";
pub(crate) const SNAPSHOT: &str = "snapshot.sqlite3";
pub(crate) const RESTORE_JOURNAL: &str = "restore.journal.v1";
pub(crate) const BACKUP_CLEANUP: &str = "cleanup.backup.journal.v1";
pub(crate) const RESTORE_CLEANUP: &str = "cleanup.restore.journal.v1";
pub(crate) const LOCK: &str = ".operation.lock";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub operator_uid: u32,
    pub operator_gid: u32,
    pub namespace: Identity,
    pub source_main: Identity,
    pub source_wal: Option<Identity>,
    pub source_shm: Option<Identity>,
    pub source_set_hash: [u8; 32],
    pub source_bytes: u64,
    pub snapshot: Identity,
    pub snapshot_hash: [u8; 32],
    pub sqlite_version: u32,
    pub page_size: u32,
    pub page_count: u32,
    pub user_version: u32,
    pub schema_hash: [u8; 32],
    pub snapshot_id: [u8; 16],
    pub created_nanos: i64,
}

impl Manifest {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(384);
        out.extend_from_slice(b"GGT50B01");
        put_u16(&mut out, 1);
        put_u32(&mut out, self.operator_uid);
        put_u32(&mut out, self.operator_gid);
        put_identity(&mut out, self.namespace);
        put_identity(&mut out, self.source_main);
        put_optional_identity(&mut out, self.source_wal);
        put_optional_identity(&mut out, self.source_shm);
        out.extend_from_slice(&self.source_set_hash);
        put_u64(&mut out, self.source_bytes);
        put_identity(&mut out, self.snapshot);
        out.extend_from_slice(&self.snapshot_hash);
        put_u32(&mut out, self.sqlite_version);
        put_u32(&mut out, self.page_size);
        put_u32(&mut out, self.page_count);
        put_u32(&mut out, self.user_version);
        out.extend_from_slice(&self.schema_hash);
        out.extend_from_slice(&self.snapshot_id);
        out.extend_from_slice(&self.created_nanos.to_be_bytes());
        let digest = Sha256::digest(&out);
        out.extend_from_slice(&digest);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() > 4096 || bytes.len() < 40 {
            return Err(());
        }
        let (body, tail) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != tail {
            return Err(());
        }
        let mut reader = Reader::new(body);
        reader.magic(b"GGT50B01")?;
        if reader.u16()? != 1 {
            return Err(());
        }
        let value = Self {
            operator_uid: reader.u32()?,
            operator_gid: reader.u32()?,
            namespace: reader.identity()?,
            source_main: reader.identity()?,
            source_wal: reader.optional_identity()?,
            source_shm: reader.optional_identity()?,
            source_set_hash: reader.array()?,
            source_bytes: reader.u64()?,
            snapshot: reader.identity()?,
            snapshot_hash: reader.array()?,
            sqlite_version: reader.u32()?,
            page_size: reader.u32()?,
            page_count: reader.u32()?,
            user_version: reader.u32()?,
            schema_hash: reader.array()?,
            snapshot_id: reader.array()?,
            created_nanos: reader.i64()?,
        };
        if !reader.done() || value.encode() != bytes {
            return Err(());
        }
        Ok(value)
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RestorePhase {
    Prepared = 1,
    IntentShm = 2,
    IntentWal = 3,
    IntentMain = 4,
    Quarantined = 5,
    NewDurable = 6,
    IntentPublish = 7,
    Published = 8,
    Verified = 9,
    Committed = 10,
    RollbackStart = 11,
    RollbackNewAbsent = 12,
    RollbackMain = 13,
    RollbackWal = 14,
    RollbackShm = 15,
    Restored = 16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RestoreRecord {
    pub sequence: u64,
    pub predecessor: [u8; 32],
    pub manifest: [u8; 32],
    pub snapshot: [u8; 32],
    pub expected_current: [u8; 32],
    pub phase: RestorePhase,
    pub bitmap: u16,
    pub original_main: Identity,
    pub original_wal: Option<Identity>,
    pub original_shm: Option<Identity>,
    pub replacement: Option<Identity>,
}

impl RestoreRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(320);
        out.extend_from_slice(b"GGT50R01");
        put_u16(&mut out, 1);
        put_u64(&mut out, self.sequence);
        out.extend_from_slice(&self.predecessor);
        out.extend_from_slice(&self.manifest);
        out.extend_from_slice(&self.snapshot);
        out.extend_from_slice(&self.expected_current);
        out.push(self.phase as u8);
        put_u16(&mut out, self.bitmap);
        put_identity(&mut out, self.original_main);
        put_optional_identity(&mut out, self.original_wal);
        put_optional_identity(&mut out, self.original_shm);
        put_optional_identity(&mut out, self.replacement);
        let digest = Sha256::digest(&out);
        out.extend_from_slice(&digest);
        out
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() > 4096 || bytes.len() < 40 {
            return Err(());
        }
        let (body, tail) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != tail {
            return Err(());
        }
        let mut reader = Reader::new(body);
        reader.magic(b"GGT50R01")?;
        if reader.u16()? != 1 {
            return Err(());
        }
        let sequence = reader.u64()?;
        let predecessor = reader.array()?;
        let manifest = reader.array()?;
        let snapshot = reader.array()?;
        let expected_current = reader.array()?;
        let phase = match reader.take(1)?[0] {
            1 => RestorePhase::Prepared,
            2 => RestorePhase::IntentShm,
            3 => RestorePhase::IntentWal,
            4 => RestorePhase::IntentMain,
            5 => RestorePhase::Quarantined,
            6 => RestorePhase::NewDurable,
            7 => RestorePhase::IntentPublish,
            8 => RestorePhase::Published,
            9 => RestorePhase::Verified,
            10 => RestorePhase::Committed,
            11 => RestorePhase::RollbackStart,
            12 => RestorePhase::RollbackNewAbsent,
            13 => RestorePhase::RollbackMain,
            14 => RestorePhase::RollbackWal,
            15 => RestorePhase::RollbackShm,
            16 => RestorePhase::Restored,
            _ => return Err(()),
        };
        let value = Self {
            sequence,
            predecessor,
            manifest,
            snapshot,
            expected_current,
            phase,
            bitmap: reader.u16()?,
            original_main: reader.identity()?,
            original_wal: reader.optional_identity()?,
            original_shm: reader.optional_identity()?,
            replacement: reader.optional_identity()?,
        };
        if !reader.done() || value.encode() != bytes {
            return Err(());
        }
        Ok(value)
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_identity(out: &mut Vec<u8>, value: Identity) {
    put_u64(out, value.device);
    put_u64(out, value.inode);
    put_u64(out, value.size);
    put_u32(out, value.uid);
    put_u32(out, value.gid);
    put_u32(out, value.mode);
}
fn put_optional_identity(out: &mut Vec<u8>, value: Option<Identity>) {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        put_identity(out, value);
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], ()> {
        let end = self.at.checked_add(count).ok_or(())?;
        let value = self.bytes.get(self.at..end).ok_or(())?;
        self.at = end;
        Ok(value)
    }
    fn magic(&mut self, value: &[u8]) -> Result<(), ()> {
        if self.take(value.len())? == value {
            Ok(())
        } else {
            Err(())
        }
    }
    fn u16(&mut self) -> Result<u16, ()> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ()> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ()> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, ()> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ()> {
        self.take(N)?.try_into().map_err(|_| ())
    }
    fn identity(&mut self) -> Result<Identity, ()> {
        Ok(Identity {
            device: self.u64()?,
            inode: self.u64()?,
            size: self.u64()?,
            uid: self.u32()?,
            gid: self.u32()?,
            mode: self.u32()?,
        })
    }
    fn optional_identity(&mut self) -> Result<Option<Identity>, ()> {
        match self.take(1)?[0] {
            0 => Ok(None),
            1 => Ok(Some(self.identity()?)),
            _ => Err(()),
        }
    }
    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }
}
