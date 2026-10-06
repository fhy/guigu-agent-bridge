use sha2::{Digest, Sha256};

use super::codec::Identity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Backup = 1,
    Restore = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    Ready = 1,
    IntentManifest = 2,
    ManifestDisabled = 3,
    IntentShm = 4,
    ShmRemoved = 5,
    IntentWal = 6,
    WalRemoved = 7,
    IntentMain = 8,
    MainRemoved = 9,
    IntentSnapshot = 10,
    SnapshotRemoved = 11,
    IntentManifestRemove = 12,
    ManifestRemoved = 13,
    Terminal = 14,
    IntentRestoreJournal = 15,
    RestoreJournalRemoved = 16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    pub sequence: u64,
    pub predecessor: [u8; 32],
    pub kind: Kind,
    pub phase: Phase,
    pub bitmap: u16,
    pub expected_snapshot: [u8; 32],
    pub manifest_digest: [u8; 32],
    pub namespace: Identity,
    pub snapshot: Identity,
    pub manifest: Identity,
    pub old_main: Option<Identity>,
    pub old_wal: Option<Identity>,
    pub old_shm: Option<Identity>,
}

impl Record {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(384);
        out.extend_from_slice(match self.kind {
            Kind::Backup => b"GGT50CB1",
            Kind::Restore => b"GGT50CR1",
        });
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.predecessor);
        out.push(self.kind as u8);
        out.push(self.phase as u8);
        out.extend_from_slice(&self.bitmap.to_be_bytes());
        out.extend_from_slice(&self.expected_snapshot);
        out.extend_from_slice(&self.manifest_digest);
        put_identity(&mut out, self.namespace);
        put_identity(&mut out, self.snapshot);
        put_identity(&mut out, self.manifest);
        put_optional(&mut out, self.old_main);
        put_optional(&mut out, self.old_wal);
        put_optional(&mut out, self.old_shm);
        out.extend_from_slice(&Sha256::digest(&out));
        out
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if !(40..=4096).contains(&bytes.len()) {
            return Err(());
        }
        let (body, tail) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != tail {
            return Err(());
        }
        let mut r = Reader { bytes: body, at: 0 };
        let magic = r.take(8)?;
        let kind = match magic {
            b"GGT50CB1" => Kind::Backup,
            b"GGT50CR1" => Kind::Restore,
            _ => return Err(()),
        };
        if r.u16()? != 1 {
            return Err(());
        }
        let sequence = r.u64()?;
        let predecessor = r.array()?;
        if r.byte()? != kind as u8 {
            return Err(());
        }
        let phase = match r.byte()? {
            1 => Phase::Ready,
            2 => Phase::IntentManifest,
            3 => Phase::ManifestDisabled,
            4 => Phase::IntentShm,
            5 => Phase::ShmRemoved,
            6 => Phase::IntentWal,
            7 => Phase::WalRemoved,
            8 => Phase::IntentMain,
            9 => Phase::MainRemoved,
            10 => Phase::IntentSnapshot,
            11 => Phase::SnapshotRemoved,
            12 => Phase::IntentManifestRemove,
            13 => Phase::ManifestRemoved,
            14 => Phase::Terminal,
            15 => Phase::IntentRestoreJournal,
            16 => Phase::RestoreJournalRemoved,
            _ => return Err(()),
        };
        let value = Self {
            sequence,
            predecessor,
            kind,
            phase,
            bitmap: r.u16()?,
            expected_snapshot: r.array()?,
            manifest_digest: r.array()?,
            namespace: r.identity()?,
            snapshot: r.identity()?,
            manifest: r.identity()?,
            old_main: r.optional()?,
            old_wal: r.optional()?,
            old_shm: r.optional()?,
        };
        if r.at != body.len() || value.encode() != bytes {
            return Err(());
        }
        Ok(value)
    }
    pub(crate) fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }
}

fn put_identity(out: &mut Vec<u8>, v: Identity) {
    out.extend_from_slice(&v.device.to_be_bytes());
    out.extend_from_slice(&v.inode.to_be_bytes());
    out.extend_from_slice(&v.size.to_be_bytes());
    out.extend_from_slice(&v.uid.to_be_bytes());
    out.extend_from_slice(&v.gid.to_be_bytes());
    out.extend_from_slice(&v.mode.to_be_bytes());
}
fn put_optional(out: &mut Vec<u8>, v: Option<Identity>) {
    out.push(u8::from(v.is_some()));
    if let Some(v) = v {
        put_identity(out, v)
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ()> {
        let end = self.at.checked_add(n).ok_or(())?;
        let v = self.bytes.get(self.at..end).ok_or(())?;
        self.at = end;
        Ok(v)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ()> {
        self.take(N)?.try_into().map_err(|_| ())
    }
    fn byte(&mut self) -> Result<u8, ()> {
        Ok(self.take(1)?[0])
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
    fn optional(&mut self) -> Result<Option<Identity>, ()> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.identity()?)),
            _ => Err(()),
        }
    }
}
