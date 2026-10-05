// SPLIT_PROTOCOL
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub manifest: [u8; 256],
    pub policy: Vec<u8>,
    pub manifest_identity: ArtifactIdentity,
    pub policy_identity: ArtifactIdentity,
    pub nonce: [u8; 16],
    pub mode: u8,
    pub parent_pid: u32,
    pub parent_starttime: u64,
    pub admission_device: u64,
    pub admission_inode: u64,
    pub deadline_ms: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactIdentity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl ArtifactIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
        }
    }

    fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.device.to_be_bytes());
        out.extend_from_slice(&self.inode.to_be_bytes());
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(&self.mode.to_be_bytes());
        out.extend_from_slice(&self.uid.to_be_bytes());
        out.extend_from_slice(&self.gid.to_be_bytes());
    }

    fn decode(bytes: &[u8], at: &mut usize) -> Result<Self, ()> {
        Ok(Self {
            device: u64_at(bytes, at)?,
            inode: u64_at(bytes, at)?,
            size: u64_at(bytes, at)?,
            mode: u32_at(bytes, at)?,
            uid: u32_at(bytes, at)?,
            gid: u32_at(bytes, at)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub nonce: [u8; 16],
    pub result: ProofResult,
    pub cause: u8,
    pub count: u32,
}

fn take<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N], ()> {
    let end = offset.checked_add(N).ok_or(())?;
    let value = bytes
        .get(*offset..end)
        .ok_or(())?
        .try_into()
        .map_err(|_| ())?;
    *offset = end;
    Ok(value)
}

fn u16_at(bytes: &[u8], offset: &mut usize) -> Result<u16, ()> {
    Ok(u16::from_be_bytes(take(bytes, offset)?))
}

fn u32_at(bytes: &[u8], offset: &mut usize) -> Result<u32, ()> {
    Ok(u32::from_be_bytes(take(bytes, offset)?))
}

fn u64_at(bytes: &[u8], offset: &mut usize) -> Result<u64, ()> {
    Ok(u64::from_be_bytes(take(bytes, offset)?))
}

impl ArtifactManifest {
    fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() != 256 {
            return Err(());
        }
        let mut at = 0;
        if &take::<8>(bytes, &mut at)? != MANIFEST_MAGIC
            || u16_at(bytes, &mut at)? != 1
            || u16_at(bytes, &mut at)? != 256
        {
            return Err(());
        }
        let value = Self {
            source_commit: take(bytes, &mut at)?,
            build_identity: take(bytes, &mut at)?,
            parent_hash: take(bytes, &mut at)?,
            helper_hash: take(bytes, &mut at)?,
            policy_hash: take(bytes, &mut at)?,
            parent_uid: u32_at(bytes, &mut at)?,
            parent_gid: u32_at(bytes, &mut at)?,
            parent_mode: u32_at(bytes, &mut at)?,
            helper_uid: u32_at(bytes, &mut at)?,
            helper_gid: u32_at(bytes, &mut at)?,
            helper_mode: u32_at(bytes, &mut at)?,
            capability_permitted: u64_at(bytes, &mut at)?,
            capability_inheritable: u64_at(bytes, &mut at)?,
            capability_flags: u32_at(bytes, &mut at)?,
            policy_uid: u32_at(bytes, &mut at)?,
            policy_gid: u32_at(bytes, &mut at)?,
            policy_mode: u32_at(bytes, &mut at)?,
            generation: u64_at(bytes, &mut at)?,
        };
        if bytes.get(at..at + 32) != Some(&[0; 32])
            || value.parent_mode != 0o755
            || value.helper_uid != 0
            || value.helper_mode != 0o750
            || value.capability_permitted != 1 << 19
            || value.capability_inheritable != 0
            || value.capability_flags != 1
            || value.policy_uid != 0
            || value.policy_gid != 0
            || value.policy_mode != 0o644
            || value.generation == 0
        {
            return Err(());
        }
        Ok(value)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn encode(&self) -> [u8; 256] {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(MANIFEST_MAGIC);
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&256u16.to_be_bytes());
        out.extend_from_slice(&self.source_commit);
        out.extend_from_slice(&self.build_identity);
        out.extend_from_slice(&self.parent_hash);
        out.extend_from_slice(&self.helper_hash);
        out.extend_from_slice(&self.policy_hash);
        for value in [
            self.parent_uid,
            self.parent_gid,
            self.parent_mode,
            self.helper_uid,
            self.helper_gid,
            self.helper_mode,
        ] {
            out.extend_from_slice(&value.to_be_bytes());
        }
        out.extend_from_slice(&self.capability_permitted.to_be_bytes());
        out.extend_from_slice(&self.capability_inheritable.to_be_bytes());
        out.extend_from_slice(&self.capability_flags.to_be_bytes());
        for value in [self.policy_uid, self.policy_gid, self.policy_mode] {
            out.extend_from_slice(&value.to_be_bytes());
        }
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(&[0; 32]);
        out.try_into().expect("fixed manifest")
    }
}

fn decode_identity(bytes: &[u8]) -> Result<FileIdentity, ()> {
    let mut at = 0;
    let path_len = usize::from(u16_at(bytes, &mut at)?);
    if !(1..=1024).contains(&path_len) {
        return Err(());
    }
    let path = bytes.get(at..at + path_len).ok_or(())?.to_vec();
    at += path_len;
    if !canonical_absolute_path(&path) {
        return Err(());
    }
    let value = FileIdentity {
        path,
        device: u64_at(bytes, &mut at)?,
        inode: u64_at(bytes, &mut at)?,
        file_type: u32_at(bytes, &mut at)?,
        mode: u32_at(bytes, &mut at)?,
        uid: u32_at(bytes, &mut at)?,
        gid: u32_at(bytes, &mut at)?,
    };
    if at != bytes.len() || value.inode == 0 {
        return Err(());
    }
    Ok(value)
}

fn canonical_absolute_path(path: &[u8]) -> bool {
    path.first() == Some(&b'/')
        && path.len() > 1
        && !path.contains(&0)
        && std::str::from_utf8(path).is_ok()
        && !path.ends_with(b"/")
        && path[1..]
            .split(|byte| *byte == b'/')
            .all(|part| !part.is_empty() && part != b"." && part != b"..")
}

#[cfg(any(test, feature = "test-support"))]
fn push_tlv(out: &mut Vec<u8>, tag: u16, value: &[u8]) {
    out.extend_from_slice(&tag.to_be_bytes());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
}

#[cfg(any(test, feature = "test-support"))]
fn encode_identity(value: &FileIdentity) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(value.path.len() as u16).to_be_bytes());
    out.extend_from_slice(&value.path);
    out.extend_from_slice(&value.device.to_be_bytes());
    out.extend_from_slice(&value.inode.to_be_bytes());
    out.extend_from_slice(&value.file_type.to_be_bytes());
    out.extend_from_slice(&value.mode.to_be_bytes());
    out.extend_from_slice(&value.uid.to_be_bytes());
    out.extend_from_slice(&value.gid.to_be_bytes());
    out
}

impl Policy {
    fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() > MAX_POLICY_SIZE || bytes.len() < 12 {
            return Err(());
        }
        let mut at = 0;
        if &take::<8>(bytes, &mut at)? != POLICY_MAGIC
            || u16_at(bytes, &mut at)? != 1
            || usize::from(u16_at(bytes, &mut at)?) != bytes.len()
        {
            return Err(());
        }
        let mut fields = Vec::new();
        while at < bytes.len() {
            let tag = u16_at(bytes, &mut at)?;
            let len = usize::from(u16_at(bytes, &mut at)?);
            let end = at.checked_add(len).ok_or(())?;
            let value = bytes.get(at..end).ok_or(())?;
            if tag != fields.len() as u16 + 1 {
                return Err(());
            }
            fields.push(value);
            at = end;
        }
        if fields.len() != 8 {
            return Err(());
        }
        let one_u32 = |value: &[u8]| -> Result<u32, ()> {
            Ok(u32::from_be_bytes(value.try_into().map_err(|_| ())?))
        };
        let ports = fields[7];
        if ports.len() != 4 {
            return Err(());
        }
        let value = Self {
            operator_uid: one_u32(fields[0])?,
            operator_gid: one_u32(fields[1])?,
            binary: decode_identity(fields[2])?,
            config: decode_identity(fields[3])?,
            database_root: decode_identity(fields[4])?,
            admission_lock: fields[5].to_vec(),
            unit: fields[6].to_vec(),
            ports: [
                u16::from_be_bytes(ports[0..2].try_into().map_err(|_| ())?),
                u16::from_be_bytes(ports[2..4].try_into().map_err(|_| ())?),
            ],
        };
        if value.admission_lock != b".runtime-admission.lock"
            || value.unit != b"guigu-codex-test.service"
            || value.ports != [9291, 9292]
        {
            return Err(());
        }
        Ok(value)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        push_tlv(&mut body, 1, &self.operator_uid.to_be_bytes());
        push_tlv(&mut body, 2, &self.operator_gid.to_be_bytes());
        push_tlv(&mut body, 3, &encode_identity(&self.binary));
        push_tlv(&mut body, 4, &encode_identity(&self.config));
        push_tlv(&mut body, 5, &encode_identity(&self.database_root));
        push_tlv(&mut body, 6, &self.admission_lock);
        push_tlv(&mut body, 7, &self.unit);
        let mut ports = Vec::new();
        ports.extend_from_slice(&self.ports[0].to_be_bytes());
        ports.extend_from_slice(&self.ports[1].to_be_bytes());
        push_tlv(&mut body, 8, &ports);
        let mut out = Vec::new();
        out.extend_from_slice(POLICY_MAGIC);
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&((12 + body.len()) as u16).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }
}
