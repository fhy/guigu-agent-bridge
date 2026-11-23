impl Request {
    fn encode(&self) -> Result<Vec<u8>, ()> {
        if self.policy.len() > MAX_POLICY_SIZE || !matches!(self.mode, 1..=5) {
            return Err(());
        }
        let mut out = Vec::new();
        out.extend_from_slice(REQUEST_MAGIC);
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&self.manifest);
        out.extend_from_slice(&(self.policy.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.policy);
        self.manifest_identity.encode(&mut out);
        self.policy_identity.encode(&mut out);
        out.extend_from_slice(&self.nonce);
        out.push(self.mode);
        out.extend_from_slice(&[0; 3]);
        out.extend_from_slice(&self.parent_pid.to_be_bytes());
        out.extend_from_slice(&self.parent_starttime.to_be_bytes());
        out.extend_from_slice(&self.admission_device.to_be_bytes());
        out.extend_from_slice(&self.admission_inode.to_be_bytes());
        out.extend_from_slice(&self.deadline_ms.to_be_bytes());
        let len = u16::try_from(out.len()).map_err(|_| ())?;
        out[10..12].copy_from_slice(&len.to_be_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() > MAX_PACKET_SIZE {
            return Err(());
        }
        let mut at = 0;
        if &take::<8>(bytes, &mut at)? != REQUEST_MAGIC
            || u16_at(bytes, &mut at)? != 1
            || usize::from(u16_at(bytes, &mut at)?) != bytes.len()
        {
            return Err(());
        }
        let manifest = take(bytes, &mut at)?;
        let policy_len = usize::from(u16_at(bytes, &mut at)?);
        let policy = bytes.get(at..at + policy_len).ok_or(())?.to_vec();
        at += policy_len;
        let manifest_identity = ArtifactIdentity::decode(bytes, &mut at)?;
        let policy_identity = ArtifactIdentity::decode(bytes, &mut at)?;
        let nonce = take(bytes, &mut at)?;
        let mode = *bytes.get(at).ok_or(())?;
        at += 1;
        if bytes.get(at..at + 3) != Some(&[0; 3]) {
            return Err(());
        }
        at += 3;
        let value = Self {
            manifest,
            policy,
            manifest_identity,
            policy_identity,
            nonce,
            mode,
            parent_pid: u32_at(bytes, &mut at)?,
            parent_starttime: u64_at(bytes, &mut at)?,
            admission_device: u64_at(bytes, &mut at)?,
            admission_inode: u64_at(bytes, &mut at)?,
            deadline_ms: u32_at(bytes, &mut at)?,
        };
        if at != bytes.len() || !matches!(value.mode, 1..=5) || value.deadline_ms > 5000 {
            return Err(());
        }
        ArtifactManifest::decode(&value.manifest)?;
        Policy::decode(&value.policy)?;
        Ok(value)
    }
}

impl Response {
    pub fn encode(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(RESULT_MAGIC);
        out[8..10].copy_from_slice(&1u16.to_be_bytes());
        out[10..12].copy_from_slice(&32u16.to_be_bytes());
        out[12..28].copy_from_slice(&self.nonce);
        out[28] = match self.result {
            ProofResult::Clean => 0,
            ProofResult::Process => 1,
            ProofResult::Listener => 2,
            ProofResult::Unknown => 3,
        };
        out[29] = self.cause;
        out[30..32].copy_from_slice(
            &u16::try_from(self.count.min(u32::from(u16::MAX)))
                .unwrap()
                .to_be_bytes(),
        );
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() != 32
            || &bytes[..8] != RESULT_MAGIC
            || bytes[8..10] != 1u16.to_be_bytes()
            || bytes[10..12] != 32u16.to_be_bytes()
        {
            return Err(());
        }
        Ok(Self {
            nonce: bytes[12..28].try_into().map_err(|_| ())?,
            result: match bytes[28] {
                0 => ProofResult::Clean,
                1 => ProofResult::Process,
                2 => ProofResult::Listener,
                3 => ProofResult::Unknown,
                _ => return Err(()),
            },
            cause: bytes[29],
            count: u32::from(u16::from_be_bytes(
                bytes[30..32].try_into().map_err(|_| ())?,
            )),
        })
    }
}
