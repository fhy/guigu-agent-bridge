use sha2::{Digest, Sha256};

use super::{
    output::{DurableOutputSpool, OutputSpoolError},
    output_files::{self, Manifest, ManifestSegment},
};
use crate::storage::{DeliveryPart, OutputStoreError};

pub(super) struct StreamingPublisher {
    spool: DurableOutputSpool,
    set_id: String,
    owner: String,
    now: String,
    set_dir: std::path::PathBuf,
    buffer: Vec<u8>,
    total: u64,
    aggregate: Sha256,
    manifest: Manifest,
    deliveries: Vec<DeliveryPart>,
}

impl StreamingPublisher {
    pub(super) fn new(
        spool: DurableOutputSpool,
        set_id: &str,
        owner: &str,
        now: &str,
    ) -> Result<Self, OutputSpoolError> {
        spool.prepare_root()?;
        let set_dir = output_files::prepare_set(&spool.policy.spool_root, set_id)?;
        Ok(Self {
            spool,
            set_id: set_id.into(),
            owner: owner.into(),
            now: now.into(),
            set_dir,
            buffer: Vec::new(),
            total: 0,
            aggregate: Sha256::new(),
            manifest: Manifest {
                format: 1,
                set_id: set_id.into(),
                total_bytes: 0,
                total_hash: String::new(),
                segments: Vec::new(),
            },
            deliveries: Vec::new(),
        })
    }

    pub(super) async fn append(&mut self, text: &str) -> Result<(), OutputSpoolError> {
        if self.total.saturating_add(text.len() as u64) > self.spool.policy.max_output_bytes {
            return Err(OutputStoreError::Quota.into());
        }
        self.aggregate.update(text.as_bytes());
        self.total += text.len() as u64;
        let target = usize::try_from(self.spool.policy.spool_segment_bytes)
            .map_err(|_| OutputSpoolError::Identity)?;
        let mut remaining = text;
        while !remaining.is_empty() {
            let free = target - self.buffer.len();
            let mut take = free.min(remaining.len());
            while take > 0 && !remaining.is_char_boundary(take) {
                take -= 1;
            }
            if take == 0 {
                self.flush().await?;
                continue;
            }
            self.buffer.extend_from_slice(&remaining.as_bytes()[..take]);
            remaining = &remaining[take..];
            if self.buffer.len() == target {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), OutputSpoolError> {
        if self.buffer.is_empty() && (!self.manifest.segments.is_empty() || self.total > 0) {
            return Ok(());
        }
        let index = self.manifest.segments.len();
        let start = self
            .manifest
            .segments
            .last()
            .map_or(0, |value| value.end as usize);
        let content = std::mem::take(&mut self.buffer);
        let end = start + content.len();
        let name = format!("segment-{index}.bin");
        let digest = output_files::hash(&content);
        self.spool
            .store
            .reserve_segment(
                &self.set_id,
                index as i64,
                start as i64,
                content.len() as i64,
                &digest,
                &name,
                &self.owner,
                &self.now,
            )
            .await?;
        output_files::publish_file(&self.set_dir, &name, &content)?;
        self.spool
            .store
            .land_segment(&self.set_id, index as i64, &self.owner, &self.now)
            .await?;
        self.add_deliveries(index, start, &content)?;
        self.manifest.segments.push(ManifestSegment {
            index: index as u64,
            start: start as u64,
            end: end as u64,
            hash: digest,
            name,
        });
        Ok(())
    }

    fn add_deliveries(
        &mut self,
        spool_index: usize,
        start: usize,
        content: &[u8],
    ) -> Result<(), OutputSpoolError> {
        let matrix = usize::try_from(self.spool.policy.matrix_segment_bytes)
            .map_err(|_| OutputSpoolError::Identity)?;
        for (local_start, local_end) in output_files::utf8_ranges(content, matrix)? {
            let global_start = start + local_start;
            let global_end = start + local_end;
            let content_hash = output_files::hash(&content[local_start..local_end]);
            let index = self.deliveries.len() as i64;
            self.deliveries.push(DeliveryPart {
                index,
                spool_index: spool_index as i64,
                byte_start: global_start as i64,
                byte_end: global_end as i64,
                stable_txn_id: output_files::stable_txn(&self.set_id, index, &content_hash),
                content_hash,
            });
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<(), OutputSpoolError> {
        self.flush().await?;
        self.manifest.total_bytes = self.total;
        self.manifest.total_hash = format!("{:x}", self.aggregate.finalize());
        let canonical =
            serde_json::to_vec(&self.manifest).map_err(|_| OutputSpoolError::Identity)?;
        output_files::publish_file(&self.set_dir, "manifest-v1.json", &canonical)?;
        self.spool
            .store
            .seal_set(
                &self.set_id,
                self.total as i64,
                &self.manifest.total_hash,
                self.deliveries.len() as i64,
                &self.deliveries,
                &self.now,
            )
            .await?;
        Ok(())
    }
}
