use std::io;

use crate::{
    acp::{output_files, output_policy::DurableOutputPolicy, output_stream::StreamingPublisher},
    storage::{DeliveryPart, OutputSegment, OutputSpoolStore, OutputStoreError, PendingDelivery},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OutputSpoolError {
    #[error("durable output filesystem failed")]
    Io(#[from] io::Error),
    #[error("durable output storage failed")]
    Store(#[from] OutputStoreError),
    #[error("durable output identity mismatch")]
    Identity,
}

#[derive(Clone)]
pub(crate) struct DurableOutputSpool {
    pub(super) policy: DurableOutputPolicy,
    pub(super) store: OutputSpoolStore,
}

pub(crate) struct DurableTurnOutput {
    sender: Option<tokio::sync::mpsc::Sender<String>>,
    join: tokio::task::JoinHandle<Result<(), OutputSpoolError>>,
    store: OutputSpoolStore,
    set_id: String,
    now: String,
    spool: DurableOutputSpool,
}

impl DurableTurnOutput {
    pub(crate) fn sender(&self) -> tokio::sync::mpsc::Sender<String> {
        self.sender
            .as_ref()
            .expect("durable sender retained")
            .clone()
    }

    pub(crate) async fn finish(mut self) -> Result<(), OutputSpoolError> {
        self.sender.take();
        let result = match self.join.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("durable output owner failed").into()),
        };
        if let Err(error) = result {
            let segments = self.store.abandon(&self.set_id, &self.now).await?;
            if !segments.is_empty() {
                self.spool.cleanup_files(&self.set_id, &segments)?;
            }
            self.store.finish_abandon(&self.set_id, &self.now).await?;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn cancel(mut self) -> Result<(), OutputSpoolError> {
        self.sender.take();
        self.join.abort();
        let _ = self.join.await;
        let segments = self.store.abandon(&self.set_id, &self.now).await?;
        if !segments.is_empty() {
            self.spool.cleanup_files(&self.set_id, &segments)?;
        }
        self.store.finish_abandon(&self.set_id, &self.now).await?;
        Ok(())
    }
}

impl DurableOutputSpool {
    pub(crate) fn new(policy: DurableOutputPolicy, store: OutputSpoolStore) -> Self {
        Self { policy, store }
    }

    pub(crate) fn prepare_root(&self) -> Result<(), OutputSpoolError> {
        output_files::prepare_root(&self.policy.spool_root)
    }

    pub(crate) async fn start_turn(
        &self,
        task_id: &str,
        owner: &str,
        now: &str,
    ) -> Result<Option<DurableTurnOutput>, OutputSpoolError> {
        let Some(prepared) = self.store.prepare_task(task_id, now).await? else {
            return Ok(None);
        };
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<String>(16);
        let spool = self.clone();
        let owner = owner.to_owned();
        let worker_now = now.to_owned();
        let set_id = prepared.set_id;
        let worker_set = set_id.clone();
        let join = tokio::spawn(async move {
            let mut writer = StreamingPublisher::new(spool, &worker_set, &owner, &worker_now)?;
            while let Some(chunk) = receiver.recv().await {
                writer.append(&chunk).await?;
            }
            writer.finish().await
        });
        Ok(Some(DurableTurnOutput {
            sender: Some(sender),
            join,
            store: self.store.clone(),
            set_id,
            now: now.to_owned(),
            spool: self.clone(),
        }))
    }

    pub(crate) async fn publish(
        &self,
        set_id: &str,
        owner: &str,
        bytes: &[u8],
        now: &str,
    ) -> Result<(), OutputSpoolError> {
        self.prepare_root()?;
        let set_dir = output_files::prepare_set(&self.policy.spool_root, set_id)?;
        let chunk = usize::try_from(self.policy.spool_segment_bytes)
            .map_err(|_| OutputSpoolError::Identity)?;
        let ranges = output_files::utf8_ranges(bytes, chunk)?;
        let mut manifest = output_files::Manifest {
            format: 1,
            set_id: set_id.into(),
            total_bytes: bytes.len() as u64,
            total_hash: output_files::hash(bytes),
            segments: Vec::new(),
        };
        for (index, (start, end)) in ranges.iter().copied().enumerate() {
            let content = &bytes[start..end];
            self.publish_segment(set_id, owner, now, index, start, content, &set_dir)
                .await?;
            manifest.segments.push(output_files::ManifestSegment {
                index: index as u64,
                start: start as u64,
                end: end as u64,
                hash: output_files::hash(content),
                name: format!("segment-{index}.bin"),
            });
        }
        let canonical = serde_json::to_vec(&manifest).map_err(|_| OutputSpoolError::Identity)?;
        output_files::publish_file(&set_dir, "manifest-v1.json", &canonical)?;
        let deliveries = self.delivery_parts(set_id, bytes, &ranges)?;
        self.store
            .seal_set(
                set_id,
                bytes.len() as i64,
                &manifest.total_hash,
                deliveries.len() as i64,
                &deliveries,
                now,
            )
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_segment(
        &self,
        set_id: &str,
        owner: &str,
        now: &str,
        index: usize,
        start: usize,
        content: &[u8],
        set_dir: &std::path::Path,
    ) -> Result<(), OutputSpoolError> {
        let name = format!("segment-{index}.bin");
        let digest = output_files::hash(content);
        if self.store.segment(set_id, index as i64).await?.is_none() {
            self.store
                .reserve_segment(
                    set_id,
                    index as i64,
                    start as i64,
                    content.len() as i64,
                    &digest,
                    &name,
                    owner,
                    now,
                )
                .await?;
        }
        let segment = self
            .store
            .segment(set_id, index as i64)
            .await?
            .ok_or(OutputSpoolError::Identity)?;
        if segment.byte_start != start as i64
            || segment.byte_end != (start + content.len()) as i64
            || segment.content_hash != digest
            || segment.relative_name != name
        {
            return Err(OutputSpoolError::Identity);
        }
        if segment.state == "reserved" && segment.reservation_owner != owner {
            self.store
                .adopt_reserved(set_id, index as i64, owner)
                .await?;
        }
        match segment.state.as_str() {
            "reserved" if set_dir.join(&name).exists() => {
                if output_files::read_segment(&self.policy.spool_root, &segment)? != content {
                    return Err(OutputSpoolError::Identity);
                }
                self.store
                    .land_segment(set_id, index as i64, owner, now)
                    .await?;
            }
            "reserved" => {
                self.store
                    .release_reserved(set_id, index as i64, owner)
                    .await?;
                self.store
                    .reserve_segment(
                        set_id,
                        index as i64,
                        start as i64,
                        content.len() as i64,
                        &digest,
                        &name,
                        owner,
                        now,
                    )
                    .await?;
                output_files::publish_file(set_dir, &name, content)?;
                self.store
                    .land_segment(set_id, index as i64, owner, now)
                    .await?;
            }
            "landed" => {
                if output_files::read_segment(&self.policy.spool_root, &segment)? != content {
                    return Err(OutputSpoolError::Identity);
                }
            }
            _ => return Err(OutputSpoolError::Identity),
        }
        Ok(())
    }

    fn delivery_parts(
        &self,
        set_id: &str,
        bytes: &[u8],
        ranges: &[(usize, usize)],
    ) -> Result<Vec<DeliveryPart>, OutputSpoolError> {
        let max = usize::try_from(self.policy.matrix_segment_bytes)
            .map_err(|_| OutputSpoolError::Identity)?;
        let mut result = Vec::new();
        for (spool_index, (start, end)) in ranges.iter().copied().enumerate() {
            for (local_start, local_end) in output_files::utf8_ranges(&bytes[start..end], max)? {
                let byte_start = start + local_start;
                let byte_end = start + local_end;
                let content_hash = output_files::hash(&bytes[byte_start..byte_end]);
                let index = result.len() as i64;
                result.push(DeliveryPart {
                    index,
                    spool_index: spool_index as i64,
                    byte_start: byte_start as i64,
                    byte_end: byte_end as i64,
                    stable_txn_id: output_files::stable_txn(set_id, index, &content_hash),
                    content_hash,
                });
            }
        }
        Ok(result)
    }

    pub(crate) fn read_delivery(
        &self,
        delivery: &PendingDelivery,
    ) -> Result<Vec<u8>, OutputSpoolError> {
        output_files::read_delivery(&self.policy.spool_root, delivery)
    }

    pub(crate) fn cleanup_files(
        &self,
        set_id: &str,
        segments: &[OutputSegment],
    ) -> Result<(), OutputSpoolError> {
        output_files::cleanup(&self.policy.spool_root, set_id, segments)
    }

    pub(crate) fn policy(&self) -> &DurableOutputPolicy {
        &self.policy
    }

    pub(crate) async fn recover_assembling(&self, owner: &str) -> Result<usize, OutputSpoolError> {
        let pending = self.store.assembling_outputs().await?;
        for row in &pending {
            let payload: crate::models::TaskEventPayload =
                serde_json::from_str(&row.payload).map_err(|_| OutputSpoolError::Identity)?;
            if let crate::models::TaskEventPayload::Completed { output } = payload {
                if row.state == "assembling" {
                    self.publish(&row.set_id, owner, output.as_bytes(), &row.created_at)
                        .await?;
                }
                if !self
                    .store
                    .activate_prepared(&row.set_id, &row.created_at)
                    .await?
                {
                    return Err(OutputSpoolError::Identity);
                }
            } else {
                let segments = self.store.abandon(&row.set_id, &row.created_at).await?;
                if !segments.is_empty() {
                    self.cleanup_files(&row.set_id, &segments)?;
                }
                self.store
                    .finish_abandon(&row.set_id, &row.created_at)
                    .await?;
            }
        }
        Ok(pending.len())
    }

    pub(crate) async fn recover_cleanup(&self, now: &str) -> Result<usize, OutputSpoolError> {
        let pending = self.store.cleanup_sets().await?;
        for set_id in &pending {
            let segments = self.store.begin_cleanup(set_id, now).await?;
            self.cleanup_files(set_id, &segments)?;
            self.store.finish_cleanup(set_id, now).await?;
        }
        let abandoned = self.store.abandoned_sets().await?;
        for set_id in &abandoned {
            let segments = self.store.abandoned_segments(set_id).await?;
            self.cleanup_files(set_id, &segments)?;
            self.store.finish_abandon(set_id, now).await?;
        }
        Ok(pending.len() + abandoned.len())
    }
}
