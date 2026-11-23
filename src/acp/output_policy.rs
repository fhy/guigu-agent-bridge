use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

pub(crate) const MEMORY_HIGH_WATER: u64 = 1_048_576;
pub(crate) const SPOOL_SEGMENT: u64 = 262_144;
pub(crate) const MATRIX_SEGMENT: u64 = 24_576;
pub(crate) const PROCESS_QUOTA: u64 = 268_435_456;
pub(crate) const DELIVERY_ATTEMPTS: u32 = 8;
pub(crate) const STALE_CLAIM: Duration = Duration::from_secs(300);
pub(crate) const MATRIX_SEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableOutputPolicy {
    pub(crate) spool_root: PathBuf,
    pub(crate) max_output_bytes: u64,
    pub(crate) memory_high_water_bytes: u64,
    pub(crate) spool_segment_bytes: u64,
    pub(crate) matrix_segment_bytes: u64,
    pub(crate) process_quota_bytes: u64,
    pub(crate) delivery_attempt_limit: u32,
    pub(crate) stale_claim_after: Duration,
    pub(crate) matrix_send_timeout: Duration,
}

impl DurableOutputPolicy {
    pub(crate) fn derive(database: &Path, turn_max: u64) -> io::Result<Self> {
        let parent = database
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or(std::env::current_dir()?);
        let parent = parent.canonicalize()?;
        let policy = Self {
            spool_root: parent.join("acp-output-spool-v1"),
            max_output_bytes: turn_max,
            memory_high_water_bytes: MEMORY_HIGH_WATER.min(turn_max),
            spool_segment_bytes: SPOOL_SEGMENT.min(turn_max),
            matrix_segment_bytes: MATRIX_SEGMENT.min(turn_max),
            process_quota_bytes: PROCESS_QUOTA,
            delivery_attempt_limit: DELIVERY_ATTEMPTS,
            stale_claim_after: STALE_CLAIM,
            matrix_send_timeout: MATRIX_SEND_TIMEOUT,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> io::Result<()> {
        let valid = self.max_output_bytes > 0
            && self.memory_high_water_bytes > 0
            && self.spool_segment_bytes > 0
            && self.matrix_segment_bytes > 0
            && self.memory_high_water_bytes <= self.max_output_bytes
            && self.spool_segment_bytes <= self.max_output_bytes
            && self.matrix_segment_bytes <= self.spool_segment_bytes
            && self.max_output_bytes <= self.process_quota_bytes
            && self.delivery_attempt_limit > 0
            && !self.matrix_send_timeout.is_zero();
        if valid {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid durable output policy",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_defaults_and_small_limits_are_derived() {
        let root = std::env::current_dir().unwrap();
        let full = DurableOutputPolicy::derive(&root.join("state.db"), 16_777_216).unwrap();
        assert_eq!(
            (
                full.memory_high_water_bytes,
                full.spool_segment_bytes,
                full.matrix_segment_bytes
            ),
            (MEMORY_HIGH_WATER, SPOOL_SEGMENT, MATRIX_SEGMENT)
        );
        assert_eq!(
            (
                full.process_quota_bytes,
                full.delivery_attempt_limit,
                full.stale_claim_after
            ),
            (PROCESS_QUOTA, DELIVERY_ATTEMPTS, STALE_CLAIM)
        );
        assert_eq!(full.matrix_send_timeout, MATRIX_SEND_TIMEOUT);
        for limit in [1, MATRIX_SEGMENT, SPOOL_SEGMENT, MEMORY_HIGH_WATER] {
            let policy = DurableOutputPolicy::derive(&root.join("state.db"), limit).unwrap();
            assert_eq!(policy.max_output_bytes, limit);
            assert!(policy.matrix_segment_bytes <= policy.spool_segment_bytes);
            assert!(policy.spool_segment_bytes <= limit);
        }
        assert!(DurableOutputPolicy::derive(&root.join("state.db"), 0).is_err());
    }
}
