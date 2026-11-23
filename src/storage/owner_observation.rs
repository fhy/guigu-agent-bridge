use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;

use chrono::{DateTime, Utc};
use serde::Serialize;

use super::RuntimeOwnerEvidence;

pub(crate) const MEMBER_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) enum ScanOutcome {
    Complete,
    ListedMembers,
    Overflow,
    Inaccessible,
    Raced,
    Unavailable,
}

#[derive(Debug)]
pub(crate) struct DomainBinding {
    generation: String,
    directory: Option<File>,
    identity: Option<(u64, u64)>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalObservation {
    pub(crate) outcome: ScanOutcome,
    pub(crate) epoch: i64,
    generation: String,
    identity: Option<(u64, u64)>,
    members: Vec<u32>,
    contained: bool,
}

impl DomainBinding {
    pub(crate) fn for_generation(generation: &str) -> Self {
        let directory = (|| {
            let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
            let path = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
            if !path.starts_with('/') || path.split('/').any(|part| part == "..") {
                return None;
            }
            File::open(format!("/sys/fs/cgroup{path}")).ok()
        })();
        let identity = directory
            .as_ref()
            .and_then(|directory| directory.metadata().ok())
            .map(|metadata| (metadata.dev(), metadata.ino()));
        Self {
            generation: generation.to_owned(),
            directory,
            identity,
        }
    }

    pub(crate) fn scan(&self, epoch: i64) -> FinalObservation {
        let mut observation = FinalObservation {
            outcome: ScanOutcome::Unavailable,
            epoch,
            generation: self.generation.clone(),
            identity: self.identity,
            members: Vec::new(),
            contained: false,
        };
        let Some(directory) = &self.directory else {
            observation.outcome = ScanOutcome::Inaccessible;
            return observation;
        };
        let descriptor = std::os::fd::AsRawFd::as_raw_fd(directory);
        let read = || -> Result<Vec<u32>, ScanOutcome> {
            let mut text = String::new();
            File::open(format!("/proc/self/fd/{descriptor}/cgroup.procs"))
                .map_err(|_| ScanOutcome::Inaccessible)?
                .take(32_769)
                .read_to_string(&mut text)
                .map_err(|_| ScanOutcome::Inaccessible)?;
            if text.len() > 32_768 {
                return Err(ScanOutcome::Overflow);
            }
            let mut members = text
                .split_whitespace()
                .map(str::parse::<u32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ScanOutcome::Raced)?;
            members.sort_unstable();
            Ok(members)
        };
        observation.outcome = match (read(), read(), directory.metadata()) {
            (Ok(first), Ok(second), Ok(metadata)) => {
                observation.members = second.clone();
                if first.len() > MEMBER_LIMIT || second.len() > MEMBER_LIMIT {
                    observation.members.clear();
                    ScanOutcome::Overflow
                } else if first != second || self.identity != Some((metadata.dev(), metadata.ino()))
                {
                    ScanOutcome::Raced
                } else if second.is_empty() {
                    ScanOutcome::Unavailable
                } else {
                    ScanOutcome::ListedMembers
                }
            }
            (Err(outcome), _, _) | (_, Err(outcome), _) => outcome,
            _ => ScanOutcome::Inaccessible,
        };
        observation
    }

    pub(crate) fn identity(&self) -> Option<(u64, u64)> {
        self.identity
    }
}

impl FinalObservation {
    pub(crate) fn bound_to(&self, generation: &str) -> bool {
        self.generation == generation
    }
    pub(crate) fn qualifies(&self, epoch: i64) -> bool {
        self.contained
            && self.outcome == ScanOutcome::Complete
            && self.members.is_empty()
            && self.identity.is_some()
            && !self.generation.is_empty()
            && self.epoch == epoch
            && epoch > 0
    }

    #[cfg(test)]
    pub(crate) fn modeled(outcome: ScanOutcome, epoch: i64, generation: &str) -> Self {
        Self {
            outcome,
            epoch,
            generation: generation.into(),
            identity: Some((1, 2)),
            members: Vec::new(),
            contained: true,
        }
    }
}

impl RuntimeOwnerEvidence {
    pub(crate) fn fingerprint() -> Option<String> {
        let metadata = std::fs::metadata("/proc/self/exe").ok()?;
        let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        Some(format!(
            "pid:{};exe:{}:{}:{}:{}:{};cgroup:{}",
            std::process::id(),
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.ctime(),
            metadata.ctime_nsec(),
            cgroup.trim()
        ))
    }

    pub(crate) fn capture() -> Option<Self> {
        let pid = std::process::id();
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let starttime = stat
            .rsplit_once(") ")?
            .1
            .split_whitespace()
            .nth(19)?
            .parse()
            .ok()?;
        let mut sample = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let status = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut sample) };
        if status != 0 {
            return None;
        }
        Some(Self {
            pid: i64::from(pid),
            proc_starttime: starttime,
            boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .ok()?
                .trim()
                .to_owned(),
            boottime_ns: sample
                .tv_sec
                .checked_mul(1_000_000_000)?
                .checked_add(sample.tv_nsec)?,
        })
    }
}

pub(crate) fn fresh_timestamp(timestamp: &str, observed: &str, window: i64) -> bool {
    if !(1..=300).contains(&window) {
        return false;
    }
    let clocks = DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .zip(DateTime::parse_from_rfc3339(observed).ok());
    clocks.is_some_and(|(timestamp, observed)| {
        let age = observed.signed_duration_since(timestamp);
        age >= chrono::Duration::seconds(-5) && age <= chrono::Duration::seconds(window)
    })
}

pub(crate) fn current_timestamp(timestamp: &str) -> bool {
    fresh_timestamp(timestamp, &Utc::now().to_rfc3339(), 30)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_domain_observation_requires_empty_bounded_matching_domain() {
        let mut observation = FinalObservation::modeled(ScanOutcome::Complete, 1, "generation");
        assert!(observation.qualifies(1));
        assert!(!observation.qualifies(2));
        assert!(!observation.bound_to("another-generation"));
        observation.members = vec![1; 257];
        assert!(!observation.qualifies(1));
        observation.members.clear();
        observation.identity = None;
        assert!(!observation.qualifies(1));
    }

    #[test]
    fn owner_real_domain_without_trusted_provisioning_never_qualifies() {
        let binding = DomainBinding::for_generation("generation");
        assert!(!binding.scan(1).qualifies(1));
        assert!(!binding.scan(-1).qualifies(-1));
        let sample = RuntimeOwnerEvidence::capture().unwrap();
        assert!(sample.pid > 0 && sample.proc_starttime > 0 && sample.boottime_ns > 0);
        assert!(!sample.boot_id.is_empty());
        assert!(RuntimeOwnerEvidence::fingerprint().is_some());
    }
}
