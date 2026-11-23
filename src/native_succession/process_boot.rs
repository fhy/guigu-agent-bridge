//! Bind old-incarnation absence separately from current resource exclusion.
use super::{Owner, Result, refuse};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BootObservation {
    pub current: String,
    pub absence: String,
}

fn valid(value: &str) -> bool {
    uuid::Uuid::parse_str(value)
        .is_ok_and(|id| !id.is_nil() && id.hyphenated().to_string() == value)
}
fn read(proc_root: &Path) -> Result<String> {
    let value = fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))
        .map_err(|_| refuse("boot-unavailable"))?;
    let value = value.strip_suffix('\n').unwrap_or(&value);
    if !valid(value) {
        return Err(refuse("boot-malformed"));
    }
    Ok(value.to_owned())
}
impl BootObservation {
    pub(super) fn capture(proc_root: &Path, owner: &Owner) -> Result<Self> {
        if owner.pid <= 0
            || owner.starttime <= 0
            || !valid(&owner.boot)
            || owner.fingerprint.is_empty()
        {
            return Err(refuse("owner-identity"));
        }
        let current = read(proc_root)?;
        let absence = if current == owner.boot {
            match fs::symlink_metadata(proc_root.join(owner.pid.to_string())) {
                Ok(_) => return Err(refuse("live-or-reused-owner-pid")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(_) => return Err(refuse("owner-unreadable")),
            }
            "same-boot-pid-absent"
        } else {
            "different-boot"
        };
        Ok(Self {
            current,
            absence: absence.into(),
        })
    }
    pub(super) fn different_boot(&self) -> bool {
        self.absence == "different-boot"
    }
    pub(super) fn revalidate(&self, proc_root: &Path, owner: &Owner) -> Result<()> {
        if &Self::capture(proc_root, owner)? != self {
            return Err(refuse("boot-proof-drift"));
        }
        Ok(())
    }
    pub(crate) fn valid_for(&self, owner: &Owner) -> bool {
        valid(&owner.boot)
            && valid(&self.current)
            && self.absence
                == if owner.boot == self.current {
                    "same-boot-pid-absent"
                } else {
                    "different-boot"
                }
    }
}
