use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictCause {
    AdmissionRuntimeMissing,
    LeaseIdentityInvalid,
    LeaseOwnerMismatch,
    LeaseCardinality,
    PlannerParity,
    Unknown,
}

impl ConflictCause {
    pub const ORDERED: [Self; 6] = [
        Self::AdmissionRuntimeMissing,
        Self::LeaseIdentityInvalid,
        Self::LeaseOwnerMismatch,
        Self::LeaseCardinality,
        Self::PlannerParity,
        Self::Unknown,
    ];

    pub const fn slug(self) -> &'static str {
        match self {
            Self::AdmissionRuntimeMissing => "admission-runtime-missing",
            Self::LeaseIdentityInvalid => "lease-identity-invalid",
            Self::LeaseOwnerMismatch => "lease-owner-mismatch",
            Self::LeaseCardinality => "lease-cardinality",
            Self::PlannerParity => "planner-parity",
            Self::Unknown => "unknown",
        }
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConflictSet(u8);

impl ConflictSet {
    pub fn insert(&mut self, cause: ConflictCause) {
        self.0 |= cause.bit();
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn causes(self) -> impl Iterator<Item = ConflictCause> {
        ConflictCause::ORDERED
            .into_iter()
            .filter(move |cause| self.0 & cause.bit() != 0)
    }

    pub fn public_causes(self) -> Vec<&'static str> {
        if self.is_empty() {
            return vec![ConflictCause::Unknown.slug()];
        }
        self.causes().map(ConflictCause::slug).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagnosedSelectionError {
    InvalidArguments,
    Empty,
    Multiple,
    Malformed,
    Conflicting(ConflictSet),
    Nonterminal,
    Busy,
    Database,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, DiagnosedSelectionError> {
    if args.len() != 3 || args[0] != "diagnose-preacceptance-conflict" || args[1] != "--database" {
        return Err(DiagnosedSelectionError::InvalidArguments);
    }
    let path = args[2]
        .to_str()
        .ok_or(DiagnosedSelectionError::InvalidArguments)?;
    if path.is_empty() {
        return Err(DiagnosedSelectionError::InvalidArguments);
    }
    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_set_is_deduplicated_and_canonical() {
        let mut set = ConflictSet::default();
        set.insert(ConflictCause::PlannerParity);
        set.insert(ConflictCause::LeaseIdentityInvalid);
        set.insert(ConflictCause::PlannerParity);
        assert_eq!(
            set.public_causes(),
            vec!["lease-identity-invalid", "planner-parity"]
        );
        assert_eq!(ConflictSet::default().public_causes(), vec!["unknown"]);
    }
}
