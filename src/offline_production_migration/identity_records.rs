use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::offline_identity::{CollectorResult, ServerObservation, StoreGeneration};

use super::{Failure, evidence, records, version2_records::Root};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IdentityIntent {
    pub protocol: u8,
    pub root_digest: String,
    pub operation_id: String,
    pub schema_complete_digest: String,
    pub producer_source: String,
    pub executable_digest: String,
    pub lock_digest: String,
    pub sdk: String,
    pub store_before: StoreGeneration,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IdentityEvidence {
    pub protocol: u8,
    pub intent_digest: String,
    pub before: ServerObservation,
    pub local: CollectorResult,
    pub after: ServerObservation,
    pub store_after: StoreGeneration,
    pub captured_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl IdentityIntent {
    pub fn validate(&self, root: &Root, schema: &str) -> Result<(), Failure> {
        if self.protocol != 1
            || self.root_digest != records::encoded_digest(root)?
            || self.operation_id != root.operation.operation_id
            || self.schema_complete_digest != schema
            || self.sdk != "matrix-sdk=0.13.0"
            || self.lock_digest != evidence::digest(include_bytes!("../../Cargo.lock"))
        {
            return Err(Failure::ManualRequired);
        }
        Ok(())
    }
}

impl IdentityEvidence {
    pub fn validate(&self, intent: &IdentityIntent, now: DateTime<Utc>) -> Result<(), Failure> {
        if self.protocol != 1
            || self.intent_digest != records::encoded_digest(intent)?
            || self.captured_at < intent.created_at
            || self.captured_at > now
            || self.expires_at != self.captured_at + Duration::minutes(15)
            || now > self.expires_at
            || self.local.isolation.process == 0
            || self.local.isolation.executable != intent.executable_digest
            || self.local.isolation.policy != crate::offline_identity::isolation_policy_digest()
            || self.local.isolation.no_new_privs != 1
            || self.local.isolation.seccomp != 2
            || self.local.isolation.denied_probes != [libc::EPERM; 6]
            || self.store_after.locator_digest != intent.store_before.locator_digest
        {
            return Err(Failure::ManualRequired);
        }
        self.before
            .require_continuity(&self.after, &self.local)
            .map_err(|_| Failure::ManualRequired)
    }
}
