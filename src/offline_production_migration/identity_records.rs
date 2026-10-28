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
            || !valid_isolation_timeline(intent, self)
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

fn valid_isolation_timeline(intent: &IdentityIntent, evidence: &IdentityEvidence) -> bool {
    let isolation = &evidence.local.isolation;
    let timeline = &isolation.timeline;
    let Some(exited) = timeline.child_exited else {
        return false;
    };
    isolation.nonce.len() == 64
        && isolation.nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !isolation.generation.is_empty()
        && timeline.setup_started >= intent.created_at
        && timeline.ready_verified >= timeline.setup_started
        && timeline.go_sent >= timeline.ready_verified
        && timeline.go_sent - timeline.setup_started <= Duration::seconds(5)
        && timeline.result_received >= timeline.go_sent
        && exited >= timeline.result_received
        && exited - timeline.setup_started <= Duration::seconds(60)
        && evidence.captured_at >= exited
}
