mod collector;
mod executable;
mod parent;
mod policy;
mod protocol;
mod server;
mod store;

pub use parent::{CollectorRefused, CollectorRequest, CollectorResult, IsolationEvidence, collect};
pub use server::{ServerObservation, observe_server};
pub use store::{StoreGeneration, store_generation};

pub(crate) fn isolation_policy_digest() -> String {
    policy::digest()
}

#[cfg(feature = "test-support")]
pub use parent::collect_from_executable;

pub fn dispatch_before_runtime(args: &[std::ffi::OsString]) -> Option<u8> {
    if args.get(1).and_then(|arg| arg.to_str()) != Some("offline-identity-collector-v1") {
        return None;
    }
    Some(collector::run(args).unwrap_or(5))
}
