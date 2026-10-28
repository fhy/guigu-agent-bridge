mod collector;
mod parent;
mod policy;
mod protocol;

pub use parent::{CollectorRefused, CollectorRequest, CollectorResult, IsolationEvidence, collect};

pub fn dispatch_before_runtime(args: &[std::ffi::OsString]) -> Option<u8> {
    if args.get(1).and_then(|arg| arg.to_str()) != Some("offline-identity-collector-v1") {
        return None;
    }
    Some(collector::run(args).unwrap_or(5))
}
