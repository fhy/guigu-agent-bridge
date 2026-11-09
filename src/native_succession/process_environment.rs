//! Shared working-directory hints alone do not identify a same-boot descendant.
use super::contains;
use std::path::PathBuf;

pub(super) fn related(environment: &[u8], roots: &[PathBuf], private_roots: &[PathBuf]) -> bool {
    environment.split(|byte| *byte == 0).any(|entry| {
        // These may be inherited from another role's shared workspace. Retain
        // all private-root evidence, including private paths inside a workspace.
        // Other keys and malformed entries keep the original conservative match.
        let selectors = if entry.starts_with(b"PWD=") || entry.starts_with(b"OLDPWD=") {
            private_roots
        } else {
            roots
        };
        selectors
            .iter()
            .any(|root| contains(entry, root.as_os_str().as_encoded_bytes()))
    })
}
