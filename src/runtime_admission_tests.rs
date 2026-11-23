#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (PathBuf, PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("t050-lock-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let database = root.join("state.db");
        let config = root.join("config.toml");
        std::fs::write(&database, b"database").unwrap();
        std::fs::write(&config, b"config").unwrap();
        (root, database, config)
    }

    #[test]
    fn live_owner_is_rejected_and_release_is_inode_bound() {
        let (root, database, config) = fixture();
        let guard = Guard::acquire(&database, &config, "startup").unwrap();
        assert!(Guard::acquire(&database, &config, "reconcile").is_err());
        guard.revalidate().unwrap();
        drop(guard);
        assert!(!root.join(".runtime-admission.lock").exists());
    }

    #[test]
    fn explicit_not_found_owner_is_quarantined_and_reclaimed() {
        let (root, database, config) = fixture();
        let guard = Guard::acquire(&database, &config, "startup").unwrap();
        let path = root.join(".runtime-admission.lock");
        let mut record = read_record(&path).unwrap();
        record.pid = u32::MAX;
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        std::mem::forget(guard);
        let replacement = Guard::acquire(&database, &config, "reconcile").unwrap();
        replacement.revalidate().unwrap();
        assert_eq!(
            std::fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".stale."))
                .count(),
            0
        );
    }

    #[test]
    fn proof_binding_and_policy_identity_are_inode_bound() {
        use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

        let (root, database, config) = fixture();
        let guard = Guard::acquire(&database, &config, "fixture-prepare").unwrap();
        let binding = guard.proof_binding().unwrap();
        assert_eq!(binding.mode, 1);
        let executable = std::env::current_exe().unwrap().canonicalize().unwrap();
        let executable_metadata = executable.metadata().unwrap();
        let config_metadata = config.metadata().unwrap();
        let root_metadata = root.metadata().unwrap();
        guard
            .verify_proof_identities(
                &ProofIdentity {
                    path: executable.as_os_str().as_bytes().to_vec(),
                    device: executable_metadata.dev(),
                    inode: executable_metadata.ino(),
                },
                &ProofIdentity {
                    path: config.as_os_str().as_bytes().to_vec(),
                    device: config_metadata.dev(),
                    inode: config_metadata.ino(),
                },
                &ProofIdentity {
                    path: root.as_os_str().as_bytes().to_vec(),
                    device: root_metadata.dev(),
                    inode: root_metadata.ino(),
                },
            )
            .unwrap();
        let mut wrong = ProofIdentity {
            path: config.as_os_str().as_bytes().to_vec(),
            device: config_metadata.dev(),
            inode: config_metadata.ino(),
        };
        wrong.inode += 1;
        assert!(
            guard
                .verify_proof_identities(
                    &ProofIdentity {
                        path: executable.as_os_str().as_bytes().to_vec(),
                        device: executable_metadata.dev(),
                        inode: executable_metadata.ino()
                    },
                    &wrong,
                    &ProofIdentity {
                        path: root.as_os_str().as_bytes().to_vec(),
                        device: root_metadata.dev(),
                        inode: root_metadata.ino()
                    },
                )
                .is_err()
        );
    }
}
