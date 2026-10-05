// SPLIT_TESTS
#[cfg(test)]
mod tests {
    use super::*;

    fn identity(path: &[u8]) -> FileIdentity {
        FileIdentity {
            path: path.to_vec(),
            device: 1,
            inode: 2,
            file_type: libc::S_IFREG,
            mode: 0o600,
            uid: 1000,
            gid: 1000,
        }
    }

    fn policy() -> Policy {
        Policy {
            operator_uid: 1000,
            operator_gid: 1000,
            binary: identity(b"/x/bin"),
            config: identity(b"/x/config"),
            database_root: identity(b"/x/db"),
            admission_lock: b".runtime-admission.lock".to_vec(),
            unit: b"guigu-codex-test.service".to_vec(),
            ports: [9291, 9292],
        }
    }

    fn manifest() -> ArtifactManifest {
        ArtifactManifest {
            source_commit: [1; 20],
            build_identity: [2; 32],
            parent_hash: [3; 32],
            helper_hash: [4; 32],
            policy_hash: [5; 32],
            parent_uid: 1000,
            parent_gid: 1000,
            parent_mode: 0o755,
            helper_uid: 0,
            helper_gid: 1000,
            helper_mode: 0o750,
            capability_permitted: 1 << 19,
            capability_inheritable: 0,
            capability_flags: 1,
            policy_uid: 0,
            policy_gid: 0,
            policy_mode: 0o644,
            generation: 1,
        }
    }

    #[test]
    fn canonical_codecs_reject_trailing_and_unknown_fields() {
        let m = manifest().encode();
        assert_eq!(ArtifactManifest::decode(&m).unwrap(), manifest());
        assert!(ArtifactManifest::decode(&m[..255]).is_err());
        let p = policy().encode();
        assert_eq!(Policy::decode(&p).unwrap(), policy());
        let mut trailing = p.clone();
        trailing.push(0);
        assert!(Policy::decode(&trailing).is_err());
        let mut duplicate = p;
        duplicate.extend_from_slice(&[0, 8, 0, 0]);
        let len = duplicate.len() as u16;
        duplicate[10..12].copy_from_slice(&len.to_be_bytes());
        assert!(Policy::decode(&duplicate).is_err());
        let mut reserved = manifest().encode();
        reserved[255] = 1;
        assert!(ArtifactManifest::decode(&reserved).is_err());
        let mut zero_generation = manifest().encode();
        zero_generation[216..224].fill(0);
        assert!(ArtifactManifest::decode(&zero_generation).is_err());
    }

    #[test]
    fn request_and_response_are_exactly_framed() {
        let p = policy().encode();
        let request = Request {
            manifest: manifest().encode(),
            policy: p,
            manifest_identity: ArtifactIdentity {
                device: 1,
                inode: 2,
                size: 256,
                mode: libc::S_IFREG | 0o644,
                uid: 0,
                gid: 0,
            },
            policy_identity: ArtifactIdentity {
                device: 1,
                inode: 3,
                size: 300,
                mode: libc::S_IFREG | 0o644,
                uid: 0,
                gid: 0,
            },
            nonce: [7; 16],
            mode: 1,
            parent_pid: 42,
            parent_starttime: 9,
            admission_device: 3,
            admission_inode: 4,
            deadline_ms: 1000,
        };
        let bytes = request.encode().unwrap();
        assert_eq!(Request::decode(&bytes).unwrap(), request);
        let mut extra = bytes;
        extra.push(0);
        assert!(Request::decode(&extra).is_err());
        assert!(Request::decode(&extra[..extra.len() - 2]).is_err());
        let response = Response {
            nonce: [7; 16],
            result: ProofResult::Clean,
            cause: 0,
            count: 0,
        };
        assert_eq!(Response::decode(&response.encode()).unwrap(), response);
        let mut oversized = response.encode().to_vec();
        oversized.push(0);
        assert!(Response::decode(&oversized).is_err());
    }

    #[test]
    fn seqpacket_credentials_bind_each_sender() {
        let (left, right) = socketpair().unwrap();
        send_packet(left.as_raw_fd(), b"request").unwrap();
        let (bytes, credentials) = recv_packet(right.as_raw_fd()).unwrap();
        assert_eq!(bytes, b"request");
        assert_eq!(credentials.pid, std::process::id() as i32);
        assert_eq!(credentials.uid, unsafe { libc::getuid() });
    }

    fn generation(pid: u32) -> Generation {
        Generation {
            pid,
            starttime: 10,
            device: 20,
            inode: u64::from(pid),
        }
    }

    fn snapshot(generation: Generation) -> ProcessSnapshot {
        ProcessSnapshot {
            uid: 1000,
            generation,
            cmdline_hash: [8; 32],
            cmdline_len: 8,
            config_argument: false,
            executable: (1, 2, libc::S_IFREG),
            descriptors: Vec::new(),
        }
    }

    #[test]
    fn stable_evaluation_rejects_process_and_global_races() {
        let first_generation = generation(41);
        let global = GlobalSnapshot {
            generations: vec![first_generation.clone()],
            listeners: BTreeSet::new(),
        };
        let before = snapshot(first_generation);
        let mut after = before.clone();
        after.cmdline_hash[0] ^= 1;
        assert_eq!(
            evaluate_stable(&global, &global, &[(before.clone(), after)], &policy(), 0),
            None
        );
        let changed = GlobalSnapshot {
            generations: vec![generation(42)],
            listeners: BTreeSet::new(),
        };
        assert_eq!(
            evaluate_stable(&global, &changed, &[(before.clone(), before)], &policy(), 0),
            None
        );
    }

    #[test]
    fn stable_evaluation_preserves_binding_precedence() {
        let process_generation = generation(41);
        let listener = Listener {
            family: 4,
            address: b"0100007F".to_vec(),
            port: 9292,
            inode: 77,
        };
        let global = GlobalSnapshot {
            generations: vec![process_generation.clone()],
            listeners: BTreeSet::from([listener]),
        };
        let mut process = snapshot(process_generation);
        process.descriptors.push(Descriptor {
            number: 5,
            device: 9,
            inode: 77,
            file_type: libc::S_IFSOCK,
        });
        assert_eq!(
            evaluate_stable(
                &global,
                &global,
                &[(process.clone(), process.clone())],
                &policy(),
                0
            ),
            Some(ProofResult::Listener)
        );
        process.descriptors.push(Descriptor {
            number: 6,
            device: 1,
            inode: 2,
            file_type: libc::S_IFREG,
        });
        assert_eq!(
            evaluate_stable(&global, &global, &[(process.clone(), process)], &policy(), 0),
            Some(ProofResult::Process)
        );
        let ignored = snapshot(generation(41));
        assert_eq!(
            evaluate_stable(&global, &global, &[(ignored.clone(), ignored)], &policy(), 41),
            Some(ProofResult::Clean)
        );
    }

    #[test]
    fn canonical_policy_rejects_noncanonical_identity_paths() {
        let mut value = policy();
        value.config.path = b"/x/../config".to_vec();
        assert!(Policy::decode(&value.encode()).is_err());
    }

    #[test]
    fn proc_lookup_audit_enforces_grammar_and_magic_link_sites() {
        use std::os::unix::fs::symlink;

        for value in [HELPER_PATH, MANIFEST_PATH, POLICY_PATH] {
            assert!(
                ![
                    "environ",
                    "mem",
                    "maps",
                    "stack",
                    "cwd",
                    "root",
                    "map_files"
                ]
                .iter()
                .any(|part| value.contains(part))
            );
        }
        let root = std::env::temp_dir().join(format!("t050-proc-{}", uuid::Uuid::now_v7()));
        let self_pid = std::process::id();
        let process_root = root.join(self_pid.to_string());
        std::fs::create_dir_all(process_root.join("fd")).unwrap();
        std::fs::create_dir(process_root.join("net")).unwrap();
        std::fs::write(
            process_root.join("stat"),
            format!("{self_pid} (x) S{} 10\n", " 0".repeat(18)),
        )
        .unwrap();
        let uid = unsafe { libc::getuid() };
        std::fs::write(
            process_root.join("status"),
            format!("Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n"),
        )
        .unwrap();
        std::fs::write(process_root.join("cmdline"), b"binary\0").unwrap();
        std::fs::write(root.join("target"), b"target").unwrap();
        symlink(root.join("target"), process_root.join("exe")).unwrap();
        symlink(root.join("target"), process_root.join("fd/5")).unwrap();
        std::fs::write(process_root.join("net/tcp"), b"local_address st\n").unwrap();
        std::fs::write(process_root.join("net/tcp6"), b"local_address st\n").unwrap();

        let proc = ProcDir::open_root(&root).unwrap();
        let (process, _) = proc.process(self_pid).unwrap();
        for allowed in [b"stat".as_slice(), b"status", b"cmdline", b"fd"] {
            assert!(proc.open_child(&process, allowed, libc::O_RDONLY, self_pid).is_ok());
        }
        for forbidden in [b"environ".as_slice(), b"mem", b"maps", b"cwd", b"root"] {
            assert!(proc.open_child(&process, forbidden, libc::O_RDONLY, self_pid).is_err());
        }
        let global = global_snapshot(&proc).unwrap();
        assert_eq!(global.generations.len(), 1);
        let snapshot = process_snapshot(&proc, &global.generations[0], b"/unmatched").unwrap();
        assert_eq!(snapshot.generation.pid, self_pid);
        assert!(!snapshot.config_argument);
        std::fs::rename(process_root.join("stat"), process_root.join("stat.original")).unwrap();
        symlink(process_root.join("stat.original"), process_root.join("stat")).unwrap();
        assert!(proc.open_child(&process, b"stat", libc::O_RDONLY, self_pid).is_err());
        assert!(proc.executable_metadata(&process, self_pid).is_ok());
        let (descriptors, numbers) = proc.descriptors(&process, self_pid).unwrap();
        assert_eq!(numbers, vec![5]);
        assert!(proc.descriptor_metadata(&descriptors, self_pid, 5).is_ok());
        assert!(proc.table(b"tcp", 1024).is_ok());
        assert!(proc.table(b"tcp6", 1024).is_ok());
        assert!(proc.table(b"udp", 1024).is_err());
        let audit = proc.audit();
        assert!(audit.contains(&ProcAudit::FollowExecutable(self_pid)));
        assert!(audit.contains(&ProcAudit::FollowDescriptor(self_pid, 5)));
        assert!(!audit.iter().any(|entry| match entry {
            ProcAudit::Open(path) | ProcAudit::Read(path) | ProcAudit::ReadDir(path) =>
                [b"environ".as_slice(), b"mem", b"maps", b"stack", b"cwd", b"root", b"map_files"]
                    .iter().any(|part| path.split(|byte| *byte == b'/').any(|value| value == *part)),
            _ => false,
        }));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unprivileged_real_proc_lookup_uses_retained_dirfd_boundary() {
        let held = File::open("/dev/null").unwrap();
        let held_number = u32::try_from(held.as_raw_fd()).unwrap();
        let proc = ProcDir::open_fixed().unwrap();
        let pid = std::process::id();
        let (process, metadata) = proc.process(pid).unwrap();
        assert!(process_starttime(&proc, &process, pid).unwrap() > 0);
        assert_eq!(process_uid(&proc, &process, pid).unwrap(), metadata.uid);
        assert_eq!(file_type(proc.executable_metadata(&process, pid).unwrap().mode), libc::S_IFREG);
        let (directory, numbers) = proc.descriptors(&process, pid).unwrap();
        assert!(numbers.contains(&held_number));
        proc.descriptor_metadata(&directory, pid, held_number).unwrap();
        assert!(proc.table(b"tcp", 4 * 1024 * 1024).is_ok());
        assert!(proc.table(b"tcp6", 4 * 1024 * 1024).is_ok());
        assert!(proc.root_pids().unwrap().contains(&pid));
        let audit = proc.audit();
        assert_eq!(audit.iter().filter(|entry| matches!(entry, ProcAudit::FollowExecutable(value) if *value == pid)).count(), 1);
        assert_eq!(audit.iter().filter(|entry| matches!(entry, ProcAudit::FollowDescriptor(value, number) if *value == pid && *number == held_number)).count(), 1);
    }

    #[test]
    fn retained_artifact_rejects_path_replacement_and_missing_capability() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("t050-artifact-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = root.join("artifact");
        std::fs::write(&path, b"first").unwrap();
        let retained = secure_open(&path, libc::O_RDONLY).unwrap();
        let metadata = retained.metadata().unwrap();
        let hash: [u8;32] = Sha256::digest(b"first").into();
        assert!(capability_tuple(&retained).is_err());
        let replacement = root.join("replacement");
        std::fs::write(&replacement, b"second").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert!(reopen_matches(&path, &retained, &metadata, 32, hash).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn syscall_profiles_include_frozen_denials_and_output_minimum() {
        for syscall in [libc::SYS_ptrace, libc::SYS_process_vm_readv, libc::SYS_pidfd_getfd,
            libc::SYS_socket, libc::SYS_mount, libc::SYS_execve, libc::SYS_kill,
            libc::SYS_write, libc::SYS_clone]
        {
            assert!(DENIED_SYSCALLS.contains(&syscall));
        }
    }

    #[test]
    fn parent_command_shape_rejects_extra_or_reordered_flags() {
        let valid: &[&[u8]] = &[b"/binary", b"migrate-isolated-legacy-fixture", b"--manifest", b"/manifest"];
        assert!(command_shape(valid, 2));
        let extra: &[&[u8]] = &[b"/binary", b"migrate-isolated-legacy-fixture", b"--manifest", b"/manifest", b"--extra", b"value"];
        assert!(!command_shape(extra, 2));
        let wrong: &[&[u8]] = &[b"/binary", b"reconcile-isolated-legacy-owner", b"--manifest", b"/manifest"];
        assert!(!command_shape(wrong, 2));
        assert!(!command_shape(wrong, 4));
    }
}
