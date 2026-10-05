// SPLIT_SCAN
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Generation {
    pid: u32,
    starttime: u64,
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Descriptor {
    number: u32,
    device: u64,
    inode: u64,
    file_type: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Listener {
    family: u8,
    address: Vec<u8>,
    port: u16,
    inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessSnapshot {
    uid: u32,
    generation: Generation,
    cmdline_hash: [u8; 32],
    cmdline_len: usize,
    config_argument: bool,
    executable: (u64, u64, u32),
    descriptors: Vec<Descriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GlobalSnapshot {
    generations: Vec<Generation>,
    listeners: BTreeSet<Listener>,
}

fn file_type(mode: u32) -> u32 {
    mode & libc::S_IFMT
}

fn proc_literal(parent: &Path, name: &str) -> Result<PathBuf, ()> {
    if matches!(name, "stat" | "status" | "cmdline" | "exe" | "fd") {
        Ok(parent.join(name))
    } else {
        Err(())
    }
}

fn proc_table(root: &Path, name: &str) -> Result<PathBuf, ()> {
    if matches!(name, "tcp" | "tcp6") {
        Ok(root.join("net").join(name))
    } else {
        Err(())
    }
}

fn process_starttime(path: &Path) -> Result<u64, ()> {
    let text = std::fs::read_to_string(proc_literal(path, "stat")?).map_err(|_| ())?;
    let close = text.rfind(')').ok_or(())?;
    text[close + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or(())?
        .parse()
        .map_err(|_| ())
}

fn process_uid(path: &Path) -> Result<u32, ()> {
    let text = std::fs::read_to_string(proc_literal(path, "status")?).map_err(|_| ())?;
    let lines = text.lines().filter_map(|line| line.strip_prefix("Uid:")).collect::<Vec<_>>();
    let [line] = lines.as_slice() else { return Err(()); };
    let values = line.split_whitespace().map(str::parse::<u32>).collect::<Result<Vec<_>, _>>().map_err(|_| ())?;
    if values.len() != 4 { return Err(()); }
    Ok(values[1])
}

fn listeners(root: &Path) -> Result<BTreeSet<Listener>, ()> {
    let mut answer = BTreeSet::new();
    for (family, name) in [(4, "tcp"), (6, "tcp6")] {
        let text = std::fs::read_to_string(proc_table(root, name)?).map_err(|_| ())?;
        let mut lines = text.lines();
        let header = lines.next().ok_or(())?;
        if !header.split_whitespace().any(|field| field == "local_address")
            || !header.split_whitespace().any(|field| field == "st")
        { return Err(()); }
        for line in lines {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 10 {
                return Err(());
            }
            let port_text = fields[1].rsplit_once(':').ok_or(())?.1;
            if port_text.len() != 4 { return Err(()); }
            let port = u16::from_str_radix(port_text, 16).map_err(|_| ())?;
            if fields[3] == "0A" && matches!(port, 9291 | 9292) {
                let (address, _) = fields[1].rsplit_once(':').ok_or(())?;
                let expected = if family == 4 { 8 } else { 32 };
                if address.len() != expected || !address.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(());
                }
                answer.insert(Listener {
                    family,
                    address: address.as_bytes().to_vec(),
                    port,
                    inode: fields[9].parse().map_err(|_| ())?,
                });
            }
        }
    }
    Ok(answer)
}

fn global_snapshot(root: &Path) -> Result<GlobalSnapshot, ()> {
    let mut generations = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|_| ())? {
        let entry = entry.map_err(|_| ())?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let metadata = entry.metadata().map_err(|_| ())?;
        generations.push(Generation {
            pid,
            starttime: process_starttime(&entry.path())?,
            device: metadata.dev(),
            inode: metadata.ino(),
        });
    }
    generations.sort();
    Ok(GlobalSnapshot {
        generations,
        listeners: listeners(root)?,
    })
}

fn process_snapshot(
    root: &Path,
    generation: &Generation,
    config: &[u8],
) -> Result<ProcessSnapshot, ()> {
    let process = root.join(generation.pid.to_string());
    let metadata = std::fs::metadata(&process).map_err(|_| ())?;
    if metadata.dev() != generation.device
        || metadata.ino() != generation.inode
        || process_starttime(&process)? != generation.starttime
    {
        return Err(());
    }
    let uid = process_uid(&process)?;
    if metadata.uid() != uid {
        return Err(());
    }
    let mut cmdline = Vec::new();
    File::open(proc_literal(&process, "cmdline")?)
        .map_err(|_| ())?
        .take((MAX_CMDLINE + 1) as u64)
        .read_to_end(&mut cmdline)
        .map_err(|_| ())?;
    if cmdline.is_empty() || cmdline.len() > MAX_CMDLINE || cmdline.last() != Some(&0) {
        return Err(());
    }
    let cmdline_len = cmdline.len();
    let cmdline_hash = Sha256::digest(&cmdline).into();
    let config_argument = cmdline.split(|byte| *byte == 0).any(|part| part == config);
    cmdline.fill(0);
    let executable = std::fs::metadata(proc_literal(&process, "exe")?).map_err(|_| ())?;
    let mut descriptors = Vec::new();
    for entry in std::fs::read_dir(proc_literal(&process, "fd")?).map_err(|_| ())? {
        let entry = entry.map_err(|_| ())?;
        let number = entry
            .file_name()
            .to_str()
            .ok_or(())?
            .parse()
            .map_err(|_| ())?;
        let target = std::fs::metadata(entry.path()).map_err(|_| ())?;
        descriptors.push(Descriptor {
            number,
            device: target.dev(),
            inode: target.ino(),
            file_type: file_type(target.mode()),
        });
        if descriptors.len() > MAX_FDS {
            return Err(());
        }
    }
    descriptors.sort();
    if descriptors
        .windows(2)
        .any(|pair| pair[0].number == pair[1].number)
    {
        return Err(());
    }
    Ok(ProcessSnapshot {
        uid,
        generation: generation.clone(),
        cmdline_hash,
        cmdline_len,
        config_argument,
        executable: (
            executable.dev(),
            executable.ino(),
            file_type(executable.mode()),
        ),
        descriptors,
    })
}

fn pidfd_open(pid: u32) -> Result<OwnedFd, ()> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

fn pidfd_live(fd: &OwnedFd) -> bool {
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    (unsafe { libc::poll(&mut poll, 1, 0) }) == 0 && poll.revents == 0
}

fn stable_scan(root: &Path, policy: &Policy, ignored_pid: u32, deadline: Instant) -> ProofResult {
    for _ in 0..3 {
        if Instant::now() >= deadline {
            return ProofResult::Unknown;
        }
        let Ok(g0) = global_snapshot(root) else {
            continue;
        };
        let mut observations = Vec::new();
        let mut pidfds = Vec::new();
        let mut unstable = false;
        for generation in &g0.generations {
            if Instant::now() >= deadline { return ProofResult::Unknown; }
            let Ok(pidfd) = pidfd_open(generation.pid) else {
                unstable = true;
                break;
            };
            if !pidfd_live(&pidfd) {
                unstable = true;
                break;
            }
            let (Ok(p0), Ok(p1)) = (
                process_snapshot(root, generation, &policy.config.path),
                process_snapshot(root, generation, &policy.config.path),
            ) else {
                unstable = true;
                break;
            };
            if !pidfd_live(&pidfd) {
                unstable = true;
                break;
            }
            observations.push((p0, p1));
            pidfds.push(pidfd);
        }
        let Ok(g1) = global_snapshot(root) else {
            continue;
        };
        if unstable || pidfds.iter().any(|fd| !pidfd_live(fd)) {
            continue;
        }
        if let Some(result) = evaluate_stable(&g0, &g1, &observations, policy, ignored_pid) {
            return result;
        }
    }
    ProofResult::Unknown
}

fn evaluate_stable(
    g0: &GlobalSnapshot,
    g1: &GlobalSnapshot,
    observations: &[(ProcessSnapshot, ProcessSnapshot)],
    policy: &Policy,
    ignored_pid: u32,
) -> Option<ProofResult> {
    if g0 != g1
        || observations.len() != g0.generations.len()
        || observations.iter().any(|(p0, p1)| p0 != p1)
    {
        return None;
    }
    let mut found_listener = false;
    for (process, _) in observations {
        if process.generation.pid == ignored_pid {
            continue;
        }
        let executable = process.executable.0 == policy.binary.device
            && process.executable.1 == policy.binary.inode;
        let config_fd = process
            .descriptors
            .iter()
            .any(|fd| fd.device == policy.config.device && fd.inode == policy.config.inode);
        let listener_fd = process.descriptors.iter().any(|fd| {
            g0.listeners
                .iter()
                .any(|listener| listener.inode == fd.inode)
        });
        if executable && (process.config_argument || config_fd) {
            return Some(ProofResult::Process);
        }
        found_listener |= executable && listener_fd;
    }
    Some(if found_listener {
        ProofResult::Listener
    } else {
        ProofResult::Clean
    })
}

fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| ())?
        .take((max + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > max {
        Err(())
    } else {
        Ok(bytes)
    }
}

fn helper_deadline(milliseconds: u32) -> Instant {
    Instant::now() + Duration::from_millis(u64::from(milliseconds.min(5000)))
}
