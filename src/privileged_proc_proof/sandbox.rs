// SPLIT_SANDBOX
#[repr(C)]
struct CapHeader {
    version: u32,
    pid: i32,
}
#[repr(C)]
#[derive(Clone, Copy)]
struct CapData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

fn status_capabilities_zero(proc: &ProcDir) -> Result<bool, ()> {
    let pid = std::process::id();
    let (process, _) = proc.process(pid)?;
    let bytes = proc.read_child(&process, b"status", pid, 64 * 1024)?;
    let status = std::str::from_utf8(&bytes).map_err(|_| ())?;
    let mut seen = 0u8;
    for line in status.lines() {
        if let Some((name, value)) = line.split_once(':')
            && matches!(name, "CapEff" | "CapPrm" | "CapInh" | "CapAmb")
        {
            if u64::from_str_radix(value.trim(), 16).map_err(|_| ())? != 0 {
                return Ok(false);
            }
            seen += 1;
        }
    }
    Ok(seen == 4)
}

fn capability_sets() -> Result<[CapData; 2], ()> {
    let mut header = CapHeader {
        version: 0x2008_0522,
        pid: 0,
    };
    let mut data = [CapData {
        effective: 0,
        permitted: 0,
        inheritable: 0,
    }; 2];
    if unsafe { libc::syscall(libc::SYS_capget, &mut header, data.as_mut_ptr()) } != 0 {
        return Err(());
    }
    Ok(data)
}

fn verify_initial_capability(proc: &ProcDir) -> Result<(), ()> {
    let data = capability_sets()?;
    let pid = std::process::id();
    let (process, _) = proc.process(pid)?;
    let bytes = proc.read_child(&process, b"status", pid, 64 * 1024)?;
    let status = std::str::from_utf8(&bytes).map_err(|_| ())?;
    let ambient_zero = status
        .lines()
        .find_map(|line| line.strip_prefix("CapAmb:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
        == Some(0);
    if !ambient_zero
        || data[0].effective != 1 << 19
        || data[0].permitted != 1 << 19
        || data[0].inheritable != 0
        || data[1].effective != 0
        || data[1].permitted != 0
        || data[1].inheritable != 0
    {
        Err(())
    } else {
        Ok(())
    }
}

fn drop_capabilities(proc: &ProcDir) -> Result<(), ()> {
    let mut header = CapHeader {
        version: 0x2008_0522,
        pid: 0,
    };
    let data = [CapData {
        effective: 0,
        permitted: 0,
        inheritable: 0,
    }; 2];
    if unsafe { libc::syscall(libc::SYS_capset, &mut header, data.as_ptr()) } != 0 {
        return Err(());
    }
    if unsafe {
        libc::prctl(
            libc::PR_CAP_AMBIENT,
            libc::PR_CAP_AMBIENT_CLEAR_ALL,
            0,
            0,
            0,
        )
    } != 0
    {
        return Err(());
    }
    let current = capability_sets()?;
    if !status_capabilities_zero(proc)?
        || current
            .iter()
            .any(|set| set.effective != 0 || set.permitted != 0 || set.inheritable != 0)
    {
        Err(())
    } else {
        Ok(())
    }
}

const HELPER_CPU_SECONDS: libc::rlim_t = 60;

fn set_resource_limits() -> Result<(), ()> {
    for (resource, value) in [
        (libc::RLIMIT_NOFILE, 65_536),
        (libc::RLIMIT_AS, 512 * 1024 * 1024),
        (libc::RLIMIT_FSIZE, 0),
        // Authentication streams the parent and helper before and after the scan.
        // The reviewed artifacts may each be as large as MAX_HELPER_SIZE, so five
        // seconds can terminate a healthy helper before it emits its proof record.
        (libc::RLIMIT_CPU, HELPER_CPU_SECONDS),
    ] {
        let limit = libc::rlimit {
            rlim_cur: value,
            rlim_max: value,
        };
        if unsafe { libc::setrlimit(resource, &limit) } != 0 {
            return Err(());
        }
    }
    Ok(())
}

fn entry_fd_table_is_exact() -> bool {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return false;
    }
    let upper = limit.rlim_cur.min(1_048_576) as RawFd;
    (0..upper).all(|fd| {
        let open = unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0;
        !open || fd <= 3
    })
}

const DENIED_SYSCALLS: &[libc::c_long] = &[
    libc::SYS_ptrace, libc::SYS_process_vm_readv, libc::SYS_process_vm_writev,
    libc::SYS_pidfd_getfd, libc::SYS_socket, libc::SYS_connect, libc::SYS_bind,
    libc::SYS_listen, libc::SYS_accept, libc::SYS_accept4, libc::SYS_mount,
    libc::SYS_umount2, libc::SYS_unshare, libc::SYS_setns, libc::SYS_clone,
    libc::SYS_clone3, libc::SYS_fork, libc::SYS_vfork, libc::SYS_execve,
    libc::SYS_execveat, libc::SYS_kill, libc::SYS_tkill, libc::SYS_tgkill,
    libc::SYS_unlink, libc::SYS_unlinkat, libc::SYS_rename, libc::SYS_renameat,
    libc::SYS_renameat2, libc::SYS_mkdir, libc::SYS_mkdirat, libc::SYS_rmdir,
    libc::SYS_creat, libc::SYS_truncate, libc::SYS_ftruncate, libc::SYS_write,
    libc::SYS_writev, libc::SYS_pwrite64, libc::SYS_chmod, libc::SYS_fchmod,
    libc::SYS_fchmodat, libc::SYS_chown, libc::SYS_fchown, libc::SYS_fchownat,
    libc::SYS_setuid, libc::SYS_setgid, libc::SYS_setreuid, libc::SYS_setregid,
    libc::SYS_setresuid, libc::SYS_setresgid,
];

#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xc000_00b7;

fn seccomp_prefix() -> Vec<libc::sock_filter> {
    vec![
        libc::sock_filter { code:0x20, jt:0, jf:0, k:4 },
        libc::sock_filter { code:0x15, jt:1, jf:0, k:AUDIT_ARCH },
        libc::sock_filter { code:0x06, jt:0, jf:0, k:0x8000_0000 },
        libc::sock_filter { code:0x20, jt:0, jf:0, k:0 },
    ]
}

fn install_deny_seccomp() -> Result<(), ()> {
    const KILL: u32 = 0x8000_0000;
    const ALLOW: u32 = 0x7fff_0000;
    let mut filters = seccomp_prefix();
    for syscall in DENIED_SYSCALLS {
        filters.push(libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: *syscall as u32,
        });
        filters.push(libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: KILL,
        });
    }
    filters.push(libc::sock_filter {
        code: 0x06,
        jt: 0,
        jf: 0,
        k: ALLOW,
    });
    let program = libc::sock_fprog {
        len: filters.len() as u16,
        filter: filters.as_mut_ptr(),
    };
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0
        || unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) } != 0
    {
        Err(())
    } else {
        Ok(())
    }
}

fn install_output_seccomp() -> Result<(), ()> {
    const KILL: u32 = 0x8000_0000;
    const ALLOW: u32 = 0x7fff_0000;
    let allowed = [
        libc::SYS_sendto,
        libc::SYS_close,
        libc::SYS_exit,
        libc::SYS_exit_group,
        libc::SYS_rt_sigreturn,
    ];
    let mut filters = seccomp_prefix();
    for syscall in allowed {
        filters.push(libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: syscall as u32,
        });
        filters.push(libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: ALLOW,
        });
    }
    filters.push(libc::sock_filter {
        code: 0x06,
        jt: 0,
        jf: 0,
        k: KILL,
    });
    let program = libc::sock_fprog {
        len: filters.len() as u16,
        filter: filters.as_mut_ptr(),
    };
    if unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) } != 0 {
        Err(())
    } else {
        Ok(())
    }
}
