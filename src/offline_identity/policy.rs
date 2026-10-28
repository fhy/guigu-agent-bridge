use sha2::{Digest, Sha256};
#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;

const LOAD: u16 = 0x20;
const EQ: u16 = 0x15;
const SET: u16 = 0x45;
const AND: u16 = 0x54;
const RET: u16 = 0x06;
const ALLOW: u32 = 0x7fff0000;
const DENY: u32 = 0x00050000 | libc::EPERM as u32;
const REQUIRED_THREADS: u32 = 0x100 | 0x800 | 0x10000;
const OPTIONAL_THREADS: u32 = 0x200 | 0x400 | 0x40000 | 0x80000 | 0x100000 | 0x200000 | 0x1000000;
const ORDINARY: &[u32] = &[
    0, 1, 3, 5, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 24, 25, 28, 32, 33, 35, 39, 60,
    63, 72, 73, 74, 75, 76, 77, 82, 83, 87, 89, 90, 91, 95, 96, 97, 98, 102, 104, 107, 108, 131,
    137, 138, 186, 202, 204, 217, 218, 219, 228, 229, 230, 231, 232, 233, 257, 258, 262, 263, 264,
    267, 268, 269, 273, 281, 290, 291, 292, 293, 302, 316, 318, 332, 334, 436, 439, 441, 449,
];

fn instruction(code: u16, value: u32, yes: u8, no: u8) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: yes,
        jf: no,
        k: value,
    }
}

fn deny_unless_equal(offset: u32, value: u32) -> Vec<libc::sock_filter> {
    vec![
        instruction(LOAD, offset, 0, 0),
        instruction(EQ, value, 1, 0),
        instruction(RET, DENY, 0, 0),
    ]
}

fn special(filter: &mut Vec<libc::sock_filter>, number: u32, body: Vec<libc::sock_filter>) {
    filter.push(instruction(EQ, number, 0, body.len() as u8));
    filter.extend(body);
}

fn program(process: u32) -> Vec<libc::sock_filter> {
    let mut filter = vec![
        instruction(LOAD, 4, 0, 0),
        instruction(EQ, 0xc000003e, 1, 0),
        instruction(RET, 0x80000000, 0, 0),
        instruction(LOAD, 0, 0, 0),
        instruction(SET, 0x40000000, 0, 1),
        instruction(RET, DENY, 0, 0),
    ];
    for number in ORDINARY {
        filter.push(instruction(EQ, *number, 0, 1));
        filter.push(instruction(RET, ALLOW, 0, 0));
    }
    let mut prctl = deny_unless_equal(20, 0);
    prctl.extend([
        instruction(LOAD, 16, 0, 0),
        instruction(EQ, libc::PR_GET_NO_NEW_PRIVS as u32, 2, 0),
        instruction(EQ, libc::PR_GET_SECCOMP as u32, 1, 0),
        instruction(RET, DENY, 0, 0),
        instruction(RET, ALLOW, 0, 0),
    ]);
    special(&mut filter, 157, prctl);
    let mut threads = deny_unless_equal(20, 0);
    threads.extend([
        instruction(LOAD, 16, 0, 0),
        instruction(AND, REQUIRED_THREADS, 0, 0),
        instruction(EQ, REQUIRED_THREADS, 1, 0),
        instruction(RET, DENY, 0, 0),
        instruction(LOAD, 16, 0, 0),
        instruction(SET, !(REQUIRED_THREADS | OPTIONAL_THREADS), 0, 1),
        instruction(RET, DENY, 0, 0),
        instruction(RET, ALLOW, 0, 0),
    ]);
    special(&mut filter, 56, threads);
    let mut signals = deny_unless_equal(20, 0);
    signals.extend(deny_unless_equal(16, process));
    signals.extend(deny_unless_equal(36, 0));
    signals.push(instruction(LOAD, 32, 0, 0));
    for signal in [4, 6, 7, 8, 10, 11, 12, 13, 15] {
        signals.push(instruction(EQ, signal, 0, 1));
        signals.push(instruction(RET, ALLOW, 0, 0));
    }
    signals.push(instruction(RET, DENY, 0, 0));
    special(&mut filter, 234, signals);
    special(
        &mut filter,
        435,
        vec![instruction(RET, 0x00050000 | libc::ENOSYS as u32, 0, 0)],
    );
    filter.push(instruction(RET, DENY, 0, 0));
    filter
}

pub(super) fn digest() -> String {
    let mut hash = Sha256::new();
    hash.update(b"R092-linux-x86_64-policy-v1\0");
    for entry in program(0) {
        hash.update(entry.code.to_le_bytes());
        hash.update([entry.jt, entry.jf]);
        hash.update(entry.k.to_le_bytes());
    }
    format!("{:x}", hash.finalize())
}

pub(super) fn install() -> Result<(), ()> {
    if !cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_pointer_width = "64"
    )) {
        return Err(());
    }
    let filter = program(std::process::id());
    let descriptor = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr().cast_mut(),
    };
    let installed = unsafe {
        libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0
            && libc::syscall(libc::SYS_seccomp, 1, 0, &descriptor) == 0
    };
    installed.then_some(()).ok_or(())
}

pub(super) fn verify() -> Result<(), ()> {
    let readback = unsafe {
        libc::prctl(libc::PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) == 1
            && libc::prctl(libc::PR_GET_SECCOMP, 0, 0, 0, 0) == 2
    };
    if !readback {
        return Err(());
    }
    for family in [libc::AF_INET, libc::AF_INET6, libc::AF_UNIX] {
        denied(unsafe { libc::socket(family, libc::SOCK_STREAM, 0) } as libc::c_long)?;
    }
    let mut descriptors = [-1; 2];
    denied(unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM,
            0,
            descriptors.as_mut_ptr(),
        )
    } as libc::c_long)?;
    denied(unsafe { libc::syscall(libc::SYS_io_uring_setup, 1, std::ptr::null::<u8>()) })?;
    denied(unsafe { libc::syscall(libc::SYS_pidfd_getfd, -1, 0, 0) })?;
    Ok(())
}

fn denied(result: libc::c_long) -> Result<(), ()> {
    (result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM))
        .then_some(())
        .ok_or(())
}
