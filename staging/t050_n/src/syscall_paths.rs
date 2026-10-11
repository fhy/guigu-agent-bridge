use crate::{Refusal, Result};

/// Read only bounded syscall pathname metadata of a child whose pidfd/start
/// certificate is already held. Never inspect argv, env or IO/secret buffers.
pub fn owned_path(pid: u32, pointer: u64) -> Result<std::path::PathBuf> {
    if pointer == 0 {
        return Err(Refusal::Path);
    }
    let mut bytes = Vec::new();
    for offset in 0..4096 {
        let mut byte = 0u8;
        let local = libc::iovec {
            iov_base: (&mut byte as *mut u8).cast(),
            iov_len: 1,
        };
        let remote = libc::iovec {
            iov_base: (pointer.checked_add(offset).ok_or(Refusal::Path)? as usize as *mut u8)
                .cast(),
            iov_len: 1,
        };
        if unsafe { libc::process_vm_readv(pid as i32, &local, 1, &remote, 1, 0) } != 1 {
            return Err(Refusal::Process);
        }
        if byte == 0 {
            let text = String::from_utf8(bytes).map_err(|_| Refusal::Path)?;
            return Ok(std::path::PathBuf::from(text));
        }
        bytes.push(byte);
    }
    Err(Refusal::Length)
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Data {
    pub nr: i32,
    pub arch: u32,
    pub instruction_pointer: u64,
    pub args: [u64; 6],
}
#[repr(C)]
#[derive(Default)]
pub struct Notification {
    pub id: u64,
    pub pid: u32,
    pub flags: u32,
    pub data: Data,
}
#[repr(C)]
#[derive(Default)]
pub struct Response {
    pub id: u64,
    pub val: i64,
    pub error: i32,
    pub flags: u32,
}
#[repr(C)]
pub struct AddFd {
    pub id: u64,
    pub flags: u32,
    pub srcfd: u32,
    pub newfd: u32,
    pub newfd_flags: u32,
}

pub const RECV: libc::c_ulong = 0xc0502100;
pub const SEND: libc::c_ulong = 0xc0182101;
pub const VALID: libc::c_ulong = 0x40082102;
pub const ADD_FD: libc::c_ulong = 0x40182103;

pub fn notify_syscalls() -> Vec<i64> {
    vec![
        libc::SYS_open,
        libc::SYS_openat,
        libc::SYS_openat2,
        libc::SYS_creat,
        libc::SYS_stat,
        libc::SYS_lstat,
        libc::SYS_newfstatat,
        libc::SYS_statx,
        libc::SYS_access,
        libc::SYS_faccessat,
        libc::SYS_faccessat2,
        libc::SYS_readlink,
        libc::SYS_readlinkat,
        libc::SYS_getdents,
        libc::SYS_getdents64,
        libc::SYS_mkdir,
        libc::SYS_mkdirat,
        libc::SYS_rmdir,
        libc::SYS_unlink,
        libc::SYS_unlinkat,
        libc::SYS_rename,
        libc::SYS_renameat,
        libc::SYS_renameat2,
        libc::SYS_link,
        libc::SYS_linkat,
        libc::SYS_symlink,
        libc::SYS_symlinkat,
        libc::SYS_truncate,
        libc::SYS_chmod,
        libc::SYS_chown,
        libc::SYS_lchown,
        libc::SYS_fchmodat,
        libc::SYS_fchownat,
        libc::SYS_execve,
        libc::SYS_execveat,
        libc::SYS_io_uring_setup,
        libc::SYS_open_by_handle_at,
        libc::SYS_name_to_handle_at,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_chdir,
        libc::SYS_fchdir,
    ]
}
