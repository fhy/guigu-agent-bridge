use crate::{Refusal, Result, contract::NamespaceSpec, supervision::ChildCertificate};
use serde::{Deserialize, Serialize};
use std::{fs::File, os::fd::AsRawFd};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerFact {
    pub pid: u32,
    pub ticks: u64,
    pub inode: u64,
    pub port: u16,
    pub loopback: bool,
    pub listening: bool,
}
pub fn verify(
    spec: &NamespaceSpec,
    child: &ChildCertificate,
    listeners: &[ListenerFact],
) -> Result<()> {
    if listeners.len() != 2 {
        return Err(Refusal::Readiness);
    }
    for (index, expected) in spec.ports().iter().enumerate() {
        let facts = &listeners[index];
        if facts.pid != child.pid
            || facts.ticks != child.start_ticks
            || facts.inode == 0
            || facts.port != *expected
            || !facts.loopback
            || !facts.listening
        {
            return Err(Refusal::Readiness);
        }
    }
    if listeners[0].inode == listeners[1].inode {
        return Err(Refusal::Readiness);
    }
    Ok(())
}
/// Called only on an original broker-created/retained socket, not a foreign FD.
pub fn socket_fact(file: &File, pid: u32, ticks: u64) -> Result<ListenerFact> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(|_| Refusal::Readiness)?;
    if metadata.mode() & libc::S_IFMT != libc::S_IFSOCK {
        return Err(Refusal::Readiness);
    }
    let mut address = std::mem::MaybeUninit::<libc::sockaddr_in>::zeroed();
    let mut length = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    if unsafe { libc::getsockname(file.as_raw_fd(), address.as_mut_ptr().cast(), &mut length) } != 0
    {
        return Err(Refusal::Readiness);
    }
    let address = unsafe { address.assume_init() };
    let mut listening: i32 = 0;
    let mut option_length = std::mem::size_of::<i32>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            file.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            (&mut listening as *mut i32).cast(),
            &mut option_length,
        )
    } != 0
    {
        return Err(Refusal::Readiness);
    }
    Ok(ListenerFact {
        pid,
        ticks,
        inode: metadata.ino(),
        port: u16::from_be(address.sin_port),
        loopback: address.sin_family == libc::AF_INET as u16
            && address.sin_addr.s_addr.to_ne_bytes() == [127, 0, 0, 1],
        listening: listening == 1,
    })
}
