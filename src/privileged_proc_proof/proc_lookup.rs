// SPLIT_PROC_LOOKUP
const SAFE_RESOLVE: u64 = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProcMetadata {
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum ProcAudit {
    Open(Vec<u8>),
    Read(Vec<u8>),
    ReadDir(Vec<u8>),
    FollowExecutable(u32),
    FollowDescriptor(u32, u32),
}

struct ProcDir {
    root: OwnedFd,
    #[cfg(test)]
    audit: std::cell::RefCell<Vec<ProcAudit>>,
}

impl ProcDir {
    fn open_fixed() -> Result<Self, ()> {
        Self::open_root(Path::new("/proc"))
    }

    fn open_root(root: &Path) -> Result<Self, ()> {
        use std::os::unix::fs::OpenOptionsExt;
        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(root)
            .map_err(|_| ())?;
        Ok(Self {
            root: root.into(),
            #[cfg(test)]
            audit: std::cell::RefCell::new(Vec::new()),
        })
    }

    #[cfg(test)]
    fn record(&self, entry: ProcAudit) {
        self.audit.borrow_mut().push(entry);
    }

    fn component(value: &[u8]) -> Result<CString, ()> {
        if value.is_empty()
            || value.contains(&b'/')
            || value.contains(&0)
            || value == b"."
            || value == b".."
        {
            return Err(());
        }
        CString::new(value).map_err(|_| ())
    }

    fn decimal(value: u32) -> Vec<u8> {
        value.to_string().into_bytes()
    }

    fn openat2(parent: RawFd, component: &[u8], flags: i32) -> Result<OwnedFd, ()> {
        let component = Self::component(component)?;
        let how = OpenHow {
            flags: (flags | libc::O_CLOEXEC) as u64,
            mode: 0,
            resolve: SAFE_RESOLVE,
        };
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                parent,
                component.as_ptr(),
                &how,
                std::mem::size_of::<OpenHow>(),
            )
        };
        if fd < 0 {
            Err(())
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
        }
    }

    fn metadata_at(parent: RawFd, component: &[u8], flags: i32) -> Result<ProcMetadata, ()> {
        let component = Self::component(component)?;
        let mut value = std::mem::MaybeUninit::<libc::stat>::zeroed();
        if unsafe { libc::fstatat(parent, component.as_ptr(), value.as_mut_ptr(), flags) } != 0 {
            return Err(());
        }
        let value = unsafe { value.assume_init() };
        Ok(ProcMetadata {
            device: value.st_dev,
            inode: value.st_ino,
            mode: value.st_mode,
            uid: value.st_uid,
        })
    }

    fn process(&self, pid: u32) -> Result<(OwnedFd, ProcMetadata), ()> {
        let name = Self::decimal(pid);
        #[cfg(test)]
        self.record(ProcAudit::Open(name.clone()));
        let fd = Self::openat2(self.root.as_raw_fd(), &name, libc::O_RDONLY | libc::O_DIRECTORY)?;
        let metadata = Self::metadata_fd(&fd)?;
        Ok((fd, metadata))
    }

    fn metadata_fd(fd: &OwnedFd) -> Result<ProcMetadata, ()> {
        let mut value = std::mem::MaybeUninit::<libc::stat>::zeroed();
        if unsafe { libc::fstat(fd.as_raw_fd(), value.as_mut_ptr()) } != 0 {
            return Err(());
        }
        let value = unsafe { value.assume_init() };
        Ok(ProcMetadata {
            device: value.st_dev,
            inode: value.st_ino,
            mode: value.st_mode,
            uid: value.st_uid,
        })
    }

    fn open_child(&self, process: &OwnedFd, name: &[u8], flags: i32, pid: u32) -> Result<OwnedFd, ()> {
        if !matches!(name, b"stat" | b"status" | b"cmdline" | b"fd") {
            return Err(());
        }
        let mut path = Self::decimal(pid);
        path.push(b'/');
        path.extend_from_slice(name);
        #[cfg(test)]
        self.record(ProcAudit::Open(path));
        Self::openat2(process.as_raw_fd(), name, flags)
    }

    fn read_child(&self, process: &OwnedFd, name: &[u8], pid: u32, max: usize) -> Result<Vec<u8>, ()> {
        let mut path = Self::decimal(pid);
        path.push(b'/');
        path.extend_from_slice(name);
        #[cfg(test)]
        self.record(ProcAudit::Read(path));
        let fd = self.open_child(process, name, libc::O_RDONLY, pid)?;
        read_fd_bounded(fd, max)
    }

    fn table(&self, name: &[u8], max: usize) -> Result<Vec<u8>, ()> {
        if !matches!(name, b"tcp" | b"tcp6") {
            return Err(());
        }
        let pid = std::process::id();
        let (process, _) = self.process(pid)?;
        let mut net_path = Self::decimal(pid);
        net_path.extend_from_slice(b"/net");
        #[cfg(test)]
        self.record(ProcAudit::Open(net_path));
        let net = Self::openat2(process.as_raw_fd(), b"net", libc::O_RDONLY | libc::O_DIRECTORY)?;
        let mut path = Self::decimal(pid);
        path.extend_from_slice(b"/net/");
        path.extend_from_slice(name);
        #[cfg(test)]
        self.record(ProcAudit::Read(path));
        let fd = Self::openat2(net.as_raw_fd(), name, libc::O_RDONLY)?;
        read_fd_bounded(fd, max)
    }

    fn executable_metadata(&self, process: &OwnedFd, _pid: u32) -> Result<ProcMetadata, ()> {
        #[cfg(test)]
        self.record(ProcAudit::FollowExecutable(_pid));
        Self::metadata_at(process.as_raw_fd(), b"exe", 0)
    }

    fn descriptor_metadata(&self, directory: &OwnedFd, _pid: u32, number: u32) -> Result<ProcMetadata, ()> {
        #[cfg(test)]
        self.record(ProcAudit::FollowDescriptor(_pid, number));
        Self::metadata_at(directory.as_raw_fd(), &Self::decimal(number), 0)
    }

    fn root_pids(&self) -> Result<Vec<u32>, ()> {
        #[cfg(test)]
        self.record(ProcAudit::ReadDir(Vec::new()));
        let directory = unsafe { libc::fcntl(self.root.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 4) };
        if directory < 0 { return Err(()); }
        let mut values = read_directory(unsafe { OwnedFd::from_raw_fd(directory) })?
            .into_iter()
            .filter_map(|name| parse_decimal(&name))
            .collect::<Vec<_>>();
        values.sort_unstable();
        values.dedup();
        Ok(values)
    }

    fn descriptors(&self, process: &OwnedFd, pid: u32) -> Result<(OwnedFd, Vec<u32>), ()> {
        let directory = self.open_child(process, b"fd", libc::O_RDONLY | libc::O_DIRECTORY, pid)?;
        let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 4) };
        if duplicate < 0 { return Err(()); }
        let mut path = Self::decimal(pid);
        path.extend_from_slice(b"/fd");
        #[cfg(test)]
        self.record(ProcAudit::ReadDir(path));
        let mut values = read_directory(unsafe { OwnedFd::from_raw_fd(duplicate) })?
            .into_iter()
            .map(|name| parse_decimal(&name).ok_or(()))
            .collect::<Result<Vec<_>, _>>()?;
        values.sort_unstable();
        Ok((directory, values))
    }

    #[cfg(test)]
    fn audit(&self) -> Vec<ProcAudit> {
        self.audit.borrow().clone()
    }
}

fn parse_decimal(value: &[u8]) -> Option<u32> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) { return None; }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn read_fd_bounded(fd: OwnedFd, max: usize) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    File::from(fd).take((max + 1) as u64).read_to_end(&mut bytes).map_err(|_| ())?;
    if bytes.len() > max { Err(()) } else { Ok(bytes) }
}

fn read_directory(fd: OwnedFd) -> Result<Vec<Vec<u8>>, ()> {
    let mut result = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = unsafe { libc::syscall(libc::SYS_getdents64, fd.as_raw_fd(), buffer.as_mut_ptr(), buffer.len()) };
        if count < 0 { return Err(()); }
        if count == 0 { break; }
        let mut offset = 0usize;
        while offset < count as usize {
            if offset + 19 > count as usize { return Err(()); }
            let record = &buffer[offset..count as usize];
            let reclen = u16::from_ne_bytes([record[16], record[17]]) as usize;
            if reclen < 20 || offset + reclen > count as usize { return Err(()); }
            let name = &buffer[offset + 19..offset + reclen];
            let end = name.iter().position(|byte| *byte == 0).ok_or(())?;
            if &name[..end] != b"." && &name[..end] != b".." { result.push(name[..end].to_vec()); }
            offset += reclen;
        }
    }
    Ok(result)
}
