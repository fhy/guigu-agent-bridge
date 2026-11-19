use std::process::Child;

pub struct RuntimeChild(pub Child);

impl std::ops::Deref for RuntimeChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for RuntimeChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for RuntimeChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[test]
fn readiness_panic_reaps_runtime_child() {
    let mut pid = 0;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        pid = child.id();
        let _owned = RuntimeChild(child);
        panic!("injected readiness failure");
    }));
    assert!(result.is_err());
    let mut status = 0;
    assert_eq!(
        unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}
