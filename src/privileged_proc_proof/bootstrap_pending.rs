#[derive(Clone, Copy)]
enum ConstructionFailure {
    Clean,
    Ambiguous,
}

struct PendingDirectory<'a> {
    parent: &'a File,
    name: CString,
    identity: Option<Identity>,
    armed: bool,
}

impl PendingDirectory<'_> {
    fn cleanup(&mut self) -> Result<(), ConstructionFailure> {
        if !self.armed {
            return Ok(());
        }
        construction_fault("directory-cleanup-inspect")
            .map_err(|_| ConstructionFailure::Ambiguous)?;
        let identity = match self.identity {
            Some(identity) => identity,
            None => current_identity(self.parent, &self.name)
                .map_err(|()| ConstructionFailure::Ambiguous)?,
        };
        construction_fault("directory-cleanup-unlink")
            .map_err(|_| ConstructionFailure::Ambiguous)?;
        remove_created_directory(self.parent, &self.name, identity)
            .map_err(|()| ConstructionFailure::Ambiguous)?;
        self.armed = false;
        Ok(())
    }

    fn fail(mut self) -> ConstructionFailure {
        match self.cleanup() {
            Ok(()) => ConstructionFailure::Clean,
            Err(failure) => failure,
        }
    }
}

struct PendingLock<'a> {
    parent: &'a File,
    file: Option<File>,
    identity: Option<Identity>,
    armed: bool,
}

impl PendingLock<'_> {
    fn cleanup(&mut self) -> Result<(), ConstructionFailure> {
        if !self.armed {
            return Ok(());
        }
        let file = self.file.as_ref().ok_or(ConstructionFailure::Ambiguous)?;
        remove_created_lock(self.parent, file).map_err(|()| ConstructionFailure::Ambiguous)?;
        self.armed = false;
        Ok(())
    }

    fn fail(mut self) -> ConstructionFailure {
        match self.cleanup() {
            Ok(()) => ConstructionFailure::Clean,
            Err(failure) => failure,
        }
    }

    fn promote(mut self) -> (File, Identity) {
        let identity = self.identity.expect("validated lock identity is set");
        let file = self.file.take().expect("pending lock file is retained");
        self.armed = false;
        (file, identity)
    }
}
