#[derive(Default)]
struct Publication {
    leaves: Vec<(String, super::codec::Identity)>,
}

impl Publication {
    fn arm(&mut self, namespace: &File, name: &str, file: &File) -> Result<(), ()> {
        super::fault::hit("before-publication-metadata")?;
        let identity = fs::identity(&file.metadata().map_err(|_| ())?)?;
        self.leaves.push((name.to_owned(), identity));
        let current = fs::open_leaf(namespace, name, false)?;
        let observed = fs::identity(&current.metadata().map_err(|_| ())?)?;
        if !fs::same(identity, observed) {
            return Err(());
        }
        super::fault::hit("after-publication-create")?;
        Ok(())
    }

    fn promote(&mut self, name: &str, file: &File) -> Result<(), ()> {
        let entry = self
            .leaves
            .iter_mut()
            .find(|(candidate, _)| candidate == name)
            .ok_or(())?;
        let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
        if !fs::same_object(entry.1, observed) {
            return Err(());
        }
        entry.1 = observed;
        Ok(())
    }

    fn rename(&mut self, old: &str, new: &str) {
        if let Some((name, _)) = self
            .leaves
            .iter_mut()
            .find(|(candidate, _)| candidate == old)
        {
            *name = new.to_owned();
        }
    }

    fn rollback(&mut self, namespace: &File) -> Result<(), ()> {
        while let Some((name, expected)) = self.leaves.pop() {
            let file = fs::open_leaf(namespace, &name, false)?;
            let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
            if !fs::same(expected, observed) {
                return Err(());
            }
            super::fault::hit("before-publication-rollback-unlink")?;
            fs::unlink(namespace, &name)?;
            super::fault::hit("after-publication-rollback-unlink")?;
            namespace.sync_all().map_err(|_| ())?;
            super::fault::hit("after-publication-rollback-fsync")?;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.leaves.clear();
    }
}
