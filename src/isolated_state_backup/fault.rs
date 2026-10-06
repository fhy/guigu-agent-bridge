#[cfg(test)]
thread_local! {
    static POINT: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
    static ACTION: std::cell::RefCell<Option<(&'static str, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn hit(point: &'static str) -> Result<(), ()> {
    #[cfg(test)]
    {
        if std::env::var("T050_BACKUP_CRASH_POINT").as_deref() == Ok(point) {
            unsafe { libc::_exit(86) }
        }
        let action = ACTION.with(|slot| {
            let matches = slot
                .borrow()
                .as_ref()
                .is_some_and(|(name, _)| *name == point);
            matches.then(|| slot.borrow_mut().take().expect("checked action").1)
        });
        if let Some(action) = action {
            action();
        }
        return POINT.with(|slot| {
            if *slot.borrow() == Some(point) {
                slot.replace(None);
                Err(())
            } else {
                Ok(())
            }
        });
    }
    #[cfg(not(test))]
    {
        let _ = point;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn once(point: &'static str) {
    POINT.with(|slot| slot.replace(Some(point)));
}

#[cfg(test)]
pub(crate) fn action(point: &'static str, action: impl FnOnce() + 'static) {
    ACTION.with(|slot| slot.replace(Some((point, Box::new(action)))));
}
