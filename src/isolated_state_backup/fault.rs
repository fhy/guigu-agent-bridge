#[cfg(test)]
type Action = Option<(&'static str, Box<dyn FnOnce()>)>;

#[cfg(test)]
thread_local! {
    static POINT: std::cell::RefCell<Option<(&'static str, usize)>> = const { std::cell::RefCell::new(None) };
    static ACTION: std::cell::RefCell<Action> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn hit(point: &'static str) -> Result<(), ()> {
    #[cfg(test)]
    {
        if std::env::var("T050_BACKUP_CRASH_POINT").as_deref() == Ok(point) {
            unsafe { libc::_exit(86) }
        }
        if std::env::var("T050_BACKUP_FAIL_POINT").as_deref() == Ok(point) {
            return Err(());
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
        POINT.with(|slot| {
            let current = *slot.borrow();
            match current {
                Some((name, 1)) if name == point => {
                    slot.replace(None);
                    Err(())
                }
                Some((name, remaining)) if name == point => {
                    slot.replace(Some((name, remaining - 1)));
                    Ok(())
                }
                _ => Ok(()),
            }
        })
    }
    #[cfg(not(test))]
    {
        let _ = point;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn once(point: &'static str) {
    nth(point, 1);
}

#[cfg(test)]
pub(crate) fn nth(point: &'static str, occurrence: usize) {
    assert!(occurrence > 0);
    POINT.with(|slot| slot.replace(Some((point, occurrence))));
}

#[cfg(test)]
pub(crate) fn action(point: &'static str, action: impl FnOnce() + 'static) {
    ACTION.with(|slot| slot.replace(Some((point, Box::new(action)))));
}
