//! Liveness belongs to the owned future, including an unpolled aborted future.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) struct RequiredOwner {
    alive: Arc<AtomicBool>,
    class: &'static str,
    on_exit: Option<Box<dyn FnOnce(&'static str) + Send>>,
}

impl RequiredOwner {
    pub(crate) fn observed(
        alive: Arc<AtomicBool>,
        on_exit: impl FnOnce(&'static str) + Send + 'static,
    ) -> Self {
        Self {
            alive,
            class: "unexpected-exit",
            on_exit: Some(Box::new(on_exit)),
        }
    }
}

impl RequiredOwner {
    pub(crate) fn fail(&mut self, class: &'static str) {
        self.class = class;
        if let Some(on_exit) = self.on_exit.take() {
            on_exit(class);
        }
        self.alive.store(false, Ordering::Release);
    }
}

impl Drop for RequiredOwner {
    fn drop(&mut self) {
        // Record cause before publishing dead, so a health reader sees both.
        if let Some(on_exit) = self.on_exit.take() {
            on_exit(self.class);
        }
        self.alive.store(false, Ordering::Release);
    }
}
