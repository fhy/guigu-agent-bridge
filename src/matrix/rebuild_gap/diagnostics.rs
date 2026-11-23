use crate::matrix::MatrixError;

/// Only static predicates and numeric context cross the diagnostic boundary.
#[derive(Debug)]
pub(crate) struct Failure {
    pub reason: &'static str,
    pub error: MatrixError,
    page: usize,
    events: usize,
    bytes: usize,
}

impl Failure {
    pub(crate) fn new(reason: &'static str) -> Self {
        Self {
            reason,
            error: MatrixError::CutoverRefused,
            page: 0,
            events: 0,
            bytes: 0,
        }
    }
    pub(crate) fn storage(reason: &'static str) -> Self {
        Self {
            error: MatrixError::Storage,
            ..Self::new(reason)
        }
    }
    pub(crate) fn transport(reason: &'static str) -> Self {
        Self {
            error: MatrixError::Transport,
            ..Self::new(reason)
        }
    }
    pub(super) fn at(mut self, page: usize, events: usize) -> Self {
        self.page = page;
        self.events = events;
        self
    }
    pub(super) fn bytes(mut self, bytes: usize) -> Self {
        self.bytes = bytes;
        self
    }
    pub(crate) fn report(&self) {
        tracing::error!(
            stage = "rebuild-state",
            category = "history-gap",
            predicate = self.reason,
            page = self.page,
            events = self.events,
            bytes = self.bytes,
            committed = false,
            "bounded rebuild first failure"
        );
    }
    pub(crate) fn reported(self) -> MatrixError {
        self.report();
        self.error
    }
}
