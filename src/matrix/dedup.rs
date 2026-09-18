use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct EventDedup {
    capacity: usize,
    order: VecDeque<(String, String)>,
    seen: HashSet<(String, String)>,
}

impl EventDedup {
    pub fn new(capacity: usize) -> Result<Self, super::MatrixError> {
        if capacity == 0 {
            return Err(super::MatrixError::Configuration);
        }
        Ok(Self {
            capacity,
            order: VecDeque::new(),
            seen: HashSet::new(),
        })
    }
    pub fn contains(&self, room: &str, event: &str) -> bool {
        self.seen.contains(&(room.into(), event.into()))
    }
    pub fn mark(&mut self, room: &str, event: &str) -> bool {
        let key = (room.to_owned(), event.to_owned());
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}
