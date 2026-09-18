use std::collections::BTreeMap;

use crate::models::EndpointId;

#[derive(Debug, Clone, Default)]
pub struct PermissionPolicy {
    users: BTreeMap<String, Option<EndpointId>>,
}

impl PermissionPolicy {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn allow_user(mut self, user_id: impl Into<String>) -> Self {
        self.users.insert(user_id.into(), None);
        self
    }
    pub fn allow_user_for(mut self, user_id: impl Into<String>, target: EndpointId) -> Self {
        self.users.insert(user_id.into(), Some(target));
        self
    }
    pub fn permits(&self, user_id: &str, target: EndpointId) -> bool {
        self.users
            .get(user_id)
            .is_some_and(|allowed| allowed.is_none_or(|id| id == target))
    }
}
