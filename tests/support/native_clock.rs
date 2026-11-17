use std::sync::Arc;

use chrono::{DateTime, Utc};
use guigu_agent_bridge::bus::{BusFuture, Clock, Timer};
use tokio::sync::watch;

pub struct NativeClock(watch::Sender<DateTime<Utc>>);

impl NativeClock {
    pub fn new() -> Arc<Self> {
        let (time, _) = watch::channel(Utc::now());
        Arc::new(Self(time))
    }

    pub fn clock(self: &Arc<Self>) -> Clock {
        let time = self.clone();
        Clock::new(move || *time.0.borrow())
    }

    pub fn expire(&self) {
        self.0
            .send_modify(|time| *time += chrono::Duration::seconds(3));
    }
}

impl Timer for NativeClock {
    fn sleep_until(&self, deadline: DateTime<Utc>) -> BusFuture<'_, ()> {
        let mut time = self.0.subscribe();
        Box::pin(async move {
            loop {
                if *time.borrow_and_update() >= deadline {
                    return;
                }
                time.changed().await.expect("fixture owns its clock");
            }
        })
    }
}
