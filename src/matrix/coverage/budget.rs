use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Clock {
    pub utc_ms: i64,
    pub boot: String,
    pub boot_ms: i64,
}
impl Clock {
    pub fn read() -> Result<Self> {
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|_| refuse("coverage-clock-boot"))?;
        uuid::Uuid::parse_str(boot.trim()).map_err(|_| refuse("coverage-clock-boot"))?;
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: valid, exclusively borrowed timespec output pointer.
        if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) } != 0 {
            return Err(refuse("coverage-clock"));
        }
        let boot_ms = ts
            .tv_sec
            .checked_mul(1000)
            .and_then(|s| s.checked_add(ts.tv_nsec / 1_000_000))
            .ok_or_else(|| refuse("coverage-clock"))?;
        Ok(Self {
            utc_ms: chrono::Utc::now().timestamp_millis(),
            boot: boot.trim().into(),
            boot_ms,
        })
    }
}
impl Coverage {
    pub fn tick(&mut self, now: Clock) -> Result<()> {
        if now.utc_ms < self.clock.utc_ms
            || now.utc_ms > self.deadline_ms
            || (now.boot == self.clock.boot && now.boot_ms < self.clock.boot_ms)
        {
            return Err(refuse("coverage-lifetime-or-clock"));
        }
        let elapsed = (now.utc_ms - self.clock.utc_ms).max(if now.boot == self.clock.boot {
            now.boot_ms - self.clock.boot_ms
        } else {
            0
        });
        let total = self
            .elapsed_ms
            .checked_add(elapsed)
            .ok_or_else(|| refuse("coverage-lifetime-or-clock"))?;
        if total > 1_800_000 {
            return Err(refuse("coverage-lifetime-or-clock"));
        }
        self.elapsed_ms = total;
        self.clock = now;
        Ok(())
    }
    pub fn segment(&mut self, now: Clock) -> Result<()> {
        self.tick(now.clone())?;
        if self.segments >= 16 || matches!(self.status.as_str(), "refused" | "published") {
            return Err(refuse("coverage-segment-budget"));
        }
        self.segments += 1;
        self.segment_start = now;
        self.segment_pages = 0;
        self.segment_events = 0;
        self.segment_bytes = 0;
        self.segment_attempts = 0;
        self.reservation = None;
        Ok(())
    }
    pub fn segment_remaining(&self, now: &Clock) -> Result<std::time::Duration> {
        if now.boot != self.segment_start.boot
            || now.utc_ms < self.clock.utc_ms
            || now.utc_ms > self.deadline_ms
        {
            return Err(refuse("coverage-segment-clock"));
        }
        let elapsed =
            (now.utc_ms - self.segment_start.utc_ms).max(now.boot_ms - self.segment_start.boot_ms);
        if !(0..90_000).contains(&elapsed) {
            return Err(refuse("coverage-segment-time"));
        }
        Ok(std::time::Duration::from_millis(
            (90_000 - elapsed).min(self.deadline_ms - now.utc_ms) as u64,
        ))
    }
    pub fn charge_sync(&mut self, now: Clock) -> Result<()> {
        self.tick(now)?;
        if self.target.is_some() || self.sync_attempts >= 2 || self.status != "initializing" {
            return Err(refuse("coverage-sync-budget"));
        }
        self.sync_attempts += 1;
        Ok(())
    }
    pub fn charge_page(&mut self, now: Clock) -> Result<()> {
        self.tick(now.clone())?;
        if self.segment_remaining(&now)?.as_secs() < 6
            || self.attempts >= 180
            || self.pages.len() >= 128
            || self.segment_pages >= 12
            || self.segment_events > 1100
            || self.segment_attempts >= 15
            || self.status != "collecting"
        {
            return Err(refuse("coverage-page-budget"));
        }
        self.reservation = self.frontier()?;
        if self.reservation.is_none() {
            return Err(refuse("coverage-no-frontier"));
        }
        self.attempts += 1;
        self.segment_attempts += 1;
        Ok(())
    }
    pub fn charge_publish(&mut self, now: Clock) -> Result<()> {
        self.tick(now)?;
        if self.publications >= 3 || self.status != "ready" {
            return Err(refuse("coverage-publication-budget"));
        }
        self.publications += 1;
        Ok(())
    }
}
