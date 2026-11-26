use std::time::{Duration, Instant};

/// Only elapsed time witnessed in this daemon advances the durable policy clock.
/// Wall timestamps are still useful for history and for detecting a clock step.
pub(super) struct Clock {
    elapsed: Duration,
    previous: Option<(u64, Instant)>,
    verified: bool,
}

#[derive(Clone, Copy)]
pub(super) struct Eligibility {
    pub elapsed: u64,
    pub verified: bool,
}

impl Clock {
    pub fn new(checkpoint: u64) -> Self {
        Self {
            elapsed: Duration::from_secs(checkpoint),
            previous: None,
            verified: false,
        }
    }

    pub fn sample(&mut self, wall: u64, now: Instant) -> Eligibility {
        self.verified = self.agrees(wall, now);
        if self.verified
            && let Some((_, last)) = self.previous
        {
            self.elapsed = self.elapsed.saturating_add(now.duration_since(last));
        }
        self.previous = Some((wall, now));
        Eligibility {
            elapsed: self.elapsed.as_secs(),
            verified: self.verified,
        }
    }

    pub fn can_dispatch(&self, wall: u64, now: Instant) -> bool {
        self.verified && self.agrees(wall, now)
    }

    fn agrees(&self, wall: u64, now: Instant) -> bool {
        self.previous.is_some_and(|(previous_wall, previous)| {
            let Some(elapsed) = now.checked_duration_since(previous) else {
                return false;
            };
            let Some(wall_elapsed) = wall.checked_sub(previous_wall) else {
                return false;
            };
            // Wall timestamps have integer-second precision. That quantization
            // can never give eligibility credit beyond the monotonic interval.
            elapsed <= Duration::from_secs(15)
                && elapsed.abs_diff(Duration::from_secs(wall_elapsed)) < Duration::from_secs(1)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_steps_and_restart_cannot_expire_a_window() {
        let start = Instant::now();
        let mut clock = Clock::new(100);
        assert!(!clock.sample(10_000, start).verified);
        let at = |seconds| start + Duration::from_secs(seconds);
        assert_eq!(clock.sample(10_002, at(2)).elapsed, 102);
        assert!(!clock.can_dispatch(90_000, at(3)));
        assert!(!clock.sample(90_000, at(4)).verified);
        assert_eq!(clock.sample(90_002, at(6)).elapsed, 104);
        assert!(!clock.sample(1_000, at(8)).verified);
        assert_eq!(clock.sample(1_002, at(10)).elapsed, 106);
        assert!(!clock.sample(1_062, at(70)).verified);
        assert_eq!(clock.sample(1_064, at(72)).elapsed, 108);
        for wall in [1, 900_000] {
            let mut restarted = Clock::new(108);
            let sample = restarted.sample(wall, at(20));
            assert!(!sample.verified);
            assert_eq!(sample.elapsed, 108);
            assert_eq!(restarted.sample(wall + 2, at(22)).elapsed, 110);
        }
    }
}
