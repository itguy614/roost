use std::time::Duration;

pub const STABLE_RUN: Duration = Duration::from_secs(300);
const LIMIT: u8 = 10;

pub fn delay(failures: &mut u8, uptime: Duration) -> Option<Duration> {
    if uptime >= STABLE_RUN {
        *failures = 0;
    }
    *failures = failures.saturating_add(1);
    if *failures >= LIMIT {
        return None;
    }
    Some(Duration::from_secs((1 << (*failures - 1)).min(30)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_delays_and_latches_at_ten_rapid_exits() {
        let mut failures = 0;
        let delays: Vec<_> = (0..10)
            .map(|_| delay(&mut failures, Duration::ZERO).map(|d| d.as_secs()))
            .collect();
        assert_eq!(
            delays,
            [
                Some(1),
                Some(2),
                Some(4),
                Some(8),
                Some(16),
                Some(30),
                Some(30),
                Some(30),
                Some(30),
                None
            ]
        );
        assert_eq!(failures, 10);
        assert_eq!(
            delay(&mut failures, STABLE_RUN),
            Some(Duration::from_secs(1))
        );
        assert_eq!(failures, 1);
    }
}
