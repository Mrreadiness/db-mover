use std::time::Duration;

pub struct ExponentialRetry {
    left_reties: usize,
    current: Duration,
    factor: u32,
}

impl ExponentialRetry {
    pub fn new(retries: usize) -> Self {
        return Self {
            left_reties: retries,
            current: Duration::from_millis(500),
            factor: 2,
        };
    }

    pub fn with_base_duration(retries: usize, duration: Duration) -> Self {
        return Self {
            left_reties: retries,
            current: duration,
            factor: 2,
        };
    }
}

impl Iterator for ExponentialRetry {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.left_reties == 0 {
            return None;
        }
        let duration = self.current;
        self.current *= self.factor;
        self.left_reties -= 1;

        return Some(duration);
    }
}
