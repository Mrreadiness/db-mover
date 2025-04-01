use std::{
    fmt::Display,
    sync::atomic,
    time::{Duration, Instant},
};

use tracing::info;

struct RateLimiter {
    last_usage: atomic::AtomicU64,
    min_duration_between: Duration,
    started: Instant,
}

impl RateLimiter {
    fn new(min_secs_between: u64) -> Self {
        return Self {
            last_usage: atomic::AtomicU64::new(0),
            min_duration_between: Duration::from_secs(min_secs_between),
            started: Instant::now(),
        };
    }

    fn get_token(&self) -> Result<(), ()> {
        let last_usage = Duration::from_secs(self.last_usage.load(atomic::Ordering::Relaxed));
        if self.started.elapsed() - last_usage < self.min_duration_between {
            return Err(());
        }
        let new_usage = self.started.elapsed();

        return match self.last_usage.compare_exchange_weak(
            last_usage.as_secs(),
            new_usage.as_secs(),
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        };
    }
}

#[derive(Debug)]
pub struct FormattedDuration(pub Duration);

impl Display for FormattedDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut t = self.0.as_secs();
        let seconds = t % 60;
        t /= 60;
        let minutes = t % 60;
        t /= 60;
        let hours = t % 24;
        t /= 24;
        if t > 0 {
            let days = t;
            write!(f, "{days}d {hours:02}:{minutes:02}:{seconds:02}")
        } else {
            write!(f, "{hours:02}:{minutes:02}:{seconds:02}")
        }
    }
}

struct ProgressTracker {
    total: Option<u64>,
    current: atomic::AtomicU64,
    started: Instant,
}

impl ProgressTracker {
    fn new(total: Option<u64>) -> Self {
        return Self {
            total,
            current: atomic::AtomicU64::new(0),
            started: Instant::now(),
        };
    }

    fn inc(&self, value: u64) {
        self.current.fetch_add(value, atomic::Ordering::Relaxed);
    }

    fn current(&self) -> u64 {
        return self.current.load(atomic::Ordering::Relaxed);
    }
}

impl Display for ProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current = self.current.load(atomic::Ordering::Relaxed);
        let per_sec = current / self.started.elapsed().as_secs();
        if let Some(total) = self.total {
            let percent = current * 100 / total;
            let percent_remainder = (current * 10000 / total) % 100;
            let eta = FormattedDuration(Duration::from_secs((total - current) / per_sec));
            f.write_fmt(format_args!(
                "[{}] Processed: {percent}.{percent_remainder}% ({current}/{total}) Rows per sec: {per_sec} ETA: {eta}",
                FormattedDuration(self.started.elapsed()),
            ))?;
        } else {
            f.write_fmt(format_args!(
                "[{}] Processed: {current} Rows per sec: {per_sec}",
                FormattedDuration(self.started.elapsed()),
            ))?;
        }
        return Ok(());
    }
}

pub struct TableMigrationProgress {
    table: String,
    reader: ProgressTracker,
    writer: ProgressTracker,
    limiter: RateLimiter,
}

impl TableMigrationProgress {
    pub fn inc_reader(&self, value: u64) {
        self.reader.inc(value);
        self.log_with_limit();
    }

    pub fn inc_writer(&self, value: u64) {
        self.writer.inc(value);
        self.log_with_limit();
    }

    pub fn reader_processed(&self) -> u64 {
        return self.reader.current();
    }

    pub fn writer_processed(&self) -> u64 {
        return self.writer.current();
    }

    fn log_with_limit(&self) {
        if self.limiter.get_token().is_ok() {
            self.log();
        }
    }

    fn log(&self) {
        info!("Reading table {} {}", self.table, self.reader);
        info!("Writing table {} {}", self.table, self.writer);
    }
}

impl Drop for TableMigrationProgress {
    fn drop(&mut self) {
        self.log();
    }
}

impl TableMigrationProgress {
    pub fn new(table: &str, num_rows: Option<u64>) -> Self {
        return Self {
            table: table.to_string(),
            reader: ProgressTracker::new(num_rows),
            writer: ProgressTracker::new(num_rows),
            limiter: RateLimiter::new(1),
        };
    }
}
