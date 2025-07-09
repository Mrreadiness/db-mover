use std::{
    fmt::Display,
    sync::atomic,
    time::{Duration, Instant},
};

use num_format::{Locale, ToFormattedString};
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
    finished: atomic::AtomicBool,
    started: Instant,
}

impl ProgressTracker {
    fn new(total: Option<u64>) -> Self {
        return Self {
            total,
            current: atomic::AtomicU64::new(0),
            finished: atomic::AtomicBool::new(false),
            started: Instant::now(),
        };
    }

    fn inc(&self, value: u64) {
        self.current.fetch_add(value, atomic::Ordering::Relaxed);
    }

    fn current(&self) -> u64 {
        return self.current.load(atomic::Ordering::Relaxed);
    }

    fn finished(&self) -> bool {
        return self.finished.load(atomic::Ordering::Relaxed);
    }

    fn finish(&self) {
        self.finished.store(true, atomic::Ordering::Relaxed);
    }
}

impl Display for ProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current = self.current();
        f.write_fmt(format_args!(
            "[{}] ",
            FormattedDuration(self.started.elapsed()),
        ))?;
        let finished = self.finished();
        if finished {
            f.write_str("Finished ")?;
        }
        // Prevent zero division
        let per_sec = current / std::cmp::max(self.started.elapsed().as_secs(), 1);

        if let Some(total) = self.total {
            let mut percent = 100;
            let mut percent_remainder = 0;
            if total > 0 {
                percent = current * 100 / total;
                percent_remainder = (current * 10000 / total) % 100;
            }
            f.write_fmt(format_args!(
                "Processed: {percent}.{percent_remainder:02}% ({}/{}) ",
                current.to_formatted_string(&Locale::en),
                total.to_formatted_string(&Locale::en),
            ))?;
            if !finished && per_sec > 0 {
                let eta = FormattedDuration(Duration::from_secs((total - current) / per_sec));
                f.write_fmt(format_args!("ETA: {eta} "))?;
            }
        } else {
            f.write_fmt(format_args!("Processed: {current} "))?;
        }

        if !finished {
            f.write_fmt(format_args!("Rows per sec: {per_sec} "))?;
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

    pub fn finish_reader(&self) {
        self.reader.finish();
    }

    pub fn finish_writer(&self) {
        self.writer.finish();
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
        info!("Reading table \"{}\" {}", self.table, self.reader);
        info!("Writing table \"{}\" {}", self.table, self.writer);
    }
}

impl Drop for TableMigrationProgress {
    fn drop(&mut self) {
        if self.reader.current() > 0 || self.writer.current() > 0 {
            self.log();
        }
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
