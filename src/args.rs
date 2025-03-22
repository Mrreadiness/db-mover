use crate::uri::URI;
use clap::Parser;
use tracing::Level;

#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
#[command(next_line_help = true)]
pub struct Args {
    /// Input URI of database
    #[arg(long, short)]
    pub input: URI,

    /// Output URI of database
    #[arg(long, short)]
    pub output: URI,

    /// List of tables
    #[arg(long, short)]
    pub table: Vec<String>,

    /// Size of queue between reader and writers
    #[arg(long, default_value_t = 100_000)]
    pub queue_size: usize,

    /// Size of batches used by writer
    #[arg(long, default_value_t = 1_000)]
    pub batch_write_size: usize,

    /// Number of retries to write a batch
    #[arg(long, default_value_t = 3)]
    pub batch_write_retries: usize,

    /// Disable output
    #[clap(long, action)]
    pub quiet: bool,

    /// Log level
    #[arg(long, default_value_t = Level::INFO)]
    pub log_level: Level,

    /// Disable the COUNT query used for progress tracking.
    /// Progress will be shown but without prognoses.
    /// Only for input table.
    #[clap(long, action)]
    pub no_count: bool,
}

impl Args {
    pub fn new(input: URI, output: URI) -> Self {
        return Args {
            input,
            output,
            table: Vec::new(),
            queue_size: 10_000,
            batch_write_size: 1_000,
            batch_write_retries: 1,
            quiet: true,
            log_level: Level::INFO,
            no_count: false,
        };
    }
}
