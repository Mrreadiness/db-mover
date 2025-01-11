use clap::Parser;
use crate::uri::URI;


#[derive(Parser)]
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
    #[arg(long, short)]
    pub queue_size: Option<usize>,
}
