use clap::Parser;
use reader::DBReader;
use writer::DBWriter;
use uri::URI;

mod channel;
mod reader;
mod writer;
mod uri;
mod sqlite;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(next_line_help = true)]
struct Args {
    /// Input URI of database
    #[arg(long, short)]
    input: URI,

    /// Output URI of database
    #[arg(long, short)]
    output: URI,

    /// List of tables
    #[arg(long, short)]
    table: Vec<String>,

    /// Size of queue between reader and writers
    #[arg(long, short)]
    queue_size: Option<usize>,
}

fn main() {
    let args = Args::parse();
    let reader = args.input.create_reader();
    let writer = args.output.create_writer();
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        reader.start_reading(sender, table);
        writer.start_writing(reciever, table);
    }
}
