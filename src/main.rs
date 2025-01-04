use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(next_line_help = true)]
struct Args {
    /// Input URI of database
    #[arg(long, short)]
    input: String,

    /// Output URI of database
    #[arg(long, short)]
    output: String,
}

fn main() {
    let args = Args::parse();
}
