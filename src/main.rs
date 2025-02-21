use clap::Parser;

fn main() {
    let args = db_mover::args::Args::parse();
    db_mover::run(args);
}
