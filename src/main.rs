use clap::Parser;

fn main() -> anyhow::Result<()> {
    let args = db_mover::args::Args::parse();
    return db_mover::run(args);
}
