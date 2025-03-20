use clap::Parser;
use tracing::Level;

fn main() -> anyhow::Result<()> {
    let args = db_mover::args::Args::parse();
    if args.quiet {
        tracing_subscriber::fmt()
            .with_max_level(tracing_subscriber::filter::LevelFilter::OFF)
            .init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    }

    return db_mover::run(args);
}
