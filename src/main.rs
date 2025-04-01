use clap::Parser;

fn main() -> anyhow::Result<()> {
    let args = db_mover::args::Args::parse();
    let level_filter = if args.quiet {
        tracing_subscriber::filter::LevelFilter::OFF
    } else {
        tracing_subscriber::filter::LevelFilter::from_level(args.log_level)
    };
    tracing_subscriber::fmt()
        .with_max_level(level_filter)
        .init();

    return db_mover::run(args);
}
