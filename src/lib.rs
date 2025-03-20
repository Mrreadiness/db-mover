use anyhow::Context;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub mod args;
pub mod channel;
pub mod postgres;
pub mod reader;
pub mod row;
pub mod sqlite;
pub mod uri;
pub mod writer;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    let mut writer = args.output.create_writer()?;
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let multi_bars = MultiProgress::new();
        let reader_progress = ProgressBar::no_length().with_style(
            ProgressStyle::with_template(
                "{msg} [{elapsed_precise}] {bar:40} {percent}% {human_pos}/{human_len} Rows per sec: {per_sec} ETA: {eta}",
            )
            .unwrap(),
        );
        let writer_progress = ProgressBar::no_length().with_style(
            ProgressStyle::with_template(
                "{msg} [{elapsed_precise}] {bar:40} {percent}% {human_pos}/{human_len} Rows per sec: {per_sec} ETA: {eta}",
            )
            .unwrap(),
        );

        multi_bars.add(reader_progress.clone());
        multi_bars.add(writer_progress.clone());
        let reader_handle = std::thread::spawn({
            let args = args.clone();
            let table = table.clone();
            let mut reader = args.input.create_reader()?;
            let table_size = reader.get_count(&table)?.into();
            reader_progress.set_length(table_size);
            writer_progress.set_length(table_size);
            reader_progress.set_message(format!("Reading table {table}"));
            move || {
                return reader.start_reading(sender, &table, reader_progress);
            }
        });
        writer_progress.set_message(format!("Writing table {table}"));
        writer
            .start_writing(reciever, table, writer_progress)
            .context("Writer failed")?;
        let reader_result = reader_handle.join().expect("Reader panicked");
        reader_result.context("Reader failed")?;
    }
    return Ok(());
}
