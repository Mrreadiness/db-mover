use anyhow::Context;
use indicatif::MultiProgress;

pub mod args;
pub mod channel;
pub mod databases;
pub mod progress;
pub mod uri;

#[cfg(test)]
mod test_utils;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    console::set_colors_enabled(false);
    let mut writer = args.output.create_writer()?;
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let multi_bars = MultiProgress::new();
        let reader_progress = progress::get_progress_bar();
        let writer_progress = progress::get_progress_bar();

        multi_bars.add(reader_progress.clone());
        multi_bars.add(writer_progress.clone());
        reader_progress.position();
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
            .start_writing(
                reciever,
                table,
                args.batch_write_size,
                args.batch_write_retries,
                writer_progress,
            )
            .context("Writer failed")?;
        let reader_result = reader_handle.join().expect("Reader panicked");
        reader_result.context("Reader failed")?;
    }
    return Ok(());
}
