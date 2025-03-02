use anyhow::Context;

pub mod args;
pub mod channel;
pub mod postgres;
pub mod reader;
pub mod row;
pub mod sqlite;
pub mod uri;
pub mod writer;

pub fn run(args: args::Args) -> anyhow::Result<()> {
    let writer = args.output.create_writer()?;
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let reader_handle = std::thread::spawn({
            let args = args.clone();
            let table = table.clone();
            let mut reader = args.input.create_reader()?;
            move || {
                return reader.start_reading(sender, &table);
            }
        });
        writer
            .start_writing(reciever, table)
            .context("Writer failed")?;
        let reader_result = reader_handle.join().expect("Reader panicked");
        reader_result.context("Reader failed")?;
    }
    return Ok(());
}
