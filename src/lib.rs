pub mod args;
pub mod channel;
pub mod reader;
pub mod row;
pub mod sqlite;
pub mod postgres;
pub mod uri;
pub mod writer;

pub fn run(args: args::Args) {
    let writer = args.output.create_writer();
    for table in &args.table {
        let (sender, reciever) = channel::create_channel(args.queue_size);
        let reader_handle = std::thread::spawn({
            let args = args.clone();
            let table = table.clone();
            move || {
                let mut reader = args.input.create_reader();
                reader.start_reading(sender, &table);
            }
        });
        writer.start_writing(reciever, table);
        reader_handle.join().expect("Failed to finish reading");
    }
}
