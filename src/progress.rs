use indicatif::{MultiProgress, ProgressBar, ProgressStyle, TermLike};
use std::io;
use tracing::info;

use crate::args::Args;

/// Terminal Like logger, workaround in case if there is no terminal
#[derive(Debug)]
pub struct TermLoggger;

impl TermLike for TermLoggger {
    fn width(&self) -> u16 {
        return 100;
    }
    fn height(&self) -> u16 {
        return 20;
    }

    fn move_cursor_up(&self, _n: usize) -> io::Result<()> {
        return Ok(());
    }
    fn move_cursor_down(&self, _n: usize) -> io::Result<()> {
        return Ok(());
    }
    fn move_cursor_right(&self, _n: usize) -> io::Result<()> {
        return Ok(());
    }
    fn move_cursor_left(&self, _n: usize) -> io::Result<()> {
        return Ok(());
    }

    /// Write a string and add a newline.
    fn write_line(&self, s: &str) -> io::Result<()> {
        if !s.trim().is_empty() {
            info!("{s}");
        }
        return Ok(());
    }

    /// Write a string
    fn write_str(&self, s: &str) -> io::Result<()> {
        return self.write_line(s);
    }
    /// Clear the current line and reset the cursor to beginning of the line
    fn clear_line(&self) -> io::Result<()> {
        return Ok(());
    }

    fn flush(&self) -> io::Result<()> {
        return Ok(());
    }
}

pub struct TableMigrationProgress {
    pub reader: ProgressBar,
    pub writer: ProgressBar,
    _multibar: MultiProgress,
}

impl TableMigrationProgress {
    pub fn new(args: &Args, table: &str, num_rows: u64) -> Self {
        let multibar = MultiProgress::new();
        if args.quiet {
            multibar.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        } else if ProgressBar::no_length().is_hidden() {
            multibar.set_draw_target(indicatif::ProgressDrawTarget::term_like_with_hz(
                Box::new(TermLoggger {}),
                1,
            ));
        }
        let style = ProgressStyle::with_template(
                "{msg} [{elapsed_precise}] {bar:40} {percent}% {human_pos}/{human_len} Rows per sec: {per_sec} ETA: {eta}",
            )
            .unwrap();
        let reader = ProgressBar::new(num_rows).with_style(style.clone());
        let writer = ProgressBar::new(num_rows).with_style(style);

        multibar.add(reader.clone());
        multibar.add(writer.clone());
        reader.set_message(format!("Reading table {table}"));
        writer.set_message(format!("Writing table {table}"));
        return Self {
            reader,
            writer,
            _multibar: multibar,
        };
    }
}
