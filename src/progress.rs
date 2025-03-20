use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

pub fn get_progress_bar() -> ProgressBar {
    return ProgressBar::with_draw_target(None, ProgressDrawTarget::stdout()).with_style(
            ProgressStyle::with_template(
                "{msg} [{elapsed_precise}] {bar:40} {percent}% {human_pos}/{human_len} Rows per sec: {per_sec} ETA: {eta}",
            )
            .unwrap(),
        );
}

pub fn log_progress_bar_if_no_term(bar: &ProgressBar) {
    if !bar.is_hidden() {
        return;
    }
    let msg = bar.message();
    let elapsed = bar.elapsed();
    let per_sec = bar.per_sec() as i32;
    let eta = bar.eta();
    let position = bar.position();
    let length = bar.length();
    if length.is_none() {
        return;
    }
    let length = length.unwrap();
    let percent = position * 100 / length;
    println!(
        "{msg} [{elapsed:?}] {percent}% {position}/{length} Rows per sec: {per_sec} ETA: {eta:?}"
    )
}
