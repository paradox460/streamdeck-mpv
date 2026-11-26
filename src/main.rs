mod actions;
mod app;
mod command;
mod mpv;
mod renderer;
mod socket_path;
mod template;

use actions::{CommandAction, DisplayAction};
use app::App;

#[tokio::main]
async fn main() -> openaction::OpenActionResult<()> {
    init_logging();
    let app = App::new();
    openaction::register_action(CommandAction { app: app.clone() }).await;
    openaction::register_action(DisplayAction { app }).await;
    openaction::run(std::env::args().collect()).await
}

/// Logs to `<plugin>.sdPlugin/logs/streamdeck-mpv.log`; the binary lives in
/// `<plugin>.sdPlugin/bin/`.
fn init_logging() {
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let exe = std::env::current_exe()?;
        let bin_dir = exe.parent().ok_or("executable has no parent directory")?;
        let log_dir = bin_dir.join("..").join("logs");
        std::fs::create_dir_all(&log_dir)?;
        let file = std::fs::File::create(log_dir.join("streamdeck-mpv.log"))?;
        simplelog::WriteLogger::init(
            simplelog::LevelFilter::Info,
            simplelog::Config::default(),
            file,
        )?;
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("streamdeck-mpv: logging disabled: {e}");
    }
}
