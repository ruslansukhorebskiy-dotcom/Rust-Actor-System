//! Простий журнал: консоль (stderr) + необов'язковий файл.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use log::{Level, LevelFilter, Log, Metadata, Record};

struct Logger {
    console_level: LevelFilter,
    file: Option<Mutex<File>>,
    started: Instant,
}

/// Поточний час UTC у форматі `ГГ:ХХ:СС.ммм` (без сторонніх залежностей).
fn utc_clock() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        now.subsec_millis()
    )
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Error => "ПОМИЛКА",
        Level::Warn => "УВАГА",
        Level::Info => "ІНФО",
        Level::Debug => "НАЛАГ",
        Level::Trace => "ТРАСА",
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.console_level
            || (self.file.is_some() && metadata.level() <= LevelFilter::Debug)
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let elapsed = self.started.elapsed().as_secs_f64();
        let level = level_name(record.level());

        // Рядок формується повністю і записується одним викликом, щоб записи
        // з різних потоків не перемішувались посеред рядка.
        if record.level() <= self.console_level {
            let line = format!("[{elapsed:>8.3}s] {level:<7} {}\n", record.args());
            let _ = io::stderr().lock().write_all(line.as_bytes());
        }
        if let Some(file) = &self.file
            && record.level() <= LevelFilter::Debug
            && let Ok(mut file) = file.lock()
        {
            let line = format!(
                "{} UTC [{elapsed:>8.3}s] {level:<7} {}\n",
                utc_clock(),
                record.args()
            );
            let _ = file.write_all(line.as_bytes());
        }
    }

    fn flush(&self) {
        if let Some(file) = &self.file
            && let Ok(mut file) = file.lock()
        {
            let _ = file.flush();
        }
    }
}

/// Ініціалізувати журнал. Повертає помилку, якщо файл не вдалося відкрити.
pub fn init(verbose: u8, quiet: bool, log_file: Option<&Path>) -> Result<(), String> {
    let console_level = match (quiet, verbose) {
        (true, _) => LevelFilter::Error,
        (false, 0) => LevelFilter::Warn,
        (false, 1) => LevelFilter::Debug,
        (false, _) => LevelFilter::Trace,
    };

    let file = match log_file {
        Some(path) => {
            if path.is_dir() {
                return Err(format!(
                    "не вдалося відкрити журнал «{}»: це каталог, а не файл",
                    path.display()
                ));
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| format!("не вдалося відкрити журнал «{}»: {e}", path.display()))?;
            Some(Mutex::new(file))
        }
        None => None,
    };

    let max_level = if file.is_some() {
        console_level.max(LevelFilter::Debug)
    } else {
        console_level
    };

    let logger = Logger {
        console_level,
        file,
        started: Instant::now(),
    };
    log::set_logger(Box::leak(Box::new(logger))).map_err(|e| e.to_string())?;
    log::set_max_level(max_level);
    Ok(())
}
