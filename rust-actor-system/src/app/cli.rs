//! Опис аргументів командного рядка та їх перевірка.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Демонстраційний застосунок акторної системи на Rust.
#[derive(Debug, Parser)]
#[command(name = "actor-system", version, about, long_about = None)]
pub struct Cli {
    /// Записувати журнал роботи у файл (дописується в кінець).
    #[arg(long, global = true, value_name = "ФАЙЛ")]
    pub log_file: Option<PathBuf>,

    /// Детальний журнал у консолі (-v — налагодження, -vv — трасування).
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Виводити в консоль лише помилки.
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Обмін повідомленнями між двома акторами (ping-pong).
    PingPong(PingPongArgs),
    /// Пул акторів-воркерів, що виконують асинхронні задачі.
    Pool(PoolArgs),
    /// Супервізія: обробка помилок, паніки, перезапуски.
    Supervision(SupervisionArgs),
    /// Інтерактивний режим: створення акторів і надсилання їм команд.
    Shell,
    /// Послідовно запустити всі демонстрації з параметрами за замовчуванням.
    Demo,
}

#[derive(Debug, Args)]
pub struct PingPongArgs {
    /// Кількість раундів обміну (1..=1000).
    #[arg(short, long, default_value_t = 5, value_parser = parse_rounds)]
    pub rounds: u32,
}

#[derive(Debug, Args)]
pub struct PoolArgs {
    /// Кількість акторів-воркерів (1..=64).
    #[arg(short, long, default_value_t = 3, value_parser = parse_workers)]
    pub workers: usize,

    /// Вхідні дані задач через кому, кожне число 0..=1000.
    /// Правила моделі: 13 — паніка, кратні 7 — помилка, >40 — довга задача.
    #[arg(short, long, default_value = "1,2,3,7,13,5,14,45,8", value_parser = parse_jobs)]
    pub jobs: JobList,

    /// Ліміт часу на одну задачу, мс (10..=60000).
    #[arg(short, long, default_value_t = 400, value_parser = parse_timeout)]
    pub timeout_ms: u64,

    /// Виконувати задачу прямо в обробнику воркера (послідовно в межах воркера),
    /// а не у фоновій асинхронній задачі.
    #[arg(long)]
    pub inline: bool,
}

#[derive(Debug, Args)]
pub struct SupervisionArgs {
    /// Максимальна кількість перезапусків актора (0..=10).
    #[arg(short, long, default_value_t = 2, value_parser = parse_restarts)]
    pub max_restarts: u32,
}

/// Список вхідних даних задач (обгортка, щоб clap не трактував його як Vec-аргумент).
#[derive(Debug, Clone)]
pub struct JobList(pub Vec<u64>);

pub const MAX_JOBS: usize = 1000;
pub const MAX_JOB_VALUE: u64 = 1000;

fn parse_in_range(raw: &str, min: u64, max: u64, what: &str) -> Result<u64, String> {
    let value: u64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("«{raw}» не є цілим невід'ємним числом ({what})"))?;
    if !(min..=max).contains(&value) {
        return Err(format!(
            "{what} має бути в межах {min}..={max}, отримано {value}"
        ));
    }
    Ok(value)
}

fn parse_rounds(raw: &str) -> Result<u32, String> {
    parse_in_range(raw, 1, 1000, "кількість раундів").map(|v| v as u32)
}

fn parse_workers(raw: &str) -> Result<usize, String> {
    parse_in_range(raw, 1, 64, "кількість воркерів").map(|v| v as usize)
}

fn parse_timeout(raw: &str) -> Result<u64, String> {
    parse_in_range(raw, 10, 60_000, "таймаут")
}

fn parse_restarts(raw: &str) -> Result<u32, String> {
    parse_in_range(raw, 0, 10, "кількість перезапусків").map(|v| v as u32)
}

pub fn parse_jobs(raw: &str) -> Result<JobList, String> {
    let items: Vec<&str> = raw.split(',').map(str::trim).collect();
    if items.iter().all(|s| s.is_empty()) {
        return Err("список задач порожній".into());
    }
    if items.len() > MAX_JOBS {
        return Err(format!(
            "забагато задач: {} (максимум {MAX_JOBS})",
            items.len()
        ));
    }
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            if item.is_empty() {
                return Err(format!("порожній елемент на позиції {}", i + 1));
            }
            parse_in_range(item, 0, MAX_JOB_VALUE, "значення задачі")
                .map_err(|e| format!("елемент {}: {e}", i + 1))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(JobList)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_parsing() {
        assert_eq!(parse_jobs("1, 2,3").unwrap().0, vec![1, 2, 3]);
        assert!(parse_jobs("").is_err());
        assert!(parse_jobs("1,,2").is_err());
        assert!(parse_jobs("1,abc").is_err());
        assert!(parse_jobs("1,-5").is_err());
        assert!(parse_jobs("1001").is_err());
    }

    #[test]
    fn range_checks() {
        assert!(parse_rounds("0").is_err());
        assert!(parse_rounds("1000").is_ok());
        assert!(parse_workers("65").is_err());
        assert!(parse_timeout("5").is_err());
        assert!(parse_restarts("11").is_err());
        assert_eq!(parse_restarts("0"), Ok(0));
    }
}
