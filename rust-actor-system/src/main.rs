//! Точка входу консольного застосунку `actor-system`.
//!
//! Коди завершення: 0 — успіх, 1 — помилка виконання, 2 — некоректні аргументи.

mod app;

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;

use actor_system::{ActorError, ActorSystem};
use app::cli::{Cli, Command};
use app::{demos, logger, shell};

async fn run(command: Command) -> Result<(), ActorError> {
    let system = ActorSystem::new("actor-system");
    let result = match command {
        Command::PingPong(args) => demos::ping_pong(&system, args.rounds).await,
        Command::Pool(args) => {
            demos::pool(
                &system,
                args.workers,
                args.jobs.0,
                args.timeout_ms,
                args.inline,
            )
            .await
        }
        Command::Supervision(args) => demos::supervision(&system, args.max_restarts).await,
        Command::Shell => shell::run(&system).await,
        Command::Demo => {
            async {
                demos::ping_pong(&system, 3).await?;
                demos::pool(&system, 3, vec![1, 2, 3, 7, 13, 5, 14, 45, 8], 400, false).await?;
                demos::supervision(&system, 2).await
            }
            .await
        }
    };

    // Коректне завершення: зупиняємо всіх акторів, але не чекаємо вічно.
    if tokio::time::timeout(Duration::from_secs(5), system.shutdown())
        .await
        .is_err()
    {
        log::error!("не всі актори зупинились за 5 с");
    }
    result
}

fn main() -> ExitCode {
    let cli = Cli::parse(); // при помилці в аргументах clap сам виведе підказку і код 2

    if let Err(e) = logger::init(cli.verbose, cli.quiet, cli.log_file.as_deref()) {
        eprintln!("Помилка: {e}");
        return ExitCode::from(1);
    }

    // Паніки в акторах перехоплюються системою; стандартний вивід замінюємо записом у журнал.
    std::panic::set_hook(Box::new(|info| {
        log::debug!(
            "перехоплено паніку: {}",
            info.to_string().replace('\n', " ")
        )
    }));

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Помилка: не вдалося запустити асинхронне середовище: {e}");
            return ExitCode::from(1);
        }
    };

    log::info!("запуск: {:?}", cli.command);
    match runtime.block_on(run(cli.command)) {
        Ok(()) => {
            log::info!("роботу завершено успішно");
            log::logger().flush();
            ExitCode::SUCCESS
        }
        Err(e) => {
            log::error!("роботу завершено з помилкою: {e}");
            log::logger().flush();
            eprintln!("Помилка: {e}");
            ExitCode::from(1)
        }
    }
}
