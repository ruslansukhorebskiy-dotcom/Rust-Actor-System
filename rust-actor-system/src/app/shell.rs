//! Інтерактивний режим: користувач створює акторів-лічильників
//! і надсилає їм команди з клавіатури.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};

use actor_system::{
    Actor, ActorError, ActorRef, ActorSystem, AskError, Context, PanicError, Reply, SpawnOptions,
    TaskTimeout,
};

/// Ліміт часу фонової задачі актора.
const TASK_TIMEOUT: Duration = Duration::from_secs(5);
/// Ліміт очікування відповіді на запит.
const ASK_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ACTORS: usize = 100;
const MAX_TASK_MS: u64 = 60_000;

#[derive(Default)]
struct Counter {
    value: i64,
    tasks_done: u32,
}

enum CounterMsg {
    Add(i64, Reply<Result<i64, String>>),
    Get(Reply<(i64, u32, usize)>),
    StartTask(u64, Reply<()>),
    TaskDone(u64, Instant, Result<u64, ActorError>),
    Fail,
    Panic,
}

impl Actor for Counter {
    type Msg = CounterMsg;

    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        log::info!("[{}] запущено (value = {})", ctx.name(), self.value);
        Ok(())
    }

    async fn handle(&mut self, msg: CounterMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            CounterMsg::Add(n, reply) => {
                // Перевірка переповнення: користувач отримує зрозуміле повідомлення.
                let result = self.value.checked_add(n).ok_or_else(|| {
                    format!("переповнення: {} + {n} виходить за межі i64", self.value)
                });
                if let Ok(v) = result {
                    self.value = v;
                }
                let _ = reply.send(result);
            }
            CounterMsg::Get(reply) => {
                let _ = reply.send((self.value, self.tasks_done, ctx.running_tasks()));
            }
            CounterMsg::StartTask(ms, reply) => {
                let started = Instant::now();
                ctx.run_task_with_timeout(
                    TASK_TIMEOUT,
                    async move {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        Ok(ms)
                    },
                    move |res| CounterMsg::TaskDone(ms, started, res),
                );
                let _ = reply.send(());
            }
            CounterMsg::TaskDone(ms, started, result) => match result {
                Ok(_) => {
                    self.value += 1;
                    self.tasks_done += 1;
                    println!(
                        "\n  [{}] фонову задачу ({ms} мс) завершено за {:.0?}, value = {}",
                        ctx.name(),
                        started.elapsed(),
                        self.value
                    );
                }
                Err(e) if e.is::<TaskTimeout>() => {
                    println!(
                        "\n  [{}] фонову задачу ({ms} мс) скасовано: {e}",
                        ctx.name()
                    );
                }
                Err(e) => println!(
                    "\n  [{}] фонова задача завершилась помилкою: {e}",
                    ctx.name()
                ),
            },
            CounterMsg::Fail => return Err("штучна помилка, викликана командою fail".into()),
            CounterMsg::Panic => panic!("штучна паніка, викликана командою panic"),
        }
        Ok(())
    }

    fn on_error(&mut self, err: &ActorError, ctx: &mut Context<Self>) -> actor_system::Directive {
        if err.is::<PanicError>() {
            println!(
                "\n  [{}] {err} — актора буде перезапущено зі скинутим станом",
                ctx.name()
            );
            actor_system::Directive::Restart
        } else {
            println!(
                "\n  [{}] помилка: {err} — роботу продовжено, стан збережено",
                ctx.name()
            );
            actor_system::Directive::Resume
        }
    }
}

const HELP: &str = "\
Команди:
  spawn <ім'я>          створити актора-лічильника
  add <ім'я> <число>    додати число до лічильника (запит-відповідь)
  get <ім'я>            показати стан актора
  task <ім'я> <мс>      запустити фонову асинхронну задачу (таймаут 5 с)
  fail <ім'я>           викликати помилку в обробнику (Resume)
  panic <ім'я>          викликати паніку в обробнику (Restart, макс. 3)
  stop <ім'я>           зупинити актора
  list                  список акторів
  help                  ця довідка
  exit | quit           вийти";

/// Помилка введення користувача.
#[derive(Debug, PartialEq, Eq)]
pub enum InputError {
    Unknown(String),
    Usage(&'static str),
    BadNumber(String),
    BadName(String),
}

impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown(c) => write!(f, "невідома команда «{c}». Введіть help для довідки"),
            Self::Usage(u) => write!(f, "неправильний формат. Використання: {u}"),
            Self::BadNumber(n) => write!(f, "«{n}» не є коректним цілим числом"),
            Self::BadName(n) => write!(
                f,
                "некоректне ім'я «{n}»: дозволено латиницю, цифри, '-' та '_', до 32 символів"
            ),
        }
    }
}

/// Розібрана команда користувача.
#[derive(Debug, PartialEq, Eq)]
pub enum ShellCmd {
    Spawn(String),
    Add(String, i64),
    Get(String),
    Task(String, u64),
    Fail(String),
    Panic(String),
    Stop(String),
    List,
    Help,
    Exit,
    Empty,
}

fn valid_name(name: &str) -> Result<String, InputError> {
    let ok = !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(name.to_string())
    } else {
        Err(InputError::BadName(name.to_string()))
    }
}

/// Розбір рядка введення.
pub fn parse_command(line: &str) -> Result<ShellCmd, InputError> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let Some((&cmd, args)) = parts.split_first() else {
        return Ok(ShellCmd::Empty);
    };
    let one = |usage| match args {
        [name] => valid_name(name),
        _ => Err(InputError::Usage(usage)),
    };
    match cmd.to_lowercase().as_str() {
        "spawn" => one("spawn <ім'я>").map(ShellCmd::Spawn),
        "get" => one("get <ім'я>").map(ShellCmd::Get),
        "fail" => one("fail <ім'я>").map(ShellCmd::Fail),
        "panic" => one("panic <ім'я>").map(ShellCmd::Panic),
        "stop" => one("stop <ім'я>").map(ShellCmd::Stop),
        "add" => match args {
            [name, n] => {
                let n = n
                    .parse()
                    .map_err(|_| InputError::BadNumber(n.to_string()))?;
                Ok(ShellCmd::Add(valid_name(name)?, n))
            }
            _ => Err(InputError::Usage("add <ім'я> <число>")),
        },
        "task" => match args {
            [name, ms] => {
                let ms: u64 = ms
                    .parse()
                    .map_err(|_| InputError::BadNumber(ms.to_string()))?;
                if ms > MAX_TASK_MS {
                    return Err(InputError::Usage(
                        "task <ім'я> <мс>, де мс у межах 0..=60000",
                    ));
                }
                Ok(ShellCmd::Task(valid_name(name)?, ms))
            }
            _ => Err(InputError::Usage("task <ім'я> <мс>")),
        },
        "list" | "ls" => Ok(ShellCmd::List),
        "help" | "?" => Ok(ShellCmd::Help),
        "exit" | "quit" => Ok(ShellCmd::Exit),
        other => Err(InputError::Unknown(other.to_string())),
    }
}

struct Shell {
    system: ActorSystem,
    actors: BTreeMap<String, ActorRef<Counter>>,
}

impl Shell {
    fn find(&mut self, name: &str) -> Result<ActorRef<Counter>, String> {
        // Прибираємо акторів, які вже зупинились (наприклад, після вичерпання перезапусків).
        self.actors.retain(|_, a| a.is_alive());
        self.actors
            .get(name)
            .cloned()
            .ok_or_else(|| format!("актора «{name}» не знайдено. Перелік: list"))
    }

    fn ask_err(e: AskError) -> String {
        format!("запит не виконано: {e}")
    }

    async fn execute(&mut self, cmd: ShellCmd) -> Result<Option<String>, String> {
        let out = match cmd {
            ShellCmd::Empty => return Ok(None),
            ShellCmd::Help => HELP.to_string(),
            ShellCmd::Exit => unreachable!("обробляється в циклі"),
            ShellCmd::Spawn(name) => {
                self.actors.retain(|_, a| a.is_alive());
                if self.actors.len() >= MAX_ACTORS {
                    return Err(format!("досягнуто ліміт кількості акторів ({MAX_ACTORS})"));
                }
                let actor = self
                    .system
                    .spawn_with(&name, Counter::default, SpawnOptions::default())
                    .map_err(|e| format!("не вдалося створити актора: {e}"))?;
                self.actors.insert(name.clone(), actor);
                format!("актора «{name}» створено")
            }
            ShellCmd::Add(name, n) => {
                let actor = self.find(&name)?;
                let result = actor
                    .ask_timeout(|r| CounterMsg::Add(n, r), ASK_TIMEOUT)
                    .await
                    .map_err(Self::ask_err)?;
                format!("{name}.value = {}", result?)
            }
            ShellCmd::Get(name) => {
                let actor = self.find(&name)?;
                let (value, done, running) = actor
                    .ask_timeout(CounterMsg::Get, ASK_TIMEOUT)
                    .await
                    .map_err(Self::ask_err)?;
                format!(
                    "{name}: value = {value}, виконано задач = {done}, активних задач = {running}"
                )
            }
            ShellCmd::Task(name, ms) => {
                let actor = self.find(&name)?;
                actor
                    .ask_timeout(|r| CounterMsg::StartTask(ms, r), ASK_TIMEOUT)
                    .await
                    .map_err(Self::ask_err)?;
                format!("фонову задачу на {ms} мс запущено; актор готовий до нових команд")
            }
            ShellCmd::Fail(name) => {
                let actor = self.find(&name)?;
                actor
                    .tell(CounterMsg::Fail)
                    .await
                    .map_err(|e| e.to_string())?;
                "повідомлення fail надіслано".to_string()
            }
            ShellCmd::Panic(name) => {
                let actor = self.find(&name)?;
                actor
                    .tell(CounterMsg::Panic)
                    .await
                    .map_err(|e| e.to_string())?;
                "повідомлення panic надіслано".to_string()
            }
            ShellCmd::Stop(name) => {
                let actor = self.find(&name)?;
                actor.stop().await;
                actor.wait_stopped().await;
                self.actors.remove(&name);
                format!("актора «{name}» зупинено")
            }
            ShellCmd::List => {
                self.actors.retain(|_, a| a.is_alive());
                if self.actors.is_empty() {
                    "акторів немає. Створіть: spawn <ім'я>".to_string()
                } else {
                    let names: Vec<_> = self.actors.keys().map(String::as_str).collect();
                    format!("актори ({}): {}", names.len(), names.join(", "))
                }
            }
        };
        Ok(Some(out))
    }
}

pub async fn run(system: &ActorSystem) -> Result<(), ActorError> {
    println!("Інтерактивний режим акторної системи. Введіть help для довідки.");
    let mut shell = Shell {
        system: system.clone(),
        actors: BTreeMap::new(),
    };
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        use std::io::Write;
        print!("actors> ");
        std::io::stdout().flush()?;

        let Some(line) = lines.next_line().await? else {
            println!();
            break; // кінець вводу (Ctrl+D / Ctrl+Z)
        };
        log::debug!("введено команду: {line}");
        match parse_command(&line) {
            Ok(ShellCmd::Exit) => break,
            Ok(cmd) => match shell.execute(cmd).await {
                Ok(Some(out)) => println!("{out}"),
                Ok(None) => {}
                Err(e) => {
                    log::debug!("команда «{}» не виконана: {e}", line.trim());
                    println!("Помилка: {e}");
                }
            },
            Err(e) => println!("Помилка: {e}"),
        }
        // Дати акторам шанс вивести повідомлення до наступного запрошення.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("Завершення роботи…");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("spawn a1"), Ok(ShellCmd::Spawn("a1".into())));
        assert_eq!(
            parse_command("  ADD a1 -5 "),
            Ok(ShellCmd::Add("a1".into(), -5))
        );
        assert_eq!(parse_command(""), Ok(ShellCmd::Empty));
        assert_eq!(
            parse_command("task a 100"),
            Ok(ShellCmd::Task("a".into(), 100))
        );
    }

    #[test]
    fn rejects_bad_input() {
        assert!(matches!(parse_command("foo"), Err(InputError::Unknown(_))));
        assert!(matches!(parse_command("add a"), Err(InputError::Usage(_))));
        assert!(matches!(
            parse_command("add a x"),
            Err(InputError::BadNumber(_))
        ));
        assert!(matches!(
            parse_command("spawn ім'я"),
            Err(InputError::BadName(_))
        ));
        assert!(matches!(
            parse_command("task a 999999"),
            Err(InputError::Usage(_))
        ));
        assert!(matches!(
            parse_command("task a -1"),
            Err(InputError::BadNumber(_))
        ));
    }
}
