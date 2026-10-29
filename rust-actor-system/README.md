# Rust Actor System — акторна система в Rust

Курсова робота з дисципліни «Системне програмування», варіант 30.

Базова акторна система поверх асинхронного середовища [Tokio](https://tokio.rs) для виконання
конкурентних задач. Кожен актор має власний стан, обробляє повідомлення зі своєї поштової скриньки
строго по одному і взаємодіє з іншими акторами лише через обмін повідомленнями — без спільної
пам'яті та явних блокувань.

## Можливості

| Вимога | Реалізація |
|---|---|
| Створення акторів і передача повідомлень | `ActorSystem::spawn` / `spawn_with`, реєстр за іменами (`lookup`), `ActorRef::tell`, `try_tell`, `ask`, `ask_timeout` |
| Виконання асинхронних задач актором | `async fn handle`, фонові задачі `Context::run_task` / `run_task_with_timeout`, відкладені повідомлення `send_later` |
| Обробка помилок у виконанні задач | перехоплення помилок і **паніки**, стратегії супервізора `Resume` / `Restart` / `Stop`, ліміт перезапусків, таймаути задач і запитів, типізовані помилки `SendError`, `AskError`, `SpawnError` |
| Перевірка вводу | усі аргументи та команди перевіряються, користувач отримує повідомлення українською, коди завершення 0 / 1 / 2 |
| Журналювання | консоль (`-v`, `-q`) і файл (`--log-file`) |
| Платформи | Windows, macOS, Linux (перевіряється в GitHub Actions) |

## Встановлення

Потрібен Rust 1.88 або новіший ([rustup.rs](https://rustup.rs)).

```bash
git clone https://github.com/igorko002/rust-actor-system.git
cd rust-actor-system
cargo build --release
```

Виконуваний файл: `target/release/actor-system` (у Windows — `target\release\actor-system.exe`).

## Використання

```bash
actor-system demo                                  # усі демонстрації
actor-system ping-pong --rounds 5                  # обмін повідомленнями
actor-system pool --workers 4 --jobs 1,2,7,13,45 --timeout-ms 300
actor-system pool --workers 4 --inline             # задачі в обробнику, без фонового запуску
actor-system supervision --max-restarts 2          # помилки, паніка, перезапуски
actor-system shell                                 # інтерактивний режим
actor-system --log-file run.log -v pool            # журнал у файл
actor-system --help
```

Правила моделі задач у режимі `pool`: число 13 викликає паніку, числа, кратні 7, — помилку,
числа понад 40 — «зависання» (спрацьовує таймаут), решта — успішне обчислення квадрата.

Команди інтерактивного режиму: `spawn`, `add`, `get`, `task`, `fail`, `panic`, `stop`, `list`,
`help`, `exit`.

## Тестування

```bash
cargo test                          # 17 модульних та інтеграційних тестів
cargo clippy --all-targets -- -D warnings
```

## Структура

```
src/
  lib.rs        публічний API бібліотеки actor_system
  actor.rs      трейт Actor, Directive
  addr.rs       ActorRef: tell / ask / stop
  context.rs    Context: фонові задачі, власна адреса, доступ до системи
  system.rs     ActorSystem: spawn, реєстр, shutdown
  runtime.rs    цикл обробки повідомлень і супервізор
  error.rs      типи помилок
  main.rs       точка входу консольного застосунку
  app/
    cli.rs      аргументи командного рядка та їх перевірка
    logger.rs   журналювання
    demos.rs    сценарії ping-pong, pool, supervision
    shell.rs    інтерактивний режим
tests/actors.rs інтеграційні тести бібліотеки
```

## Мінімальний приклад використання бібліотеки

```rust
use actor_system::{Actor, ActorError, ActorSystem, Context, Reply};

#[derive(Default)]
struct Counter { value: i64 }

enum Msg { Inc(i64), Get(Reply<i64>) }

impl Actor for Counter {
    type Msg = Msg;

    async fn handle(&mut self, msg: Msg, _ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            Msg::Inc(n) if n < 0 => return Err("від'ємне число".into()),
            Msg::Inc(n) => self.value += n,
            Msg::Get(reply) => { let _ = reply.send(self.value); }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), ActorError> {
    let system = ActorSystem::new("app");
    let counter = system.spawn("counter", Counter::default())?;
    counter.tell(Msg::Inc(5)).await?;
    counter.tell(Msg::Inc(-1)).await?;             // помилка -> Resume, стан збережено
    println!("{}", counter.ask(Msg::Get).await?);  // 5
    system.shutdown().await;
    Ok(())
}
```

## Ліцензія

MIT
