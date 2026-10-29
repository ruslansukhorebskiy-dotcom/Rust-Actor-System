//! # mini_actors — базова акторна система на Rust + Tokio
//!
//! * [`Actor`] — трейт актора: стан + асинхронна обробка повідомлень по одному.
//! * [`ActorRef`] — адреса актора: `tell` (без відповіді), `ask` (запит-відповідь).
//! * [`Context`] — запуск фонових асинхронних задач, створення інших акторів, зупинка.
//! * [`ActorSystem`] — створення акторів, реєстр за іменами, коректне завершення.
//! * Обробка помилок: помилки та паніки перехоплюються, а [`Actor::on_error`]
//!   повертає [`Directive`] — `Resume`, `Restart` або `Stop`.

mod actor;
mod addr;
mod context;
mod error;
mod runtime;
mod system;

pub use actor::{Actor, Directive};
pub use addr::{ActorRef, Reply};
pub use context::Context;
pub use error::{ActorError, AskError, PanicError, SendError, SpawnError, TaskTimeout};
pub use system::{ActorSystem, SpawnOptions};
