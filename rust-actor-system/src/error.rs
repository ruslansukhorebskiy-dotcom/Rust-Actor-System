use std::any::Any;
use std::fmt;
use std::time::Duration;

/// Помилка, яку повертає обробник актора.
///
/// Це `Box<dyn Error>`, тому оператор `?` працює з будь-яким типом помилки,
/// а рядок можна перетворити через `.into()`: `Err("щось пішло не так".into())`.
pub type ActorError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Паніка, перехоплена всередині обробника актора або асинхронної задачі.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PanicError(pub String);

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "паніка: {}", self.0)
    }
}

impl std::error::Error for PanicError {}

/// Асинхронна задача не завершилась за відведений час.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("задача не завершилась за {0:?} (таймаут)")]
pub struct TaskTimeout(pub Duration);

/// Помилка надсилання повідомлення.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    #[error("актор не працює (поштову скриньку закрито)")]
    Closed,
    #[error("поштова скринька актора переповнена")]
    Full,
}

/// Помилка запиту з очікуванням відповіді (`ask`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AskError {
    #[error("актор не працює (поштову скриньку закрито)")]
    Closed,
    #[error("актор не надіслав відповідь (помилка під час обробки)")]
    NoReply,
    #[error("відповідь не отримано за {0:?}")]
    Timeout(Duration),
}

/// Помилка створення актора.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpawnError {
    #[error("актор з іменем `{0}` вже існує")]
    NameTaken(String),
    #[error("акторна система завершує роботу")]
    ShuttingDown,
}

pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "невідомий вміст паніки".to_string()
    }
}

pub(crate) fn panic_error(payload: Box<dyn Any + Send>) -> ActorError {
    Box::new(PanicError(panic_message(payload)))
}
