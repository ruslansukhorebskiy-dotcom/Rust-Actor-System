use std::future::Future;

use crate::context::Context;
use crate::error::{ActorError, PanicError};

/// Що робити з актором після помилки в обробнику.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    /// Пропустити повідомлення і продовжити роботу з поточним станом.
    Resume,
    /// Створити актора заново (через фабрику з `spawn_with`) з чистим станом.
    Restart,
    /// Зупинити актора.
    Stop,
}

/// Актор: незалежний об'єкт зі своїм станом, який обробляє повідомлення
/// з поштової скриньки строго по одному.
///
/// Обов'язково реалізувати лише [`Actor::handle`]; решта методів мають
/// типову поведінку.
pub trait Actor: Send + Sized + 'static {
    /// Тип повідомлень, які приймає актор.
    type Msg: Send + 'static;

    /// Викликається один раз перед обробкою першого повідомлення
    /// (і після кожного перезапуску).
    fn started(
        &mut self,
        _ctx: &mut Context<Self>,
    ) -> impl Future<Output = Result<(), ActorError>> + Send {
        async { Ok(()) }
    }

    /// Обробка одного повідомлення.
    fn handle(
        &mut self,
        msg: Self::Msg,
        ctx: &mut Context<Self>,
    ) -> impl Future<Output = Result<(), ActorError>> + Send;

    /// Супервізія: рішення після помилки в `started` або `handle`.
    ///
    /// Типово: після паніки — перезапуск, після звичайної помилки — продовжити.
    fn on_error(&mut self, err: &ActorError, _ctx: &mut Context<Self>) -> Directive {
        if err.is::<PanicError>() {
            Directive::Restart
        } else {
            Directive::Resume
        }
    }

    /// Викликається після зупинки актора (або перед перезапуском).
    fn stopped(&mut self) -> impl Future<Output = ()> + Send {
        async {}
    }
}
