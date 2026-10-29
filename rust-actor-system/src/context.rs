use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use tokio::task::AbortHandle;

use crate::actor::Actor;
use crate::addr::ActorRef;
use crate::error::{ActorError, TaskTimeout, panic_error};
use crate::system::ActorSystem;

/// Контекст виконання актора: доступ до власної адреси, системи
/// та запуску асинхронних задач.
pub struct Context<A: Actor> {
    pub(crate) addr: ActorRef<A>,
    pub(crate) system: ActorSystem,
    pub(crate) stopping: bool,
    tasks: Vec<AbortHandle>,
}

impl<A: Actor> Context<A> {
    pub(crate) fn new(addr: ActorRef<A>, system: ActorSystem) -> Self {
        Self {
            addr,
            system,
            stopping: false,
            tasks: Vec::new(),
        }
    }

    /// Власна адреса актора (щоб передати її іншим акторам).
    pub fn address(&self) -> ActorRef<A> {
        self.addr.clone()
    }

    pub fn name(&self) -> &str {
        self.addr.name()
    }

    /// Система, у якій працює актор (наприклад, щоб створити дочірніх акторів).
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Зупинити актора після завершення поточного обробника.
    pub fn stop(&mut self) {
        self.stopping = true;
    }

    /// Кількість запущених і ще не завершених фонових задач.
    pub fn running_tasks(&mut self) -> usize {
        self.tasks.retain(|h| !h.is_finished());
        self.tasks.len()
    }

    /// Запустити асинхронну задачу у фоні, не блокуючи скриньку актора.
    ///
    /// Результат задачі (успіх, помилка або перехоплена паніка) перетворюється
    /// функцією `on_done` на повідомлення і надсилається цьому ж актору.
    pub fn run_task<T, Fut, F>(&mut self, fut: Fut, on_done: F)
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, ActorError>> + Send + 'static,
        F: FnOnce(Result<T, ActorError>) -> A::Msg + Send + 'static,
    {
        self.spawn_task(fut, None, on_done);
    }

    /// Як [`run_task`](Self::run_task), але задача, що не встигла за `timeout`,
    /// скасовується і повертає [`TaskTimeout`].
    pub fn run_task_with_timeout<T, Fut, F>(&mut self, timeout: Duration, fut: Fut, on_done: F)
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, ActorError>> + Send + 'static,
        F: FnOnce(Result<T, ActorError>) -> A::Msg + Send + 'static,
    {
        self.spawn_task(fut, Some(timeout), on_done);
    }

    /// Надіслати собі повідомлення із затримкою.
    pub fn send_later(&mut self, delay: Duration, msg: A::Msg) {
        self.run_task(
            async move {
                tokio::time::sleep(delay).await;
                Ok(())
            },
            move |_| msg,
        );
    }

    fn spawn_task<T, Fut, F>(&mut self, fut: Fut, timeout: Option<Duration>, on_done: F)
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, ActorError>> + Send + 'static,
        F: FnOnce(Result<T, ActorError>) -> A::Msg + Send + 'static,
    {
        self.tasks.retain(|h| !h.is_finished());
        let addr = self.addr.clone();

        let handle = tokio::spawn(async move {
            let guarded = AssertUnwindSafe(fut).catch_unwind();
            let caught = match timeout {
                Some(limit) => match tokio::time::timeout(limit, guarded).await {
                    Ok(caught) => caught,
                    Err(_) => Ok(Err(Box::new(TaskTimeout(limit)) as ActorError)),
                },
                None => guarded.await,
            };
            let result = caught.unwrap_or_else(|payload| Err(panic_error(payload)));
            // Якщо актор уже зупинився — результат нікому не потрібен.
            let _ = addr.tell(on_done(result)).await;
        });

        self.tasks.push(handle.abort_handle());
    }

    /// Скасувати всі фонові задачі (при зупинці або перезапуску).
    pub(crate) fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}
