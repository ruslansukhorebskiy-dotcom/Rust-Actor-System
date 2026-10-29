use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::actor::Actor;
use crate::error::{AskError, SendError};

/// Канал для відповіді на запит `ask`.
pub type Reply<T> = oneshot::Sender<T>;

pub(crate) enum Envelope<M> {
    Msg(M),
    Stop,
}

/// Адреса актора. Дешево клонується і може передаватись між потоками
/// та іншими акторами — саме через неї актори обмінюються повідомленнями.
pub struct ActorRef<A: Actor> {
    tx: mpsc::Sender<Envelope<A::Msg>>,
    name: Arc<str>,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            name: self.name.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("name", &self.name)
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl<A: Actor> ActorRef<A> {
    pub(crate) fn new(tx: mpsc::Sender<Envelope<A::Msg>>, name: Arc<str>) -> Self {
        Self { tx, name }
    }

    pub(crate) fn same_actor(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }

    /// Ім'я актора.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Чи актор ще працює.
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Надіслати повідомлення без очікування відповіді («fire-and-forget»).
    /// Якщо скринька заповнена — чекає, поки звільниться місце (backpressure).
    pub async fn tell(&self, msg: A::Msg) -> Result<(), SendError> {
        self.tx
            .send(Envelope::Msg(msg))
            .await
            .map_err(|_| SendError::Closed)
    }

    /// Надіслати повідомлення без очікування; помилка, якщо скринька заповнена.
    pub fn try_tell(&self, msg: A::Msg) -> Result<(), SendError> {
        self.tx.try_send(Envelope::Msg(msg)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })
    }

    /// Запит-відповідь. `make` будує повідомлення, вкладаючи в нього канал відповіді:
    ///
    /// ```ignore
    /// let value = counter.ask(CounterMsg::Get).await?;
    /// ```
    pub async fn ask<R, F>(&self, make: F) -> Result<R, AskError>
    where
        F: FnOnce(Reply<R>) -> A::Msg,
    {
        let (tx, rx) = oneshot::channel();
        self.tell(make(tx)).await.map_err(|_| AskError::Closed)?;
        rx.await.map_err(|_| AskError::NoReply)
    }

    /// Те саме, що [`ask`](Self::ask), але з обмеженням часу очікування.
    pub async fn ask_timeout<R, F>(&self, make: F, timeout: Duration) -> Result<R, AskError>
    where
        F: FnOnce(Reply<R>) -> A::Msg,
    {
        tokio::time::timeout(timeout, self.ask(make))
            .await
            .map_err(|_| AskError::Timeout(timeout))?
    }

    /// Попросити актора зупинитись. Повідомлення, що вже в черзі, буде оброблено.
    pub async fn stop(&self) {
        let _ = self.tx.send(Envelope::Stop).await;
    }

    /// Дочекатися повної зупинки актора (після `stopped()`).
    pub async fn wait_stopped(&self) {
        self.tx.closed().await;
    }
}
