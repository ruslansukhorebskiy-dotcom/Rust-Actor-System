//! Цикл виконання актора та супервізія.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use tokio::sync::{mpsc, watch};

use crate::actor::{Actor, Directive};
use crate::addr::Envelope;
use crate::context::Context;
use crate::error::{ActorError, panic_error};
use crate::system::SpawnOptions;

pub(crate) type Factory<A> = Box<dyn Fn() -> A + Send + Sync>;

/// Виконати обробник, перетворивши паніку на `PanicError`.
async fn guarded<F>(fut: F) -> Result<(), ActorError>
where
    F: Future<Output = Result<(), ActorError>>,
{
    AssertUnwindSafe(fut)
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| Err(panic_error(payload)))
}

struct Supervisor<A: Actor> {
    factory: Option<Factory<A>>,
    restarts: u32,
    max_restarts: u32,
}

impl<A: Actor> Supervisor<A> {
    /// Обробити помилку актора. Повертає `true`, якщо актор продовжує роботу.
    async fn recover(&mut self, actor: &mut A, ctx: &mut Context<A>, mut err: ActorError) -> bool {
        loop {
            match actor.on_error(&err, ctx) {
                Directive::Resume => {
                    log::info!("[{}] помилка обробки, продовжую роботу: {err}", ctx.name());
                    return true;
                }
                Directive::Stop => {
                    log::error!("[{}] зупинено через помилку: {err}", ctx.name());
                    return false;
                }
                Directive::Restart => {
                    let Some(factory) = &self.factory else {
                        log::error!(
                            "[{}] перезапуск неможливий без фабрики (spawn_with), зупиняю: {err}",
                            ctx.name()
                        );
                        return false;
                    };
                    if self.restarts >= self.max_restarts {
                        log::error!(
                            "[{}] вичерпано ліміт перезапусків ({}), зупиняю: {err}",
                            ctx.name(),
                            self.max_restarts
                        );
                        return false;
                    }
                    self.restarts += 1;
                    log::warn!(
                        "[{}] перезапуск {}/{} після: {err}",
                        ctx.name(),
                        self.restarts,
                        self.max_restarts
                    );

                    ctx.abort_tasks();
                    let _ = AssertUnwindSafe(actor.stopped()).catch_unwind().await;
                    *actor = factory();

                    match guarded(actor.started(ctx)).await {
                        Ok(()) => return true,
                        Err(e) => err = e, // помилка при старті — знову до супервізора
                    }
                }
            }
        }
    }
}

pub(crate) async fn run_actor<A: Actor>(
    mut actor: A,
    factory: Option<Factory<A>>,
    mut rx: mpsc::Receiver<Envelope<A::Msg>>,
    mut ctx: Context<A>,
    options: SpawnOptions,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut supervisor = Supervisor {
        factory,
        restarts: 0,
        max_restarts: options.max_restarts,
    };

    let mut running = !*shutdown.borrow();
    if running && let Err(err) = guarded(actor.started(&mut ctx)).await {
        running = supervisor.recover(&mut actor, &mut ctx, err).await;
    }

    while running && !ctx.stopping {
        let envelope = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            envelope = rx.recv() => envelope,
        };

        match envelope {
            Some(Envelope::Msg(msg)) => {
                if let Err(err) = guarded(actor.handle(msg, &mut ctx)).await {
                    running = supervisor.recover(&mut actor, &mut ctx, err).await;
                }
            }
            Some(Envelope::Stop) | None => break,
        }
    }

    ctx.abort_tasks();
    ctx.system.unregister(&ctx.addr);
    log::debug!("[{}] актор зупинено", ctx.name());
    let _ = AssertUnwindSafe(actor.stopped()).catch_unwind().await;
    // Закриваємо скриньку: незавершені `ask` отримають `AskError::NoReply`,
    // а `wait_stopped()` повернеться.
    drop(rx);
}
