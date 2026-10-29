use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::actor::Actor;
use crate::addr::ActorRef;
use crate::context::Context;
use crate::error::SpawnError;
use crate::runtime::{Factory, run_actor};

/// Налаштування актора при створенні.
#[derive(Debug, Clone)]
pub struct SpawnOptions {
    /// Розмір поштової скриньки (обмежена черга -> backpressure).
    pub mailbox_capacity: usize,
    /// Скільки разів супервізор може перезапустити актора, перш ніж зупинити його.
    pub max_restarts: u32,
}

impl Default for SpawnOptions {
    fn default() -> Self {
        Self {
            mailbox_capacity: 64,
            max_restarts: 3,
        }
    }
}

/// Акторна система: створює акторів, зберігає реєстр за іменами
/// та керує їх завершенням.
#[derive(Clone)]
pub struct ActorSystem {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    registry: Mutex<HashMap<String, Box<dyn Any + Send + Sync>>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    shutdown_tx: watch::Sender<bool>,
}

impl ActorSystem {
    pub fn new(name: impl Into<String>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                name: name.into(),
                registry: Mutex::new(HashMap::new()),
                handles: Mutex::new(Vec::new()),
                shutdown_tx,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Створити актора з готового значення. Без фабрики директива
    /// `Restart` неможлива — актор буде зупинений.
    pub fn spawn<A: Actor>(
        &self,
        name: impl Into<String>,
        actor: A,
    ) -> Result<ActorRef<A>, SpawnError> {
        self.spawn_inner(name.into(), actor, None, SpawnOptions::default())
    }

    /// Створити актора через фабрику: супервізор зможе перезапускати його
    /// з чистим станом.
    pub fn spawn_with<A, F>(
        &self,
        name: impl Into<String>,
        factory: F,
        options: SpawnOptions,
    ) -> Result<ActorRef<A>, SpawnError>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        let actor = factory();
        self.spawn_inner(name.into(), actor, Some(Box::new(factory)), options)
    }

    fn spawn_inner<A: Actor>(
        &self,
        name: String,
        actor: A,
        factory: Option<Factory<A>>,
        options: SpawnOptions,
    ) -> Result<ActorRef<A>, SpawnError> {
        if self.is_shutting_down() {
            return Err(SpawnError::ShuttingDown);
        }

        let (tx, rx) = mpsc::channel(options.mailbox_capacity.max(1));
        let addr = ActorRef::new(tx, Arc::from(name.as_str()));

        {
            let mut registry = self.inner.registry.lock().unwrap();
            if registry.contains_key(&name) {
                return Err(SpawnError::NameTaken(name));
            }
            registry.insert(name, Box::new(addr.clone()));
        }

        log::debug!(
            "[{}] актора створено (скринька: {})",
            addr.name(),
            options.mailbox_capacity
        );
        let ctx = Context::new(addr.clone(), self.clone());
        let shutdown_rx = self.inner.shutdown_tx.subscribe();
        let handle = tokio::spawn(run_actor(actor, factory, rx, ctx, options, shutdown_rx));

        let mut handles = self.inner.handles.lock().unwrap();
        handles.retain(|h| !h.is_finished());
        handles.push(handle);

        Ok(addr)
    }

    /// Знайти актора за іменем. Повертає `None`, якщо такого немає
    /// або він іншого типу.
    pub fn lookup<A: Actor>(&self, name: &str) -> Option<ActorRef<A>> {
        let registry = self.inner.registry.lock().unwrap();
        registry.get(name)?.downcast_ref::<ActorRef<A>>().cloned()
    }

    /// Імена всіх живих акторів.
    pub fn actor_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .inner
            .registry
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    pub(crate) fn unregister<A: Actor>(&self, addr: &ActorRef<A>) {
        let mut registry = self.inner.registry.lock().unwrap();
        let is_same = registry
            .get(addr.name())
            .and_then(|entry| entry.downcast_ref::<ActorRef<A>>())
            .is_some_and(|existing| existing.same_actor(addr));
        if is_same {
            registry.remove(addr.name());
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.inner.shutdown_tx.borrow()
    }

    /// Зупинити всіх акторів і дочекатися їх завершення.
    /// Необроблені повідомлення в скриньках відкидаються.
    pub async fn shutdown(&self) {
        log::info!("система `{}`: завершення роботи", self.name());
        self.inner.shutdown_tx.send_replace(true);
        loop {
            let handles = std::mem::take(&mut *self.inner.handles.lock().unwrap());
            if handles.is_empty() {
                break;
            }
            for handle in handles {
                let _ = handle.await;
            }
        }
    }
}
