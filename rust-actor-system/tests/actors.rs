use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use actor_system::{
    Actor, ActorError, ActorSystem, AskError, Context, Directive, PanicError, Reply, SendError,
    SpawnError, SpawnOptions, TaskTimeout,
};

#[derive(Default)]
struct Counter {
    value: i64,
}

enum CounterMsg {
    Inc(i64),
    Get(Reply<i64>),
    Fail,
    Panic,
    StopSelf,
    Sleep(Duration, Reply<()>),
}

impl Actor for Counter {
    type Msg = CounterMsg;

    async fn handle(&mut self, msg: CounterMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            CounterMsg::Inc(n) => self.value += n,
            CounterMsg::Get(r) => {
                let _ = r.send(self.value);
            }
            CounterMsg::Fail => return Err("boom".into()),
            CounterMsg::Panic => panic!("kaboom"),
            CounterMsg::StopSelf => ctx.stop(),
            CounterMsg::Sleep(d, r) => {
                tokio::time::sleep(d).await;
                let _ = r.send(());
            }
        }
        Ok(())
    }
}

fn quiet_panics() {
    std::panic::set_hook(Box::new(|_| {}));
}

#[tokio::test]
async fn tell_and_ask() {
    let sys = ActorSystem::new("t");
    let c = sys.spawn("c", Counter::default()).unwrap();
    for _ in 0..10 {
        c.tell(CounterMsg::Inc(2)).await.unwrap();
    }
    assert_eq!(c.ask(CounterMsg::Get).await.unwrap(), 20);
    sys.shutdown().await;
}

#[tokio::test]
async fn error_resumes_with_state_kept() {
    let sys = ActorSystem::new("t");
    let c = sys.spawn("c", Counter::default()).unwrap();
    c.tell(CounterMsg::Inc(5)).await.unwrap();
    c.tell(CounterMsg::Fail).await.unwrap();
    assert_eq!(c.ask(CounterMsg::Get).await.unwrap(), 5);
    sys.shutdown().await;
}

#[tokio::test]
async fn panic_restarts_with_fresh_state() {
    quiet_panics();
    let sys = ActorSystem::new("t");
    let c = sys
        .spawn_with("c", Counter::default, SpawnOptions::default())
        .unwrap();
    c.tell(CounterMsg::Inc(5)).await.unwrap();
    c.tell(CounterMsg::Panic).await.unwrap();
    assert_eq!(c.ask(CounterMsg::Get).await.unwrap(), 0);
    assert!(c.is_alive());
    sys.shutdown().await;
}

#[tokio::test]
async fn panic_without_factory_stops_actor() {
    quiet_panics();
    let sys = ActorSystem::new("t");
    let c = sys.spawn("c", Counter::default()).unwrap();
    c.tell(CounterMsg::Panic).await.unwrap();
    c.wait_stopped().await;
    assert_eq!(c.tell(CounterMsg::Inc(1)).await, Err(SendError::Closed));
    assert!(sys.lookup::<Counter>("c").is_none());
}

#[tokio::test]
async fn restart_limit_stops_actor() {
    quiet_panics();
    let sys = ActorSystem::new("t");
    let c = sys
        .spawn_with(
            "c",
            Counter::default,
            SpawnOptions {
                max_restarts: 2,
                ..Default::default()
            },
        )
        .unwrap();
    for _ in 0..3 {
        let _ = c.tell(CounterMsg::Panic).await;
    }
    c.wait_stopped().await;
    assert!(!c.is_alive());
}

#[tokio::test]
async fn ask_errors() {
    let sys = ActorSystem::new("t");
    let c = sys.spawn("c", Counter::default()).unwrap();

    let timeout = c
        .ask_timeout(
            |r| CounterMsg::Sleep(Duration::from_millis(200), r),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(timeout, Err(AskError::Timeout(Duration::from_millis(20))));

    c.stop().await;
    c.wait_stopped().await;
    assert_eq!(c.ask(CounterMsg::Get).await, Err(AskError::Closed));
}

#[tokio::test]
async fn stop_from_context_and_registry() {
    let sys = ActorSystem::new("t");
    let c = sys.spawn("c", Counter::default()).unwrap();
    assert!(sys.lookup::<Counter>("c").is_some());
    assert_eq!(
        sys.spawn("c", Counter::default()).unwrap_err(),
        SpawnError::NameTaken("c".into())
    );

    c.tell(CounterMsg::StopSelf).await.unwrap();
    c.wait_stopped().await;
    assert!(sys.lookup::<Counter>("c").is_none());
    // ім'я звільнилось
    assert!(sys.spawn("c", Counter::default()).is_ok());
    sys.shutdown().await;
}

// --- Асинхронні задачі ------------------------------------------------------

struct Runner;

enum RunnerMsg {
    Run(u32, Reply<Result<u32, String>>),
    RunWithTimeout(Reply<bool>),
    Done(Result<u32, ActorError>, Reply<Result<u32, String>>),
    TimedOut(Result<(), ActorError>, Reply<bool>),
}

impl Actor for Runner {
    type Msg = RunnerMsg;

    async fn handle(&mut self, msg: RunnerMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            RunnerMsg::Run(n, reply) => ctx.run_task(
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    match n {
                        0 => Err("zero".into()),
                        1 => panic!("one"),
                        n => Ok(n * 10),
                    }
                },
                move |res| RunnerMsg::Done(res, reply),
            ),
            RunnerMsg::RunWithTimeout(reply) => ctx.run_task_with_timeout(
                Duration::from_millis(20),
                async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(())
                },
                move |res| RunnerMsg::TimedOut(res, reply),
            ),
            RunnerMsg::Done(res, reply) => {
                let _ = reply.send(res.map_err(|e| {
                    if e.is::<PanicError>() {
                        "panic".into()
                    } else {
                        e.to_string()
                    }
                }));
            }
            RunnerMsg::TimedOut(res, reply) => {
                let _ = reply.send(res.is_err_and(|e| e.is::<TaskTimeout>()));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn async_tasks_success_error_panic_timeout() {
    quiet_panics();
    let sys = ActorSystem::new("t");
    let r = sys.spawn("runner", Runner).unwrap();

    assert_eq!(r.ask(|tx| RunnerMsg::Run(4, tx)).await.unwrap(), Ok(40));
    assert_eq!(
        r.ask(|tx| RunnerMsg::Run(0, tx)).await.unwrap(),
        Err("zero".into())
    );
    assert_eq!(
        r.ask(|tx| RunnerMsg::Run(1, tx)).await.unwrap(),
        Err("panic".into())
    );
    assert!(r.ask(RunnerMsg::RunWithTimeout).await.unwrap());
    sys.shutdown().await;
}

#[tokio::test]
async fn tasks_run_concurrently_without_blocking_mailbox() {
    let sys = ActorSystem::new("t");
    let r = sys.spawn("runner", Runner).unwrap();
    let start = std::time::Instant::now();
    let asks: Vec<_> = (2..12)
        .map(|n| r.ask(move |tx| RunnerMsg::Run(n, tx)))
        .collect();
    let results = futures::future::join_all(asks).await;
    assert!(results.iter().all(|r| matches!(r, Ok(Ok(_)))));
    // 10 задач по 50 мс паралельно — значно менше ніж 500 мс послідовно
    // (запас на повільні CI-сервери)
    assert!(start.elapsed() < Duration::from_millis(300));
    sys.shutdown().await;
}

// --- Хуки та стратегія Stop -------------------------------------------------

struct Hooks {
    started: Arc<AtomicU32>,
    stopped: Arc<AtomicU32>,
}

impl Actor for Hooks {
    type Msg = ();

    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<(), ActorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, _: (), _ctx: &mut Context<Self>) -> Result<(), ActorError> {
        Err("fatal".into())
    }

    fn on_error(&mut self, _err: &ActorError, _ctx: &mut Context<Self>) -> Directive {
        Directive::Stop
    }

    async fn stopped(&mut self) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn stop_directive_runs_hooks_once() {
    let sys = ActorSystem::new("t");
    let started = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));
    let a = sys
        .spawn(
            "h",
            Hooks {
                started: started.clone(),
                stopped: stopped.clone(),
            },
        )
        .unwrap();
    a.tell(()).await.unwrap();
    a.wait_stopped().await;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_stops_everything() {
    let sys = ActorSystem::new("t");
    let refs: Vec<_> = (0..5)
        .map(|i| sys.spawn(format!("c{i}"), Counter::default()).unwrap())
        .collect();
    sys.shutdown().await;
    assert!(refs.iter().all(|r| !r.is_alive()));
    assert!(sys.actor_names().is_empty());
    assert_eq!(
        sys.spawn("late", Counter::default()).unwrap_err(),
        SpawnError::ShuttingDown
    );
}
