//! Демонстраційні сценарії: ping-pong, пул воркерів, супервізія.

use std::time::{Duration, Instant};

use actor_system::{
    Actor, ActorError, ActorRef, ActorSystem, Context, Directive, PanicError, Reply, SpawnOptions,
    TaskTimeout,
};

// ===========================================================================
// 1. Ping-pong — обмін повідомленнями між двома акторами
// ===========================================================================

struct Ping {
    pong: ActorRef<Pong>,
    rounds: u32,
    done: Option<Reply<u32>>,
}

enum PingMsg {
    Start { rounds: u32, done: Reply<u32> },
    Pong(u32),
}

impl Actor for Ping {
    type Msg = PingMsg;

    async fn handle(&mut self, msg: PingMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            PingMsg::Start { rounds, done } => {
                self.rounds = rounds;
                self.done = Some(done);
                let me = ctx.address();
                self.pong.tell(PongMsg::Ping { n: 1, reply_to: me }).await?;
            }
            PingMsg::Pong(n) => {
                println!("  ping <- pong #{n}");
                if n < self.rounds {
                    let me = ctx.address();
                    self.pong
                        .tell(PongMsg::Ping {
                            n: n + 1,
                            reply_to: me,
                        })
                        .await?;
                } else if let Some(done) = self.done.take() {
                    let _ = done.send(n);
                }
            }
        }
        Ok(())
    }
}

struct Pong;

enum PongMsg {
    Ping { n: u32, reply_to: ActorRef<Ping> },
}

impl Actor for Pong {
    type Msg = PongMsg;

    async fn handle(&mut self, msg: PongMsg, _ctx: &mut Context<Self>) -> Result<(), ActorError> {
        let PongMsg::Ping { n, reply_to } = msg;
        println!("  pong <- ping #{n}");
        reply_to.tell(PingMsg::Pong(n)).await?;
        Ok(())
    }
}

pub async fn ping_pong(system: &ActorSystem, rounds: u32) -> Result<(), ActorError> {
    println!("=== Ping-pong: {rounds} раунд(ів) ===");
    let started = Instant::now();
    let pong = system.spawn("pong", Pong)?;
    let ping = system.spawn(
        "ping",
        Ping {
            pong: pong.clone(),
            rounds: 0,
            done: None,
        },
    )?;

    let done = ping
        .ask_timeout(
            |done| PingMsg::Start { rounds, done },
            Duration::from_secs(30),
        )
        .await?;

    println!(
        "  завершено {done} раунд(ів), {} повідомлень за {:.2?}\n",
        done * 2,
        started.elapsed()
    );
    ping.stop().await;
    pong.stop().await;
    ping.wait_stopped().await;
    pong.wait_stopped().await;
    Ok(())
}

// ===========================================================================
// 2. Пул воркерів, що виконують асинхронні задачі
// ===========================================================================

/// Модель обчислювальної задачі з передбачуваними відмовами:
/// 13 — паніка, кратні 7 — помилка, > 40 — «зависання», інакше — квадрат числа.
pub async fn compute(input: u64) -> Result<u64, ActorError> {
    let work = Duration::from_millis((20 + input * 5).min(300));
    tokio::time::sleep(work).await;
    match input {
        13 => panic!("нещасливе число 13"),
        n if n % 7 == 0 => Err(format!("{n} кратне 7 — задачу відхилено").into()),
        n if n > 40 => {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(n)
        }
        n => Ok(n * n),
    }
}

/// Результат виконання задачі.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    Success(u64),
    Failed(String),
    Panicked(String),
    TimedOut,
}

impl JobOutcome {
    fn from_result(result: Result<u64, ActorError>) -> Self {
        match result {
            Ok(v) => Self::Success(v),
            Err(e) if e.is::<TaskTimeout>() => Self::TimedOut,
            Err(e) => match e.downcast::<PanicError>() {
                Ok(p) => Self::Panicked(p.0),
                Err(e) => Self::Failed(e.to_string()),
            },
        }
    }
}

struct Worker {
    id: usize,
    timeout: Duration,
    /// true — задача виконується прямо в обробнику (скринька заблокована до завершення);
    /// false — у фоновій задачі (`run_task_with_timeout`).
    inline: bool,
}

/// Виконати задачу в обробнику з таймаутом і перехопленням паніки.
async fn run_inline(timeout: Duration, input: u64) -> Result<u64, ActorError> {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;
    match tokio::time::timeout(timeout, AssertUnwindSafe(compute(input)).catch_unwind()).await {
        Err(_) => Err(Box::new(TaskTimeout(timeout))),
        Ok(Ok(result)) => result,
        Ok(Err(payload)) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "невідома паніка".into());
            Err(Box::new(PanicError(msg)))
        }
    }
}

enum WorkerMsg {
    Job {
        job_id: usize,
        input: u64,
        reply_to: ActorRef<Coordinator>,
    },
    Finished {
        job_id: usize,
        input: u64,
        started: Instant,
        reply_to: ActorRef<Coordinator>,
        result: Result<u64, ActorError>,
    },
}

impl Actor for Worker {
    type Msg = WorkerMsg;

    async fn handle(&mut self, msg: WorkerMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            // Задача запускається у фоні — скринька воркера не блокується.
            WorkerMsg::Job {
                job_id,
                input,
                reply_to,
            } => {
                log::debug!(
                    "[worker-{}] отримав задачу #{job_id} (вхід {input})",
                    self.id
                );
                let started = Instant::now();
                if self.inline {
                    let result = run_inline(self.timeout, input).await;
                    let report = JobReport {
                        job_id,
                        input,
                        worker: self.id,
                        elapsed: started.elapsed(),
                        outcome: JobOutcome::from_result(result),
                    };
                    reply_to.tell(CoordMsg::JobDone(report)).await?;
                    return Ok(());
                }
                ctx.run_task_with_timeout(self.timeout, compute(input), move |result| {
                    WorkerMsg::Finished {
                        job_id,
                        input,
                        started,
                        reply_to,
                        result,
                    }
                });
            }
            WorkerMsg::Finished {
                job_id,
                input,
                started,
                reply_to,
                result,
            } => {
                let report = JobReport {
                    job_id,
                    input,
                    worker: self.id,
                    elapsed: started.elapsed(),
                    outcome: JobOutcome::from_result(result),
                };
                reply_to.tell(CoordMsg::JobDone(report)).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct JobReport {
    pub job_id: usize,
    pub input: u64,
    pub worker: usize,
    pub elapsed: Duration,
    pub outcome: JobOutcome,
}

struct Coordinator {
    pool_size: usize,
    timeout: Duration,
    inline: bool,
    workers: Vec<ActorRef<Worker>>,
    next: usize,
    pending: usize,
    reports: Vec<JobReport>,
    waiters: Vec<Reply<Vec<JobReport>>>,
}

enum CoordMsg {
    Submit(Vec<u64>),
    JobDone(JobReport),
    AwaitAll(Reply<Vec<JobReport>>),
}

impl Coordinator {
    fn notify_if_done(&mut self) {
        if self.pending == 0 {
            for waiter in self.waiters.drain(..) {
                let _ = waiter.send(self.reports.clone());
            }
        }
    }
}

impl Actor for Coordinator {
    type Msg = CoordMsg;

    // Координатор створює дочірніх акторів під час запуску.
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        for id in 0..self.pool_size {
            let (timeout, inline) = (self.timeout, self.inline);
            let worker = ctx.system().spawn_with(
                format!("worker-{id}"),
                move || Worker {
                    id,
                    timeout,
                    inline,
                },
                SpawnOptions::default(),
            )?;
            self.workers.push(worker);
        }
        log::info!("координатор запустив {} воркер(ів)", self.workers.len());
        Ok(())
    }

    async fn handle(&mut self, msg: CoordMsg, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            CoordMsg::Submit(inputs) => {
                for input in inputs {
                    // Розподіл задач по колу (round-robin).
                    let worker = &self.workers[self.next % self.workers.len()];
                    let job_id = self.next + 1;
                    self.next += 1;
                    self.pending += 1;
                    let me = ctx.address();
                    worker
                        .tell(WorkerMsg::Job {
                            job_id,
                            input,
                            reply_to: me,
                        })
                        .await?;
                }
            }
            CoordMsg::JobDone(report) => {
                self.pending -= 1;
                let text = match &report.outcome {
                    JobOutcome::Success(v) => format!("✔ успіх, результат = {v}"),
                    JobOutcome::Failed(e) => format!("✘ помилка: {e}"),
                    JobOutcome::Panicked(e) => format!("✘ паніка: {e}"),
                    JobOutcome::TimedOut => "✘ таймаут".to_string(),
                };
                println!(
                    "  задача #{:<3} вхід {:>4}  worker-{}  {:>7.1?}  {text}",
                    report.job_id, report.input, report.worker, report.elapsed
                );
                log::debug!(
                    "задача #{} (worker-{}) завершена: {:?}",
                    report.job_id,
                    report.worker,
                    report.outcome
                );
                self.reports.push(report);
                self.notify_if_done();
            }
            CoordMsg::AwaitAll(reply) => {
                self.waiters.push(reply);
                self.notify_if_done();
            }
        }
        Ok(())
    }
}

/// Запустити пул і повернути звіти про всі задачі.
pub async fn run_pool(
    system: &ActorSystem,
    workers: usize,
    jobs: Vec<u64>,
    timeout: Duration,
    inline: bool,
) -> Result<Vec<JobReport>, ActorError> {
    // Скринька координатора вміщує всі звіти: інакше при великій кількості задач
    // координатор (чекає на місце у скриньці воркера) і воркер (чекає на місце
    // у скриньці координатора) заблокували б один одного.
    let options = SpawnOptions {
        mailbox_capacity: jobs.len() + 16,
        max_restarts: 0,
    };
    let coordinator = system.spawn_with(
        "coordinator",
        move || Coordinator {
            pool_size: workers,
            timeout,
            inline,
            workers: Vec::new(),
            next: 0,
            pending: 0,
            reports: Vec::new(),
            waiters: Vec::new(),
        },
        options,
    )?;
    coordinator.tell(CoordMsg::Submit(jobs)).await?;
    let reports = coordinator.ask(CoordMsg::AwaitAll).await?;
    coordinator.stop().await;
    coordinator.wait_stopped().await;
    Ok(reports)
}

pub async fn pool(
    system: &ActorSystem,
    workers: usize,
    jobs: Vec<u64>,
    timeout_ms: u64,
    inline: bool,
) -> Result<(), ActorError> {
    let mode = if inline {
        "в обробнику"
    } else {
        "фонові задачі"
    };
    println!(
        "=== Пул воркерів: {workers} воркер(ів), {} задач(і), таймаут {timeout_ms} мс, режим: {mode} ===",
        jobs.len()
    );
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    let reports = run_pool(system, workers, jobs, timeout, inline).await?;
    let elapsed = started.elapsed();
    let sequential: Duration = reports.iter().map(|r| r.elapsed).sum();

    let count = |f: fn(&JobOutcome) -> bool| reports.iter().filter(|r| f(&r.outcome)).count();
    println!("  ------------------------------------------------------------");
    println!("  усього задач:   {}", reports.len());
    println!(
        "  успішних:       {}",
        count(|o| matches!(o, JobOutcome::Success(_)))
    );
    println!(
        "  помилок:        {}",
        count(|o| matches!(o, JobOutcome::Failed(_)))
    );
    println!(
        "  панік:          {}",
        count(|o| matches!(o, JobOutcome::Panicked(_)))
    );
    println!(
        "  таймаутів:      {}",
        count(|o| matches!(o, JobOutcome::TimedOut))
    );
    println!("  загальний час:  {elapsed:.2?} (сума часу задач {sequential:.2?})\n");
    Ok(())
}

// ===========================================================================
// 3. Супервізія — обробка помилок і паніки
// ===========================================================================

#[derive(Default)]
struct Accumulator {
    total: i64,
}

enum AccMsg {
    Add(i64, Reply<i64>),
    Slow(Reply<()>),
}

impl Actor for Accumulator {
    type Msg = AccMsg;

    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<(), ActorError> {
        println!("  [{}] запущено, total = {}", ctx.name(), self.total);
        Ok(())
    }

    async fn handle(&mut self, msg: AccMsg, _ctx: &mut Context<Self>) -> Result<(), ActorError> {
        match msg {
            AccMsg::Add(0, _) => panic!("додавання нуля заборонено"),
            AccMsg::Add(n, _) if n < 0 => return Err(format!("від'ємне число: {n}").into()),
            AccMsg::Add(n, reply) => {
                self.total = self.total.checked_add(n).ok_or("переповнення суми")?;
                let _ = reply.send(self.total);
            }
            AccMsg::Slow(reply) => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = reply.send(());
            }
        }
        Ok(())
    }

    fn on_error(&mut self, err: &ActorError, ctx: &mut Context<Self>) -> Directive {
        if err.is::<PanicError>() {
            println!("  [{}] {err} -> Restart", ctx.name());
            Directive::Restart
        } else {
            println!(
                "  [{}] помилка «{err}» -> Resume (стан збережено)",
                ctx.name()
            );
            Directive::Resume
        }
    }

    async fn stopped(&mut self) {
        println!("  [accumulator] зупинено, total = {}", self.total);
    }
}

pub async fn supervision(system: &ActorSystem, max_restarts: u32) -> Result<(), ActorError> {
    println!("=== Супервізія: ліміт перезапусків {max_restarts} ===");
    let acc = system.spawn_with(
        "accumulator",
        Accumulator::default,
        SpawnOptions {
            max_restarts,
            ..Default::default()
        },
    )?;

    let show = |label: &str, value: String| println!("  {label:<22} -> {value}");
    fn res<T: std::fmt::Debug>(r: Result<T, actor_system::AskError>) -> String {
        match r {
            Ok(v) => format!("Ok({v:?})"),
            Err(e) => format!("Err: {e}"),
        }
    }

    show("add 10", res(acc.ask(|r| AccMsg::Add(10, r)).await));
    show("add 5", res(acc.ask(|r| AccMsg::Add(5, r)).await));
    show(
        "add -3 (помилка)",
        res(acc.ask(|r| AccMsg::Add(-3, r)).await),
    );
    show("add 1", res(acc.ask(|r| AccMsg::Add(1, r)).await));
    show("add 0 (паніка)", res(acc.ask(|r| AccMsg::Add(0, r)).await));
    show("add 2", res(acc.ask(|r| AccMsg::Add(2, r)).await));
    let slow = acc
        .ask_timeout(AccMsg::Slow, Duration::from_millis(100))
        .await;
    show("slow (таймаут 100 мс)", res(slow));

    // Вичерпуємо ліміт перезапусків.
    let mut panics = 0;
    while acc.is_alive() && panics <= max_restarts {
        let _ = acc.ask(|r| AccMsg::Add(0, r)).await;
        panics += 1;
    }
    acc.wait_stopped().await;
    show("актор живий?", acc.is_alive().to_string());
    show(
        "add 1 (після зупинки)",
        res(acc.ask(|r| AccMsg::Add(1, r)).await),
    );
    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pool_classifies_all_outcomes() {
        std::panic::set_hook(Box::new(|_| {}));
        let system = ActorSystem::new("test");
        let mut reports = run_pool(
            &system,
            2,
            vec![3, 7, 13, 45],
            Duration::from_millis(150),
            false,
        )
        .await
        .unwrap();
        reports.sort_by_key(|r| r.job_id);
        let outcomes: Vec<_> = reports.into_iter().map(|r| r.outcome).collect();
        assert_eq!(outcomes[0], JobOutcome::Success(9));
        assert!(matches!(outcomes[1], JobOutcome::Failed(_)));
        assert!(matches!(outcomes[2], JobOutcome::Panicked(_)));
        assert_eq!(outcomes[3], JobOutcome::TimedOut);
        system.shutdown().await;
    }

    #[tokio::test]
    async fn inline_mode_and_many_jobs_do_not_deadlock() {
        std::panic::set_hook(Box::new(|_| {}));
        for inline in [false, true] {
            let system = ActorSystem::new("test");
            let jobs: Vec<u64> = (0..200).map(|i| [1, 2, 7, 13][i % 4]).collect();
            let reports = tokio::time::timeout(
                Duration::from_secs(30),
                run_pool(&system, 2, jobs, Duration::from_millis(200), inline),
            )
            .await
            .expect("пул не повинен зависати")
            .unwrap();
            assert_eq!(reports.len(), 200);
            let panics = reports
                .iter()
                .filter(|r| matches!(r.outcome, JobOutcome::Panicked(_)))
                .count();
            assert_eq!(panics, 50);
            system.shutdown().await;
        }
    }
}
