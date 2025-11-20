use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;

use tokio::task::{JoinHandle, spawn};
use tokio::time::Duration;

/// A boxed future that returns a Result with the specified output type.
pub type JobFuture<T, E> = std::pin::Pin<Box<dyn Future<Output = Result<T, E>> + Send + Sync>>;

/// A job that can be run asynchronously.
pub trait Job: Send + Sync {
    type Output: Send + Sync + 'static;
    type Error: Send + Sync + 'static;
    fn run(self) -> JobFuture<Self::Output, Self::Error>;
}

/// A sequence of jobs to be executed in order.
pub struct JobSequence<T, E> {
    chain: Box<dyn FnOnce() -> JobFuture<T, E> + Send + Sync>,
}

// Implement methods for JobSequence
impl<T: Send + Sync + 'static, E: Send + Sync + 'static> JobSequence<T, E> {
    /// Start a new job sequence with the given job.
    pub fn start_with<J: Job<Output = T, Error = E> + 'static>(job: J) -> Self {
        Self {
            chain: Box::new(move || Box::pin(async move { job.run().await })),
        }
    }

    /// Takes the output of the previous job (whether success or failure) and passes it to the provided function,
    /// which returns the next job to be executed.
    pub fn take_then<Fut, Out, Err, J>(self, func: Fut) -> JobSequence<Out, Err>
    where
        Fut: FnOnce(Result<T, E>) -> J + Send + Sync + 'static,
        Out: Send + Sync + 'static,
        Err: Send + Sync + 'static + std::convert::From<E>,
        J: Job<Output = Out, Error = Err> + Send + Sync + 'static,
    {
        let previous_chain = self.chain;
        JobSequence {
            chain: Box::new(move || {
                let fut = previous_chain();
                Box::pin(async move {
                    let output = fut.await;
                    let job = func(output);
                    job.run().await
                })
            }),
        }
    }

    /// Chains another job to be executed after the previous one, ignoring its output.
    pub fn then<Fut, Out, Err>(self, job: Fut) -> JobSequence<Out, Err>
    where
        Fut: Job<Output = Out, Error = Err> + Send + Sync + 'static,
        Out: Send + Sync + 'static,
        Err: Send + Sync + 'static + std::convert::From<E>,
    {
        let previous_chain = self.chain;
        JobSequence {
            chain: Box::new(move || {
                let fut = previous_chain();
                Box::pin(async move {
                    fut.await?;
                    job.run().await
                })
            }),
        }
    }
}

// Implement the Job trait for JobSequence
impl<T, E> Job for JobSequence<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    type Output = T;
    type Error = E;

    // Run the sequence of jobs as a single job
    fn run(self) -> JobFuture<Self::Output, Self::Error> {
        (self.chain)()
    }
}

/// Manages the execution of asynchronous jobs.
pub struct WorkManager<E> {
    handles: HashMap<String, JoinHandle<Result<(), E>>>,
}

// Implement common methods for WorkManager
impl<E> WorkManager<E> {
    /// Creates a new WorkManager.
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    /// Cancels an enqueued job by the given name.
    pub fn cancel(&mut self, name: &str) {
        if let Some(handle) = self.handles.remove(name) {
            handle.abort();
        }
    }

    /// Cancels all enqueued jobs.
    pub fn cancel_all(&mut self) {
        for (_, handle) in self.handles.drain() {
            handle.abort();
        }
    }
}

// Implement job-focused methods for WorkManager
impl<E: Debug + Send + Sync + 'static> WorkManager<E> {
    /// Enqueues a job for execution.
    /// If a job with the same name already exists, it will be overwritten if `overwrite` is true.
    pub fn enqueue(
        &mut self,
        name: &str,
        job: impl Job<Error = E> + 'static,
        overwrite: bool,
    ) -> bool {
        if self.handles.contains_key(name) {
            if overwrite {
                self.cancel(name);
            } else {
                return false;
            }
        }

        let handle: JoinHandle<Result<(), E>> = spawn(async move {
            job.run().await?;
            Ok(())
        });

        self.handles.insert(name.into(), handle);
        return true;
    }

    /// Enqueues a periodic job for execution at specified intervals.
    /// If a job with the same name already exists, it will be overwritten if `overwrite` is true.
    /// If `panic` is true, the periodic job will cancel and return the error upon encountering one.
    pub fn enqueue_periodic(
        &mut self,
        name: &str,
        job: impl Job<Error = E> + Clone + 'static,
        interval: Duration,
        overwrite: bool,
        panic: bool,
    ) -> bool {
        if self.handles.contains_key(name) {
            if overwrite {
                self.cancel(name);
            } else {
                return false;
            }
        }

        let handle: JoinHandle<Result<(), E>> = spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                match job.clone().run().await {
                    Ok(_) => {}
                    Err(e) => {
                        if panic {
                            return Err(e);
                        } else {
                            eprintln!("Error: {:?}", e);
                        }
                    }
                }
                interval.tick().await;
            }
        });

        self.handles.insert(name.into(), handle);
        return true;
    }
}

impl<E> Drop for WorkManager<E> {
    fn drop(&mut self) {
        self.cancel_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // create a mock job
    #[derive(Debug, Clone)]
    struct CounterJob {
        count: u64,
    }

    // implement the Job trait for the mock job
    impl Job for CounterJob {
        type Output = u64;
        type Error = std::io::Error;

        fn run(self) -> JobFuture<Self::Output, Self::Error> {
            Box::pin(async move {
                let mut index = 0;

                // Simulate asynchronous work.
                while index < self.count {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    println!("Counter: {}", index);
                    index += 1;
                }

                Ok(index)
            })
        }
    }

    #[tokio::test]
    async fn run_job() {
        let counter = CounterJob { count: 5 };
        let result = counter.run().await;

        match result {
            Ok(result) => {
                assert_eq!(result, 5);
            }
            Err(_) => {
                panic!("Job failed");
            }
        }
    }

    #[tokio::test]
    async fn run_sequence() {
        let counter = CounterJob { count: 5 };
        let sequence = JobSequence::start_with(counter).take_then(|result| match result {
            Ok(result) => {
                assert_eq!(result, 5);
                println!("First counter finished: {}", result);
                CounterJob { count: result }
            }
            Err(_) => {
                panic!("Job failed");
            }
        });

        let result = sequence.run().await;

        match result {
            Ok(result) => {
                assert_eq!(result, 5);
            }
            Err(_) => {
                panic!("Job failed");
            }
        }
    }

    #[test]
    fn create_work_manager() {
        let mut manager = WorkManager::<std::io::Error>::new();
        manager.cancel_all();
    }
}
