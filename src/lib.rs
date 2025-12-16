//! # Work Manager
//!
//! `work-manager` is a library designed to simplify the management of asynchronous tasks.
//! It allows you to define [`Job`]s, chain them into [`Sequence`]s, and schedule them using a [`WorkManager`].
//!
//! ## Key Features
//!
//! *   **Job Abstraction**: Implement the [`Job`] trait to define units of work.
//! *   **Job Sequencing**: Chain jobs together using [`Job::then`], passing results from one to the next.
//! *   **Task Management**: Use [`WorkManager`] to enqueue one-off or periodic jobs.
//! *   **Cancellation**: Easily cancel jobs by name.
//!
//! ## Example
//!
//! ```rust
//! use work_manager::{Job, WorkManager};
//! use std::time::Duration;
//!
//! #[derive(Clone)]
//! struct PrintJob {
//!     message: String,
//! }
//!
//! impl Job for PrintJob {
//!     type Output = ();
//!     type Error = std::io::Error;
//!
//!     async fn run(self) -> Result<Self::Output, Self::Error> {
//!         println!("{}", self.message);
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mut manager = WorkManager::new();
//!
//! // Enqueue a single job
//! manager.enqueue("print_hello", PrintJob { message: "Hello".into() }, false);
//!
//! // Enqueue a periodic job
//! manager.enqueue_periodic(
//!     "print_tick",
//!     PrintJob { message: "Tick".into() },
//!     Duration::from_secs(1),
//!     false,
//!     |_| Ok(())
//! );
//! # }
//! ```

use std::collections::HashMap;
use std::error::Error;
use tokio::task::{JoinHandle, spawn};
use tokio::time::Duration;

/// A job that can be run asynchronously.
pub trait Job: Send + Sync + Sized {
    type Output: Send + Sync + 'static;
    /// Job-specific error type. Must be convertible into a boxed error so
    /// `WorkManager` does not need to be generic over the error.
    type Error: Send + Sync + 'static + Into<Box<dyn Error + Send + Sync>>;

    /// Runs the job and returns the result.
    fn run(
        self,
    ) -> impl std::future::Future<Output = Result<Self::Output, Self::Error>> + Send + Sync;

    /// Turns this job into a sequence where the next job depends on the output of this job.
    /// Does not execute anything until the entire sequence is run.
    fn then<F, Next>(self, next: F) -> Sequence<Self, Next, F>
    where
        F: FnOnce(Result<Self::Output, Self::Error>) -> Result<Next, Self::Error> + Send + Sync,
        Next: Job<Error = Self::Error>,
    {
        Sequence { prev: self, next }
    }
}

/// A struct representing a sequence where the second job is generated based on the first and the result is passed into its next function generator.
pub struct Sequence<
    Prev: Job,
    Next,
    F: FnOnce(Result<Prev::Output, Prev::Error>) -> Result<Next, Prev::Error> + Send + Sync,
> {
    prev: Prev,
    next: F,
}

impl<Prev, Next, F> Clone for Sequence<Prev, Next, F>
where
    Prev: Job + Clone,
    F: FnOnce(Result<Prev::Output, Prev::Error>) -> Result<Next, Prev::Error> + Send + Sync + Clone,
    Next: Job<Error = Prev::Error>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            next: self.next.clone(),
        }
    }
}

impl<Prev, Next, F> Job for Sequence<Prev, Next, F>
where
    Prev: Job,
    F: FnOnce(Result<Prev::Output, Prev::Error>) -> Result<Next, Prev::Error> + Send + Sync,
    Next: Job<Error = Prev::Error>,
{
    type Output = Next::Output;
    type Error = Prev::Error;

    /// Runs the sequence as a job and returns the result.
    async fn run(self) -> Result<Self::Output, Self::Error> {
        // run all previous jobs
        let output = self.prev.run().await;

        // pass the output to the next job generator and run it
        let job = (self.next)(output)?;
        job.run().await
    }

    fn then<Fn, Nx>(self, next: Fn) -> Sequence<Self, Nx, Fn>
    where
        Fn: FnOnce(Result<Self::Output, Self::Error>) -> Result<Nx, Self::Error> + Send + Sync,
        Nx: Job<Error = Self::Error>,
    {
        Sequence { prev: self, next }
    }
}

/// Manages the execution of asynchronous jobs.
pub struct WorkManager {
    handles: HashMap<String, JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>>,
}

// Implement common methods for WorkManager
impl WorkManager {
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
impl WorkManager {
    /// Enqueues a job for execution.
    /// If a job with the same name already exists, it will be overwritten if `overwrite` is true.
    pub fn enqueue(&mut self, name: &str, job: impl Job + 'static, overwrite: bool) -> bool {
        if self.handles.contains_key(name) {
            if overwrite {
                self.cancel(name);
            } else {
                return false;
            }
        }

        let handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> =
            spawn(async move { job.run().await.map(|_| ()).map_err(Into::into) });

        self.handles.insert(name.into(), handle);
        true
    }

    /// Enqueues a periodic job for execution at specified intervals.
    /// If a job with the same name already exists, it will be overwritten if `overwrite` is true.
    ///
    /// `on_error` is a callback that is invoked whenever the job returns an error.
    /// The callback receives the job's error and returns `Ok` to continue or
    /// `Err` to stop the periodic execution and finish the task.
    pub fn enqueue_periodic<J, F>(
        &mut self,
        name: &str,
        job: J,
        interval: Duration,
        overwrite: bool,
        on_error: F,
    ) -> bool
    where
        J: Job + Clone + 'static,
        F: Fn(J::Error) -> Result<(), J::Error> + Send + Sync + 'static,
    {
        if self.handles.contains_key(name) {
            if overwrite {
                self.cancel(name);
            } else {
                return false;
            }
        }

        let handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> = spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                match job.clone().run().await {
                    Ok(_) => {}
                    Err(e) => match on_error(e) {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(e.into());
                        }
                    },
                }
                interval.tick().await;
            }
        });

        self.handles.insert(name.into(), handle);
        true
    }
}

impl Drop for WorkManager {
    fn drop(&mut self) {
        self.cancel_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // create a mock job
    struct EchoJob {
        message: String,
    }

    // create a mock job that errors
    struct ErrorJob {}

    // implement the Job trait for the mock job
    impl Job for EchoJob {
        type Output = String;
        type Error = std::io::Error;

        async fn run(self) -> Result<Self::Output, Self::Error> {
            println!("{}", self.message);
            Ok(self.message)
        }
    }

    // implement the Job trait for the mock job that errors
    impl Job for ErrorJob {
        type Output = String;
        type Error = std::io::Error;

        async fn run(self) -> Result<Self::Output, Self::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "This error is expected",
            ))
        }
    }

    // create a mock job that sends a message via channel
    struct ChannelJob {
        sender: mpsc::Sender<String>,
        message: String,
    }

    impl Job for ChannelJob {
        type Output = ();
        type Error = std::io::Error;

        async fn run(self) -> Result<Self::Output, Self::Error> {
            let _ = self.sender.send(self.message).await;
            Ok(())
        }
    }

    #[derive(Clone)]
    struct PeriodicChannelJob {
        sender: mpsc::Sender<String>,
        message: String,
    }

    impl Job for PeriodicChannelJob {
        type Output = ();
        type Error = std::io::Error;

        async fn run(self) -> Result<Self::Output, Self::Error> {
            let _ = self.sender.send(self.message.clone()).await;
            Ok(())
        }
    }

    // test to see if we can run a single job
    #[tokio::test]
    async fn run_job() {
        let echo = EchoJob {
            message: "run_job".into(),
        };

        let result = echo.run().await;
        assert_eq!(result.unwrap(), "run_job");
    }

    // test to see if we can run a sequence of jobs
    #[tokio::test]
    async fn run_sequence() {
        let echo = EchoJob {
            message: "run_sequence_1".into(),
        };

        let sequence = echo.then(|result| {
            assert_eq!(result?, "run_sequence_1");
            Ok(EchoJob {
                message: "run_sequence_2".into(),
            })
        });

        let result = sequence.run().await;
        assert_eq!(result.unwrap(), "run_sequence_2");
    }

    // test to see if we can run a sequence of jobs
    #[tokio::test]
    async fn run_sequence_err() {
        let echo = EchoJob {
            message: "run_sequence_err_1".into(),
        };

        let sequence = echo
            .then(|result| {
                assert_eq!(result?, "run_sequence_err_1");
                Ok(ErrorJob {})
            })
            .then(|result| {
                assert_eq!(result?, "run_sequence_err_1");
                Ok(EchoJob {
                    message: "run_sequence_err_2".into(),
                })
            });

        let result = sequence.run().await;
        assert!(result.is_err());
        assert_eq!(
            format!("{:?}", result),
            "Err(Custom { kind: Other, error: \"This error is expected\" })"
        );
    }

    #[tokio::test]
    async fn test_work_manager_enqueue() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut manager = WorkManager::new();

        let job = ChannelJob {
            sender: tx,
            message: "enqueue_test".into(),
        };

        assert!(manager.enqueue("test_job", job, false));

        let result = rx.recv().await;
        assert_eq!(result, Some("enqueue_test".to_string()));
    }

    #[tokio::test]
    async fn test_work_manager_overwrite() {
        let mut manager = WorkManager::new();
        let echo = EchoJob {
            message: "1".into(),
        };

        // First enqueue
        assert!(manager.enqueue("job", echo, false));

        // Try enqueue same name, overwrite false
        let echo2 = EchoJob {
            message: "2".into(),
        };
        assert!(!manager.enqueue("job", echo2, false));

        // Try enqueue same name, overwrite true
        let echo3 = EchoJob {
            message: "3".into(),
        };
        assert!(manager.enqueue("job", echo3, true));
    }

    #[tokio::test]
    async fn test_work_manager_cancel() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut manager = WorkManager::new();

        // A job that waits a bit then sends
        struct DelayedJob {
            sender: mpsc::Sender<String>,
        }
        impl Job for DelayedJob {
            type Output = ();
            type Error = std::io::Error;
            async fn run(self) -> Result<(), std::io::Error> {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = self.sender.send("done".to_string()).await;
                Ok(())
            }
        }

        manager.enqueue("delayed", DelayedJob { sender: tx }, false);
        manager.cancel("delayed");

        // Wait a bit longer than the job duration
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should not receive anything because it was cancelled
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_work_manager_periodic() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut manager = WorkManager::new();

        let job = PeriodicChannelJob {
            sender: tx,
            message: "tick".into(),
        };

        // No error handler: errors (if any) are ignored, job continues
        manager.enqueue_periodic(
            "periodic",
            job,
            Duration::from_millis(10),
            false,
            |_| Ok(()),
        );

        // Expect at least 3 ticks
        for _ in 0..3 {
            let val = rx.recv().await;
            assert_eq!(val, Some("tick".to_string()));
        }
        manager.cancel("periodic");
    }

    #[tokio::test]
    async fn test_periodic_panic_behavior() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut manager = WorkManager::new();

        #[derive(Clone)]
        struct FailsAfterOne {
            sender: mpsc::Sender<String>,
            counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }
        impl Job for FailsAfterOne {
            type Output = ();
            type Error = std::io::Error;
            async fn run(self) -> Result<(), std::io::Error> {
                let count = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count == 0 {
                    let _ = self.sender.send("ok".to_string()).await;
                    Ok(())
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "boom"))
                }
            }
        }

        let job = FailsAfterOne {
            sender: tx,
            counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };

        // Use an error handler that stops after the first error (panic-like behavior)
        manager.enqueue_periodic("fail_job", job, Duration::from_millis(10), false, |err| {
            Err(err)
        });

        assert_eq!(rx.recv().await, Some("ok".to_string()));

        // Wait to ensure no more messages come (it should have errored and stopped)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_periodic_panic_continue() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut manager = WorkManager::new();

        #[derive(Clone)]
        struct FailsAfterOne {
            sender: mpsc::Sender<String>,
            counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }
        impl Job for FailsAfterOne {
            type Output = ();
            type Error = std::io::Error;
            async fn run(self) -> Result<(), std::io::Error> {
                let count = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count == 0 {
                    let _ = self.sender.send("ok".to_string()).await;
                    Ok(())
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "boom"))
                }
            }
        }

        let job = FailsAfterOne {
            sender: tx,
            counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };

        // Use an error handler that continues after an error (panic-like behavior)
        manager.enqueue_periodic(
            "fail_job",
            job,
            Duration::from_millis(10),
            false,
            |_| Ok(()),
        );

        // Expect the first ok
        let val = rx.recv().await;
        assert_eq!(val, Some("ok".to_string()));

        // Expect at least 3 ignored errors
        for _ in 0..3 {
            assert!(rx.try_recv().is_err());
        }
        manager.cancel("fail_job");
    }
}
