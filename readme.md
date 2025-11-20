# Work Manager

`work-manager` is a Rust library designed to simplify the management of asynchronous tasks. It allows you to define `Job`s, chain them into `Sequence`s, and schedule them using a `WorkManager`.

## Features

- **Job Abstraction**: Implement the `Job` trait to define units of work.
- **Job Sequencing**: Chain jobs together using `.then()`, passing results from one to the next.
- **Task Management**: Use `WorkManager` to enqueue one-off or periodic jobs.
- **Cancellation**: Easily cancel jobs by name.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
work-manager = "0.1.0"
tokio = { version = "1", features = ["rt", "time", "macros"] }
```

## Usage

```rust
use work_manager::{Job, WorkManager};
use std::time::Duration;

struct PrintJob {
    message: String,
}

impl Job for PrintJob {
    type Output = ();
    type Error = std::io::Error;

    async fn run(self) -> Result<Self::Output, Self::Error> {
        println!("{}", self.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut manager = WorkManager::new();

    // Enqueue a single job
    manager.enqueue("print_hello", PrintJob { message: "Hello".into() }, false);

    // Enqueue a periodic job
    manager.enqueue_periodic(
        "print_tick",
        PrintJob { message: "Tick".into() },
        Duration::from_secs(1),
        false,
        false
    );
}
```

## License

This project is licensed under the MIT License.
