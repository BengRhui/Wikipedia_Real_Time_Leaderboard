# WikiRT: Real-Time Wikipedia Leaderboard
A firm real-time system designed to ingest, process, and analyze live event streams from Wikipedia's API to maintain a dynamic, real-time leaderboard. This project serves as an empirical exploration of concurrency models and memory safety in high-throughput environments.

---

## System Architecture
Built entirely in Rust, the system prioritizes temporal correctness, memory safety, and high-performance execution. It implements and contrasts two distinct concurrency models to evaluate their efficiency in real-time stream processing.
* Language: Rust
* Concurrency Models: OS-level Multi-threading vs. Asynchronous Tasks (Tokio)
* Real-time concepts implemented:
  * Zero-Copy Parsing
  * Backpressure Management with Bounded Channels
  * Priority Scheduling
  * Resource Synchronization using Mutex, RwLock, Atomic and DashMap
  * Fault Injection
  * Watchdog Monitoring
  * Graceful Shutdown
  * Fail-Safe Mechanism

---

## Prerequisite for Execution
* Rust toolchain (cargo, rustc)

---

## Core Features
* **Live Stream Ingestion**: Connects to Wikipedia's live API to fetch and parse continuous event streams in real-time.
* **Concurrent Event Processing**: Utilizes advanced concurrency structures to process incoming events, update statistics, and manage state without blocking operations.
* **Performance Profiling**: Features an integrated benchmarking suite via `Criterion.rs` to analyze execution latency, throughput, and system resource utilization under heavy loads.

---

## Documentation
The report of this project can be accessed [here]().
